//! Pack writer and reader.
//!
//! A Pack is one container file (see [`crate::container`]) holding the record
//! store, admin context table, text index and spatial index. Builds write a new
//! generation next to the published one and switch the `CURRENT` pointer only
//! after the new Pack passes validation, so running servers never see a
//! partial Pack.
//!
//! ```text
//! <pack dir>/CURRENT                         id of the published generation
//! <pack dir>/generations/<id>/pack.ogp       the Pack
//! <pack dir>/generations/<id>/audit/         build report and rejections
//! ```

use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    container::{Container, ContainerWriter},
    context::{ContextReader, ContextTupleWriter},
    extsort::Scratch,
    memory::MemoryBudget,
    record::{AddressRecord, InterpolationRecord, Layer, Record, RejectedRecord, StreetRecord},
    records::{RecordsReader, RecordsWriter},
    spatial_index::{RecordCells, SpatialIndexWriter},
    street_filter::{PlaceTowns, RoutingKeyWriter, record_keys},
    text_index::TextIndexWriter,
};

pub use crate::{
    context::{AdminContextTuple, RecordContext},
    records::{ContextRecord, RecordPoint, RecordPointPrecision, RecordSource, RecordSummary},
};

pub type RecordId = u64;

mod publication;
pub use publication::resolve_pack_path;

pub const PACK_FILE: &str = "pack.ogp";
pub const AUDIT_DIR: &str = "audit";
pub const REJECTIONS_FILE: &str = "rejections.jsonl";
pub const BUILD_REPORT_FILE: &str = "build-report.json";
pub const PACK_SCHEMA_VERSION: u32 = 6;

const MANIFEST_SECTION: &str = "manifest";
const MANIFEST_VERSION: u32 = 1;
const SCRATCH_DIR: &str = ".scratch";
/// Default memory for the spatial index sorter. The builder passes its own.
const DEFAULT_SORT_BUDGET_BYTES: usize = 512 << 20;
const DEFAULT_TEXT_INDEX_MEMORY_BYTES: usize = 1 << 30;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackManifest {
    pub schema_version: u32,
    pub crate_version: String,
    pub built_at_unix: u64,
    pub record_count: u64,
    pub layer_counts: BTreeMap<String, u64>,
    pub string_count: u64,
    pub context_tuple_count: u64,
    pub text_document_count: u64,
    pub spatial: PackSpatialManifest,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackSpatialManifest {
    pub point_count: u64,
    pub segment_count: u64,
    pub cell_count: u64,
    pub context_cell_count: u64,
}

#[derive(Debug, Clone)]
pub struct PackWriterOptions {
    /// Directory for temporary build files. Defaults to a directory inside the
    /// new generation.
    pub scratch_dir: Option<PathBuf>,
    /// Pool every buffer of the build reserves from.
    pub memory: Arc<MemoryBudget>,
    /// Share of the pool the spatial index sorter may hold before it spills.
    pub sort_budget_bytes: usize,
    /// Memory for Tantivy's indexing buffers, taken from the pool.
    pub text_index_memory_bytes: usize,
}

impl Default for PackWriterOptions {
    fn default() -> Self {
        Self {
            scratch_dir: None,
            memory: MemoryBudget::unlimited(),
            sort_budget_bytes: DEFAULT_SORT_BUDGET_BYTES,
            text_index_memory_bytes: DEFAULT_TEXT_INDEX_MEMORY_BYTES,
        }
    }
}

/// Writes records in their final order. Record ids are assigned sequentially.
pub struct PackWriter {
    destination: PathBuf,
    generation: GenerationGuard,
    scratch: Arc<Scratch>,
    records: RecordsWriter,
    contexts: ContextTupleWriter,
    text: TextIndexWriter,
    spatial: SpatialIndexWriter,
    /// Street keys of every record, for the routing filter section.
    routing: RoutingKeyWriter,
    /// Town names of the boundary places written so far, kept outside the memory pool like the
    /// boundary polygons: 16 bytes per admin place.
    places: PlaceTowns,
    layer_counts: BTreeMap<Layer, u64>,
}

/// A finished, unpublished Pack.
pub struct SealedPack {
    destination: PathBuf,
    generation: GenerationGuard,
    manifest: PackManifest,
    bytes: u64,
}

/// Removes a generation that was never published: no reader can be using it.
struct GenerationGuard {
    path: PathBuf,
    keep: bool,
}

impl Drop for GenerationGuard {
    fn drop(&mut self) {
        if !self.keep {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

/// Statistics a build reports after sealing the Pack.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct PackBuildStats {
    pub bytes: u64,
    pub sections: BTreeMap<String, u64>,
    pub spatial_pair_runs: u64,
    pub text_index_bytes: u64,
    /// Routing keys pushed to the sorter, duplicates included, and the runs it spilled.
    pub routing_key_items: u64,
    pub routing_key_runs: u64,
    /// Unique keys in the routing filter; 0 when the Pack has none.
    pub routing_filter_keys: u64,
}

impl PackWriter {
    pub fn create(destination: impl AsRef<Path>) -> Result<Self> {
        Self::create_with(destination, PackWriterOptions::default())
    }

    pub fn create_with(destination: impl AsRef<Path>, options: PackWriterOptions) -> Result<Self> {
        let destination = destination.as_ref().to_path_buf();
        let generation = GenerationGuard {
            path: publication::create_generation(&destination)?,
            keep: false,
        };
        let scratch = Scratch::create(
            options
                .scratch_dir
                .map(|dir| dir.join(generation.path.file_name().expect("generation name")))
                .unwrap_or_else(|| generation.path.join(SCRATCH_DIR)),
        )?;
        Ok(Self {
            records: RecordsWriter::create(&scratch, &options.memory)?,
            contexts: ContextTupleWriter::default(),
            text: TextIndexWriter::create(
                &scratch,
                &options.memory,
                options.text_index_memory_bytes,
            )?,
            spatial: SpatialIndexWriter::new(&scratch, &options.memory, options.sort_budget_bytes),
            // The share the spatial context sorter takes: keys are 8 bytes each, far fewer than
            // the fine cell pairs.
            routing: RoutingKeyWriter::new(
                &scratch,
                &options.memory,
                options.sort_budget_bytes / 8,
            ),
            places: PlaceTowns::default(),
            scratch,
            destination,
            generation,
            layer_counts: BTreeMap::new(),
        })
    }

    /// Scratch directory shared with the builder's own sorters.
    pub fn scratch(&self) -> &Arc<Scratch> {
        &self.scratch
    }

    /// Directory of the generation being built, for build outputs that live
    /// beside the Pack file.
    pub fn generation(&self) -> &Path {
        &self.generation.path
    }

    pub fn record_count(&self) -> u64 {
        self.records.record_count()
    }

    pub fn write(&mut self, record: &Record, context: Option<RecordContext>) -> Result<RecordId> {
        let first = self.records.record_count();
        self.write_batch(std::slice::from_ref(&(record.clone(), context)))?;
        Ok(first)
    }

    /// Write records in order. Text documents and spatial cells are computed in
    /// parallel; the store itself is appended sequentially.
    pub fn write_batch(&mut self, batch: &[(Record, Option<RecordContext>)]) -> Result<()> {
        let first = self.records.record_count();
        let fields = self.text.fields();
        let places = &self.places;
        let prepared = batch
            .par_iter()
            .enumerate()
            .map(|(offset, (record, context))| {
                let record_id = first + offset as u64;
                Ok((
                    fields.document(record_id, record),
                    RecordCells::for_record(record_id, record)?,
                    record_keys(record, context.as_ref(), places),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        for (offset, ((record, context), (document, cells, keys))) in
            batch.iter().zip(prepared).enumerate()
        {
            let context = match context {
                Some(context) => self.contexts.intern(*context)?,
                None => None,
            };
            self.records.write(record, context)?;
            self.text.add(document)?;
            self.spatial.add(cells)?;
            self.routing.add(&keys)?;
            if let Record::Place(layer, place) = record {
                self.places.note(first + offset as u64, *layer, &place.name);
            }
            *self.layer_counts.entry(record.layer()).or_default() += 1;
        }
        Ok(())
    }

    /// Write the Pack file without publishing it.
    pub fn seal(self) -> Result<(SealedPack, PackBuildStats)> {
        let Self {
            destination,
            generation,
            scratch,
            records,
            contexts,
            text,
            spatial,
            routing,
            places,
            layer_counts,
        } = self;
        drop(places);
        let path = generation.path.join(PACK_FILE);
        let mut pack = ContainerWriter::create(&path)?;
        let record_count = records.record_count();
        let string_count = records.string_count();
        let context_tuple_count = contexts.tuple_count();

        records.finish(&mut pack)?;
        contexts.finish(&mut pack)?;
        let text_commit = text.finish(&mut pack)?;
        let spatial_commit = spatial.finish(&mut pack)?;
        let routing_commit = routing.finish(&mut pack)?;

        let manifest = PackManifest {
            schema_version: PACK_SCHEMA_VERSION,
            crate_version: env!("CARGO_PKG_VERSION").to_string(),
            built_at_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or_default(),
            record_count,
            layer_counts: layer_counts
                .iter()
                .map(|(layer, count)| (layer.as_str().to_string(), *count))
                .collect(),
            string_count,
            context_tuple_count,
            text_document_count: text_commit.document_count,
            spatial: PackSpatialManifest {
                point_count: spatial_commit.point_count,
                segment_count: spatial_commit.segment_count,
                cell_count: spatial_commit.cell_count,
                context_cell_count: spatial_commit.context_cell_count,
            },
        };
        pack.add(
            MANIFEST_SECTION,
            MANIFEST_VERSION,
            &serde_json::to_vec_pretty(&manifest)?,
        )?;
        let bytes = pack.finish()?;
        // Scratch files are no longer needed once the Pack file is complete.
        drop(scratch);

        let sections = Container::open(&path)?
            .sections()
            .iter()
            .map(|(name, info)| (name.clone(), info.len))
            .collect();
        Ok((
            SealedPack {
                destination,
                generation,
                manifest,
                bytes,
            },
            PackBuildStats {
                bytes,
                sections,
                spatial_pair_runs: spatial_commit.fine_pairs.runs
                    + spatial_commit.context_pairs.runs,
                text_index_bytes: text_commit.bytes,
                routing_key_items: routing_commit.items,
                routing_key_runs: routing_commit.runs,
                routing_filter_keys: routing_commit.key_count,
            },
        ))
    }

    /// Seal and publish.
    pub fn finish(self) -> Result<PackManifest> {
        self.seal()?.0.publish()
    }
}

impl SealedPack {
    pub fn generation(&self) -> &Path {
        &self.generation.path
    }

    pub fn manifest(&self) -> &PackManifest {
        &self.manifest
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Validate the Pack with the serving readers, then point `CURRENT` at it.
    pub fn publish(mut self) -> Result<PackManifest> {
        publication::publish(
            &self.destination,
            &self.generation.path,
            &self.manifest,
            &mut self.generation.keep,
        )?;
        Ok(self.manifest)
    }
}

pub struct PackReader {
    path: PathBuf,
    container: Container,
    manifest: PackManifest,
    records: RecordsReader,
    context: ContextReader,
}

impl std::fmt::Debug for PackReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PackReader")
            .field("path", &self.path)
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}

impl PackReader {
    /// Open a Pack directory (its published generation) or a Pack file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = resolve_pack_path(path)?;
        let container = Container::open(&path)?;
        let manifest: PackManifest =
            serde_json::from_slice(&container.section(MANIFEST_SECTION, MANIFEST_VERSION)?)
                .with_context(|| format!("failed to parse the manifest of {}", path.display()))?;
        if manifest.schema_version != PACK_SCHEMA_VERSION {
            bail!(
                "Pack schema version {} is unsupported; rebuild the Pack for schema {}",
                manifest.schema_version,
                PACK_SCHEMA_VERSION
            );
        }
        let records = RecordsReader::open(&container)?;
        if records.record_count() != manifest.record_count {
            bail!(
                "record store has {} records but the manifest declares {}",
                records.record_count(),
                manifest.record_count
            );
        }
        let context = ContextReader::open(&container)?;
        Ok(Self {
            path,
            container,
            manifest,
            records,
            context,
        })
    }

    pub const fn manifest(&self) -> &PackManifest {
        &self.manifest
    }

    /// The Pack file opened by this reader.
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn container(&self) -> &Container {
        &self.container
    }

    /// Check every section of the Pack file against its checksum.
    pub fn verify(&self) -> Result<()> {
        self.container
            .verify()
            .with_context(|| format!("{} failed verification", self.path.display()))
    }

    pub fn records(&self) -> &RecordsReader {
        &self.records
    }

    /// Section sizes in bytes, by section name.
    pub fn section_sizes(&self) -> BTreeMap<String, u64> {
        self.container
            .sections()
            .iter()
            .map(|(name, info)| (name.clone(), info.len))
            .collect()
    }

    pub fn record_summary(&self, record_id: RecordId) -> Result<RecordSummary> {
        self.records.summary(record_id)
    }

    pub fn record_json(&self, record_id: RecordId) -> Result<Value> {
        self.records.record_json(record_id)
    }

    pub fn records_json_by_layer(&self, layer: &str, limit: usize) -> Result<Vec<Value>> {
        let layer =
            Layer::parse(layer).with_context(|| format!("unknown record layer: {layer}"))?;
        let mut records = Vec::new();
        for record_id in 0..self.records.record_count() {
            if self.records.header(record_id)?.layer != layer {
                continue;
            }
            records.push(self.record_json(record_id)?);
            if limit > 0 && records.len() >= limit {
                break;
            }
        }
        Ok(records)
    }

    pub fn find_by_source_id(&self, source_id: &str) -> Result<Option<RecordId>> {
        for record_id in 0..self.records.record_count() {
            if self.record_summary(record_id)?.id == source_id {
                return Ok(Some(record_id));
            }
        }
        Ok(None)
    }

    pub fn address(&self, record_id: RecordId) -> Result<Option<AddressRecord>> {
        Ok(match self.records.record(record_id)? {
            Record::Address(record) => Some(record),
            _ => None,
        })
    }

    pub fn interpolation(&self, record_id: RecordId) -> Result<Option<InterpolationRecord>> {
        if self.records.header(record_id)?.layer != Layer::Interpolation {
            return Ok(None);
        }
        Ok(match self.records.record(record_id)? {
            Record::Interpolation(record) => Some(record),
            _ => None,
        })
    }

    pub fn street(&self, record_id: RecordId) -> Result<Option<StreetRecord>> {
        if self.records.header(record_id)?.layer != Layer::Street {
            return Ok(None);
        }
        Ok(match self.records.record(record_id)? {
            Record::Street(record) => Some(record),
            _ => None,
        })
    }

    pub fn context_record(&self, record_id: RecordId) -> Result<Option<ContextRecord>> {
        self.records.context_record(record_id)
    }

    pub fn boundary_context(&self, record_id: RecordId) -> Result<Option<RecordContext>> {
        self.records
            .header(record_id)?
            .context
            .map(|reference| self.context.resolve(reference))
            .transpose()
    }

    /// Rejected source objects recorded by the build that produced this Pack.
    pub fn rejections(&self, limit: usize) -> Result<Vec<RejectedRecord>> {
        let path = self
            .path
            .parent()
            .map(|dir| dir.join(AUDIT_DIR).join(REJECTIONS_FILE))
            .context("Pack file has no parent directory")?;
        let file = fs::File::open(&path)
            .with_context(|| format!("failed to open build audit {}", path.display()))?;
        let mut rejections = Vec::new();
        for line in BufReader::new(file).lines() {
            if limit > 0 && rejections.len() >= limit {
                break;
            }
            rejections.push(serde_json::from_str(&line?).context("invalid rejection record")?);
        }
        Ok(rejections)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use crate::{
        record::{
            AddressComponents, InterpolationAddressComponents, InterpolationRange,
            LocationPrecision, OsmObjectType, PlaceRecord, SourceProvenance, point_geometry,
        },
        search::{PackTextSearcher, TextSearchOptions},
        street_filter::{
            ROUTING_FILTER_SECTION, ROUTING_FILTER_VERSION, RoutingFilter, pair_hash, street_hash,
            town_hash,
        },
    };

    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("open-geocode-{name}-{}", uuid::Uuid::new_v4()))
    }

    fn address_record(object_id: i64, number: &str, street: &str) -> Record {
        Record::Address(AddressRecord {
            address: AddressComponents {
                number: number.to_string(),
                street: Some(street.to_string()),
                place: None,
                unit: None,
                locality: None,
                region: None,
                postcode: None,
                country: None,
            },
            geometry: point_geometry(-79.0, 43.0),
            location_precision: LocationPrecision::Point,
            source: SourceProvenance::osm(OsmObjectType::Node, object_id),
        })
    }

    fn write_pack(root: &Path, records: &[Record]) -> PackManifest {
        let mut writer = PackWriter::create(root).expect("writer");
        for record in records {
            writer.write(record, None).expect("write");
        }
        writer.finish().expect("finish")
    }

    fn first_hit(root: &Path, query: &str) -> Option<String> {
        PackTextSearcher::open(root)
            .expect("searcher")
            .search(TextSearchOptions {
                query: query.into(),
                limit: 1,
                layer: None,
            })
            .expect("search")
            .first()
            .map(|hit| hit.record.id.clone())
    }

    #[test]
    fn writes_one_file_and_reads_records_by_row_layer_and_source_id() {
        let root = temp_root("pack");
        let manifest = write_pack(
            &root,
            &[
                address_record(1, "10", "King Street"),
                address_record(2, "20", "Queen Street"),
            ],
        );
        assert_eq!(manifest.record_count, 2);
        assert_eq!(manifest.text_document_count, 2);
        assert_eq!(manifest.spatial.point_count, 2);

        let reader = PackReader::open(&root).expect("reader");
        assert_eq!(
            reader.path().file_name().and_then(|name| name.to_str()),
            Some(PACK_FILE)
        );
        assert_eq!(reader.record_summary(1).expect("row 1").id, "osm:node:2");
        assert_eq!(
            reader.find_by_source_id("osm:node:1").expect("lookup"),
            Some(0)
        );
        assert_eq!(
            reader
                .records_json_by_layer("address", 10)
                .expect("layer")
                .len(),
            2
        );
        assert!(
            reader
                .records_json_by_layer("street", 10)
                .expect("layer")
                .is_empty()
        );
        assert!(reader.records_json_by_layer("nope", 10).is_err());
        let sections = reader.section_sizes();
        assert!(sections.contains_key("records/blocks"));
        assert!(sections.keys().any(|name| name.starts_with("text/")));
        assert!(sections.contains_key("spatial/fine/data"));
        assert!(
            sections.contains_key("routing/filter"),
            "{:?}",
            sections.keys()
        );
        assert_eq!(first_hit(&root, "queen").as_deref(), Some("osm:node:2"));

        // The Pack file alone is a complete Pack.
        let copy = temp_root("copy").with_extension("ogp");
        fs::copy(reader.path(), &copy).expect("copy");
        assert_eq!(first_hit(&copy, "king").as_deref(), Some("osm:node:1"));
        assert!(!reader.path().parent().unwrap().join(SCRATCH_DIR).exists());
    }

    fn routing_filter(reader: &PackReader) -> Option<RoutingFilter<Vec<u8>>> {
        reader
            .container()
            .optional_section(ROUTING_FILTER_SECTION, ROUTING_FILTER_VERSION)
            .expect("section")
            .map(|bytes| RoutingFilter::parse(bytes.as_slice().to_vec()).expect("parse"))
    }

    #[test]
    fn routing_filter_holds_the_street_and_town_keys_of_every_kind_of_record() {
        let root = temp_root("routing");
        let mut writer = PackWriter::create(&root).expect("writer");
        // The boundary is written before the address that sits in it, as the OSM builder does.
        let rotorua = writer
            .write(
                &Record::Place(
                    crate::record::PlaceLayer::Locality,
                    PlaceRecord {
                        name: "Rotorua".to_string(),
                        place_type: "city".to_string(),
                        geometry: point_geometry(176.25, -38.14),
                        source: SourceProvenance::osm(OsmObjectType::Relation, 7),
                    },
                ),
                None,
            )
            .expect("place");
        let in_rotorua = RecordContext {
            admin_context: AdminContextTuple {
                locality_record_id: Some(rotorua),
                ..AdminContextTuple::default()
            },
            flags: 0,
        };
        writer
            .write(&address_record(1, "15", "Eruera St"), Some(in_rotorua))
            .expect("boundary town");
        let Record::Address(mut king) = address_record(2, "10", "King Street") else {
            unreachable!()
        };
        king.address.locality = Some("Toronto".to_string());
        writer.write(&king.into(), None).expect("own locality");
        writer
            .write(
                &Record::Interpolation(InterpolationRecord {
                    address: InterpolationAddressComponents {
                        street: Some("Elm Street".to_string()),
                        place: None,
                        locality: Some("Toronto".to_string()),
                        region: None,
                        postcode: None,
                        country: None,
                    },
                    interpolation: InterpolationRange {
                        kind: "even".to_string(),
                        start: 2,
                        end: 10,
                        step: 2,
                    },
                    anchor_node_ids: [1, 2],
                    geometry: geojson::Geometry::new(geojson::GeometryValue::LineString {
                        coordinates: vec![vec![-79.0, 43.0].into(), vec![-79.0, 43.001].into()],
                    }),
                    representative_point: [-79.0, 43.0005],
                    source: SourceProvenance::osm(OsmObjectType::Way, 3),
                }),
                None,
            )
            .expect("interpolation");
        writer
            .write(
                &Record::Street(StreetRecord {
                    name: "Queen Street".to_string(),
                    geometry: geojson::Geometry::new(geojson::GeometryValue::LineString {
                        coordinates: vec![vec![-79.0, 43.0].into(), vec![-79.0, 43.001].into()],
                    }),
                    representative_point: [-79.0, 43.0005],
                    source: SourceProvenance::osm(OsmObjectType::Way, 4),
                }),
                Some(in_rotorua),
            )
            .expect("street");
        writer.finish().expect("finish");

        let reader = PackReader::open(&root).expect("reader");
        let filter = routing_filter(&reader).expect("filter");
        let street = |name: &str| street_hash(name).expect("street");
        let pair = |name: &str, town: &str| {
            filter.maybe_contains(pair_hash(street(name), town_hash(town).expect("town")))
        };
        for name in ["Eruera Street", "King Street", "Elm Street", "Queen Street"] {
            assert!(filter.maybe_contains(street(name)), "{name}");
        }
        assert!(pair("Eruera Street", "Rotorua"), "town from the boundary");
        assert!(pair("King Street", "Toronto"), "own locality");
        assert!(pair("Elm Street", "Toronto"), "interpolation");
        assert!(
            pair("Queen Street", "Rotorua"),
            "street record with a boundary"
        );
        assert!(!pair("King Street", "Rotorua"));
        assert!(!filter.maybe_contains(street("Unknown Avenue")));
        // 4 streets, 4 street|town pairs.
        assert_eq!(filter.key_count(), 8);
    }

    #[test]
    fn a_pack_without_streets_gets_an_empty_routing_filter_and_still_opens() {
        let root = temp_root("no-routing");
        let mut writer = PackWriter::create(&root).expect("writer");
        writer
            .write(
                &Record::Place(
                    crate::record::PlaceLayer::Locality,
                    PlaceRecord {
                        name: "Rotorua".to_string(),
                        place_type: "city".to_string(),
                        geometry: point_geometry(176.25, -38.14),
                        source: SourceProvenance::osm(OsmObjectType::Relation, 7),
                    },
                ),
                None,
            )
            .expect("place");
        writer.finish().expect("finish");
        let reader = PackReader::open(&root).expect("open");
        reader.verify().expect("verify");
        // Absent would read as "built before the section existed, might hold anything"; this Pack
        // provably holds no street.
        let filter = routing_filter(&reader).expect("an empty filter");
        assert_eq!(filter.key_count(), 0);
        assert!(!filter.maybe_contains(street_hash("King Street").unwrap()));
        assert_eq!(
            first_hit(&root, "rotorua").as_deref(),
            Some("osm:relation:7")
        );
    }

    #[test]
    fn failed_osm_rebuild_preserves_published_pack() {
        let root = temp_root("rebuild");
        write_pack(&root, &[address_record(1, "10", "King Street")]);
        let generations = fs::read_dir(root.join("generations")).expect("dir").count();

        let result = crate::builder::build_osm_pack(crate::builder::BuildOsmOptions {
            input: root.join("missing.osm.pbf"),
            pack: root.clone(),
            ..crate::builder::BuildOsmOptions::default()
        });
        assert!(result.is_err());
        assert_eq!(first_hit(&root, "king").as_deref(), Some("osm:node:1"));
        assert_eq!(
            fs::read_dir(root.join("generations")).expect("dir").count(),
            generations,
            "the failed generation is removed"
        );
    }

    #[test]
    fn publishes_replacement_without_changing_existing_readers() {
        let root = temp_root("publish");
        write_pack(&root, &[address_record(1, "10", "King Street")]);
        let original = PackReader::open(&root).expect("original reader");
        let searcher = PackTextSearcher::open(&root).expect("original searcher");

        let mut replacement = PackWriter::create(&root).expect("replacement");
        replacement
            .write(&address_record(2, "20", "Queen Street"), None)
            .expect("address");
        assert_eq!(first_hit(&root, "king").as_deref(), Some("osm:node:1"));
        replacement.finish().expect("publish");

        assert_eq!(first_hit(&root, "queen").as_deref(), Some("osm:node:2"));
        assert_eq!(
            original.record_summary(0).expect("original").id,
            "osm:node:1"
        );
        let hits = searcher
            .search(TextSearchOptions {
                query: "king".into(),
                limit: 5,
                layer: None,
            })
            .expect("existing searcher after publish");
        assert_eq!(hits[0].record.id, "osm:node:1");
    }

    #[test]
    fn validation_failure_does_not_publish_replacement() {
        let root = temp_root("validation");
        write_pack(&root, &[address_record(1, "10", "King Street")]);
        let published = fs::read(root.join("CURRENT")).expect("pointer");

        let mut replacement = PackWriter::create(&root).expect("replacement");
        replacement
            .write(&address_record(2, "20", "Queen Street"), None)
            .expect("address");
        let (sealed, _) = replacement.seal().expect("seal");
        let generation = sealed.generation().to_path_buf();
        // Damage the table of contents of the unpublished Pack.
        let pack = generation.join(PACK_FILE);
        let mut bytes = fs::read(&pack).expect("read");
        let len = bytes.len();
        bytes[len - 1] ^= 0xff;
        fs::File::create(&pack)
            .and_then(|mut file| file.write_all(&bytes))
            .expect("corrupt");
        assert!(sealed.publish().is_err());
        assert_eq!(fs::read(root.join("CURRENT")).expect("pointer"), published);
        assert!(!generation.exists());
        assert_eq!(first_hit(&root, "king").as_deref(), Some("osm:node:1"));
    }

    #[test]
    fn publishing_refuses_a_pack_corrupted_inside_a_section() {
        let root = temp_root("checksum");
        write_pack(&root, &[address_record(1, "10", "King Street")]);
        let published = fs::read(root.join("CURRENT")).expect("pointer");

        let mut replacement = PackWriter::create(&root).expect("replacement");
        replacement
            .write(&address_record(2, "20", "Queen Street"), None)
            .expect("address");
        let (sealed, _) = replacement.seal().expect("seal");
        let pack = sealed.generation().join(PACK_FILE);
        let offset = Container::open(&pack).expect("open").sections()["records/strings"].offset;
        let mut bytes = fs::read(&pack).expect("read");
        let last = bytes.len() - 1;
        bytes[(offset as usize + 20).min(last)] ^= 0x55;
        fs::write(&pack, &bytes).expect("corrupt");

        let error = sealed.publish().unwrap_err();
        assert!(format!("{error:#}").contains("checksum"), "{error:#}");
        assert_eq!(fs::read(root.join("CURRENT")).expect("pointer"), published);
    }

    #[test]
    fn publishing_refuses_a_pack_with_a_corrupted_routing_filter() {
        let root = temp_root("routing-corrupt");
        write_pack(&root, &[address_record(1, "10", "King Street")]);
        let published = fs::read(root.join("CURRENT")).expect("pointer");

        let mut replacement = PackWriter::create(&root).expect("replacement");
        replacement
            .write(&address_record(2, "20", "Queen Street"), None)
            .expect("address");
        let (sealed, _) = replacement.seal().expect("seal");
        let pack = sealed.generation().join(PACK_FILE);
        let info = Container::open(&pack).expect("open").sections()[ROUTING_FILTER_SECTION].clone();
        let mut bytes = fs::read(&pack).expect("read");
        // A byte of the fingerprints, past the 16-byte header and the descriptor.
        bytes[info.offset as usize + 40] ^= 0x55;
        fs::write(&pack, &bytes).expect("corrupt");

        let error = sealed.publish().unwrap_err();
        assert!(
            format!("{error:#}").contains(ROUTING_FILTER_SECTION),
            "{error:#}"
        );
        assert_eq!(fs::read(root.join("CURRENT")).expect("pointer"), published);
    }

    #[test]
    fn a_failure_after_the_pointer_switch_keeps_the_published_generation() {
        let root = temp_root("switch-failure");
        write_pack(&root, &[address_record(1, "10", "King Street")]);
        let mut replacement = PackWriter::create(&root).expect("replacement");
        replacement
            .write(&address_record(2, "20", "Queen Street"), None)
            .expect("address");
        let (sealed, _) = replacement.seal().expect("seal");

        publication::FAIL_AFTER_SWITCH.set(true);
        let result = sealed.publish();
        publication::FAIL_AFTER_SWITCH.set(false);
        assert!(result.is_err());
        // CURRENT already names the new generation, so it must still exist.
        assert_eq!(first_hit(&root, "queen").as_deref(), Some("osm:node:2"));
    }

    #[test]
    fn text_index_memory_fits_the_budget_and_waits_for_records() {
        let root = temp_root("text-memory");
        let memory = MemoryBudget::new(64 << 20);
        let mut writer = PackWriter::create_with(
            &root,
            PackWriterOptions {
                scratch_dir: None,
                memory: Arc::clone(&memory),
                sort_budget_bytes: 16 << 20,
                text_index_memory_bytes: 16 << 20,
            },
        )
        .expect("writer");
        assert_eq!(
            memory.used(),
            0,
            "nothing is reserved before records arrive"
        );
        writer
            .write(&address_record(1, "10", "King Street"), None)
            .expect("address");
        assert!(memory.used() > 0);
        assert!(
            memory.peak() <= memory.limit(),
            "peak {} of {}",
            memory.peak(),
            memory.limit()
        );
        writer.finish().expect("finish");
        assert_eq!(first_hit(&root, "king").as_deref(), Some("osm:node:1"));
    }

    #[test]
    fn rejects_paths_without_a_pack() {
        let root = temp_root("empty");
        fs::create_dir_all(&root).expect("dir");
        assert!(PackReader::open(&root).is_err());
    }
}
