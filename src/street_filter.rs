//! Routing filter: which Pack holds a street.
//!
//! `open-geocode route` has to pick a country for a request that names none. Comparing the raw
//! Tantivy scores of several Packs does not do that (they are not comparable), but a street name
//! mostly does: "Eruera Street" is in one country's register and not in the others'. So the
//! builder writes a compact membership filter of street keys into the Pack, the Runtime serves its
//! bytes, and the router asks every Runtime's filter which of them can hold the street.
//!
//! Two kinds of key are hashed, both with the crate's own text normalization so a build-time key
//! and a query-time key are the same bytes:
//!
//! - `street`: the street of an address, interpolation or street record;
//! - `street|town`: that street with each town the record names, from its own locality and from
//!   the district, locality, neighbourhood and place boundaries that contain it.
//!
//! The section is an 8-bit binary fuse filter (about 1.2 bytes per key, about 0.4% false
//! positives). It never has false negatives, so a Pack that holds a street always says so.
//!
//! ```text
//! 0    magic "OGRF" | format u8 (1 = BinaryFuse8) | shard_bits u8 | key_scheme u16 | key_seed u64
//! 16   shard blobs, back to back: xorf descriptor (20 bytes) | fingerprints
//! ...  shard table: 1 << shard_bits entries of (offset u64 from the section start, len u64);
//!      len 0 is an empty shard
//! end  unique key count u64 (diagnostics)
//! ```
//!
//! `key_scheme` names the rules that turn a street into a key (`normalize_index_text`,
//! `expand_address_abbreviations`, the hash above). The builder and the router are different
//! binaries, so a router built with other rules would hash the same street to other bytes and a
//! Pack that holds it would stop saying so. A reader refuses a filter of another scheme, and
//! [`KEY_SCHEME`] is bumped with any change to those rules; `key_scheme_is_pinned` fails when one
//! changes without it.
//!
//! Keys are sharded by their top hash bits so a planet build constructs one bounded shard at a time
//! (xorf needs its keys in memory). The offsets live in a trailer so the writer never seeks.

use std::{io::Write, sync::Arc};

use anyhow::{Context, Result, anyhow, bail, ensure};
use xorf::{BinaryFuse8, BinaryFuse8Ref, DmaSerializable, Filter, FilterRef};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::{
    container::{Container, ContainerWriter},
    context::RecordContext,
    extsort::{ExternalSorter, Scratch},
    memory::{MemoryBudget, Reservation},
    pack::RecordId,
    record::{PlaceLayer, Record},
    search::{
        expand_address_abbreviations, is_house_number_token, street_fallback_query,
        strip_unit_terms,
    },
    text_index::normalize_index_text,
    util::codec::read_u64_le,
};

/// Never named `text/...`: the text index loads every section with that prefix.
pub(crate) const ROUTING_FILTER_SECTION: &str = "routing/filter";
pub(crate) const ROUTING_FILTER_VERSION: u32 = 1;
/// Bump with any change to `normalize_index_text`, `expand_address_abbreviations`, the hashes or
/// the keys a record gives; see the module docs.
pub(crate) const KEY_SCHEME: u16 = 1;

/// Fixed forever and stored in the header, so a reader can refuse a filter hashed another way.
pub(crate) const ROUTING_SEED: u64 = 0x6f67_726f_7574_6531;
/// Keeps a pair hash apart from the plain hash of any string.
const PAIR_SALT: u64 = 1;

const MAGIC: &[u8; 4] = b"OGRF";
const FORMAT_BINARY_FUSE8: u8 = 1;
const HEADER_BYTES: usize = 16;
const TABLE_ENTRY_BYTES: usize = 16;
const TRAILER_BYTES: usize = 8;
const DESCRIPTOR_BYTES: usize = BinaryFuse8::DESCRIPTOR_LEN;
const MAX_SHARD_BITS: u8 = 10;
/// Keys one shard may hold: its `Vec<u64>` is 8 bytes a key and xorf's build scratch is several
/// times that, so this bounds the memory a planet-scale build needs here.
const SHARD_TARGET_KEYS: u64 = 4_000_000;
/// Slots of the cache that drops a key just seen: records arrive in space-filling-curve order, so
/// a street and its towns repeat densely and the sorter would otherwise hold each many times.
const RECENT_SLOTS: usize = 1 << 16;

/// A street as a record stores it: normalized, then abbreviations expanded ("St" to "street").
/// Idempotent, so the already-expanded street of a query hashes the same.
pub(crate) fn street_hash(street: &str) -> Option<u64> {
    let normalized = normalize_index_text(street)?;
    Some(xxh3_64_with_seed(
        expand_address_abbreviations(&normalized).as_bytes(),
        ROUTING_SEED,
    ))
}

/// A town. Localities are never abbreviation-expanded, at build or at query time.
pub(crate) fn town_hash(town: &str) -> Option<u64> {
    let normalized = normalize_index_text(town)?;
    Some(xxh3_64_with_seed(normalized.as_bytes(), ROUTING_SEED))
}

/// The `street|town` key. A hash of hashes, so the build never keeps town strings.
pub(crate) fn pair_hash(street: u64, town: u64) -> u64 {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&street.to_le_bytes());
    bytes[8..].copy_from_slice(&town.to_le_bytes());
    xxh3_64_with_seed(&bytes, ROUTING_SEED ^ PAIR_SALT)
}

/// The streets an address typed by a person can name, most likely first, as hashes. Unit terms and
/// the house number are dropped. Two readings, because text before the number is either noise
/// ("Level 3, 100 George St") or part of the name ("Highway 6 North"), and nothing in the text
/// says which:
///
/// 1. the text from its first non-number token: it keeps "Highway 6 North" whole, and drops the
///    number of "12A St Kilda Rd" before "St" is read as "Street";
/// 2. what follows the first number, which drops "Level 3" and "Lot 5" too.
///
/// A bare street ("Peacock St") has no number to drop and is taken whole. A numeric street name
/// ("12 10 Mile Road") loses its 10 and finds no holder, which only sends the request to every
/// Pack.
pub(crate) fn query_street_hashes(text: &str) -> Vec<u64> {
    let leading = normalize_index_text(text)
        .and_then(|normalized| strip_unit_terms(&normalized))
        .map(|stripped| {
            stripped
                .split_whitespace()
                .skip_while(|token| is_house_number_token(token))
                .collect::<Vec<_>>()
                .join(" ")
        });
    let mut hashes: Vec<u64> = [leading, street_fallback_query(text)]
        .into_iter()
        .flatten()
        .filter_map(|street| street_hash(&street))
        .collect();
    hashes.dedup();
    hashes
}

/// Names of the boundary places a context tuple points at, by record id. The tuple holds ids, not
/// names, and the build only has the records it is writing, so the town hash of each place is
/// kept as it goes by. Context records are written before the records they contain (ids only
/// grow), so the table stays sorted. A place written in the same batch as the addresses that use
/// it is not known yet and gives those records fewer town keys, never a wrong one.
#[derive(Debug, Default)]
pub(crate) struct PlaceTowns {
    towns: Vec<(RecordId, u64)>,
}

impl PlaceTowns {
    pub(crate) fn note(&mut self, id: RecordId, layer: PlaceLayer, name: &str) {
        // Region and country are not towns: the routing order only separates countries by street
        // and town.
        if matches!(layer, PlaceLayer::Country | PlaceLayer::Region)
            || self.towns.last().is_some_and(|(last, _)| *last >= id)
        {
            return;
        }
        if let Some(hash) = town_hash(name) {
            self.towns.push((id, hash));
        }
    }

    fn get(&self, id: RecordId) -> Option<u64> {
        self.towns
            .binary_search_by_key(&id, |(place, _)| *place)
            .ok()
            .map(|index| self.towns[index].1)
    }
}

/// The keys one record contributes: its street, and the street with each town it names. Places,
/// postcodes and the like give none.
pub(crate) fn record_keys(
    record: &Record,
    context: Option<&RecordContext>,
    places: &PlaceTowns,
) -> Vec<u64> {
    // The same subject the text index files an address under: the street, or else the place.
    let (street, own_locality) = match record {
        Record::Address(record) => (
            record
                .address
                .street
                .as_deref()
                .or(record.address.place.as_deref()),
            record.address.locality.as_deref(),
        ),
        Record::Interpolation(record) => (
            record
                .address
                .street
                .as_deref()
                .or(record.address.place.as_deref()),
            record.address.locality.as_deref(),
        ),
        Record::Street(record) => (Some(record.name.as_str()), None),
        Record::Postcode(_) | Record::Place(..) => return Vec::new(),
    };
    let Some(street) = street.and_then(street_hash) else {
        return Vec::new();
    };

    let mut towns = Vec::with_capacity(5);
    towns.extend(own_locality.and_then(town_hash));
    if let Some(context) = context {
        // The layers `hit_matches_address_context` accepts as a locality.
        let tuple = context.admin_context;
        for id in [
            tuple.district_record_id,
            tuple.locality_record_id,
            tuple.neighbourhood_record_id,
            tuple.place_record_id,
        ]
        .into_iter()
        .flatten()
        {
            towns.extend(places.get(id));
        }
    }
    towns.sort_unstable();
    towns.dedup();

    let mut keys = Vec::with_capacity(1 + towns.len());
    keys.push(street);
    keys.extend(towns.into_iter().map(|town| pair_hash(street, town)));
    keys
}

fn shard_of(hash: u64, shard_bits: u8) -> usize {
    // A shift by 64 would overflow, so a single shard is its own case.
    if shard_bits == 0 {
        0
    } else {
        (hash >> (64 - u32::from(shard_bits))) as usize
    }
}

fn shard_bits_for(items: u64) -> u8 {
    let shards = items.div_ceil(SHARD_TARGET_KEYS).max(1).next_power_of_two();
    (shards.trailing_zeros() as u8).min(MAX_SHARD_BITS)
}

/// Writes a filter section: header, shard blobs, then the shard table and key count.
pub(crate) struct FilterEncoder<W: Write> {
    out: W,
    shard_bits: u8,
    offset: u64,
    table: Vec<(u64, u64)>,
}

impl<W: Write> FilterEncoder<W> {
    pub(crate) fn new(mut out: W, shard_bits: u8) -> Result<Self> {
        ensure!(
            shard_bits <= MAX_SHARD_BITS,
            "routing filter shard_bits {shard_bits} is above {MAX_SHARD_BITS}"
        );
        out.write_all(MAGIC)?;
        out.write_all(&[FORMAT_BINARY_FUSE8, shard_bits])?;
        out.write_all(&KEY_SCHEME.to_le_bytes())?;
        out.write_all(&ROUTING_SEED.to_le_bytes())?;
        Ok(Self {
            out,
            shard_bits,
            offset: HEADER_BYTES as u64,
            table: Vec::new(),
        })
    }

    /// The next shard, whose keys must be distinct. Shards come in index order, empty ones too.
    pub(crate) fn push_shard(&mut self, keys: &[u64]) -> Result<()> {
        ensure!(
            self.table.len() < 1 << self.shard_bits,
            "more routing filter shards than shard_bits allows"
        );
        if keys.is_empty() {
            self.table.push((0, 0));
            return Ok(());
        }
        let filter = BinaryFuse8::try_from(keys).map_err(|error| {
            anyhow!(
                "failed to build routing filter shard {}: {error}",
                self.table.len()
            )
        })?;
        let mut descriptor = [0u8; DESCRIPTOR_BYTES];
        filter.dma_copy_descriptor_to(&mut descriptor);
        let fingerprints = filter.dma_fingerprints();
        self.out.write_all(&descriptor)?;
        self.out.write_all(fingerprints)?;
        let len = (DESCRIPTOR_BYTES + fingerprints.len()) as u64;
        self.table.push((self.offset, len));
        self.offset += len;
        Ok(())
    }

    pub(crate) fn finish(mut self, key_count: u64) -> Result<W> {
        ensure!(
            self.table.len() == 1 << self.shard_bits,
            "routing filter has {} of {} shards",
            self.table.len(),
            1 << self.shard_bits
        );
        for (offset, len) in &self.table {
            self.out.write_all(&offset.to_le_bytes())?;
            self.out.write_all(&len.to_le_bytes())?;
        }
        self.out.write_all(&key_count.to_le_bytes())?;
        Ok(self.out)
    }
}

/// Build a filter from sorted keys and return the writer with the unique key count. Shards are
/// built one at a time; `upper_bound` is the number of keys including duplicates, which sizes the
/// shard buffer.
fn encode_sorted<W: Write>(
    out: W,
    shard_bits: u8,
    keys: impl Iterator<Item = Result<u64>>,
    upper_bound: u64,
    budget: &Arc<MemoryBudget>,
) -> Result<(W, u64)> {
    let mut encoder = FilterEncoder::new(out, shard_bits)?;
    let per_shard = (upper_bound >> shard_bits) as usize;
    let capacity = per_shard + per_shard / 8 + 1024;
    // The shard buffer sits in the pool; xorf's own build scratch (a few times its size) does not.
    let _buffer = Reservation::forced(budget, capacity * std::mem::size_of::<u64>());
    let mut shard: Vec<u64> = Vec::with_capacity(capacity);
    let mut current = 0usize;
    let mut previous = None;
    let mut key_count = 0u64;
    for key in keys {
        let key = key?;
        if previous == Some(key) {
            continue;
        }
        previous = Some(key);
        let index = shard_of(key, shard_bits);
        while current < index {
            encoder.push_shard(&shard)?;
            shard.clear();
            current += 1;
        }
        shard.push(key);
        key_count += 1;
    }
    while current < 1 << shard_bits {
        encoder.push_shard(&shard)?;
        shard.clear();
        current += 1;
    }
    Ok((encoder.finish(key_count)?, key_count))
}

/// What the build learned about the filter it wrote.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RoutingCommit {
    /// Keys pushed to the sorter, duplicates included.
    pub(crate) items: u64,
    pub(crate) runs: u64,
    /// Unique keys in the filter.
    pub(crate) key_count: u64,
}

/// Collects the keys of every record written and builds the filter section when the Pack is sealed.
pub(crate) struct RoutingKeyWriter {
    keys: ExternalSorter<(u64,)>,
    recent: Box<[u64]>,
    budget: Arc<MemoryBudget>,
}

impl RoutingKeyWriter {
    pub(crate) fn new(
        scratch: &Arc<Scratch>,
        budget: &Arc<MemoryBudget>,
        share_bytes: usize,
    ) -> Self {
        Self {
            keys: ExternalSorter::new(scratch, "routing", budget, share_bytes),
            recent: vec![0; RECENT_SLOTS].into_boxed_slice(),
            budget: Arc::clone(budget),
        }
    }

    pub(crate) fn add(&mut self, keys: &[u64]) -> Result<()> {
        for &key in keys {
            let slot = &mut self.recent[key as usize & (RECENT_SLOTS - 1)];
            // The exact dedup happens on the sorted stream, so this cache cannot change the output.
            // Slots start at 0, so a real key 0 is never skipped.
            if key != 0 && *slot == key {
                continue;
            }
            *slot = key;
            self.keys.push((key,))?;
        }
        Ok(())
    }

    /// Write the section. A Pack without a single street gets an empty filter, which says it holds
    /// nothing; only a Pack built before the section existed has none, and the router treats that
    /// one as holding anything.
    pub(crate) fn finish(self, pack: &mut ContainerWriter) -> Result<RoutingCommit> {
        let stats = self.keys.stats();
        let shard_bits = shard_bits_for(stats.items);
        let sorted = self.keys.finish()?.map(|key| key.map(|(key,)| key));
        pack.begin(ROUTING_FILTER_SECTION, ROUTING_FILTER_VERSION)?;
        let (_, key_count) =
            encode_sorted(&mut *pack, shard_bits, sorted, stats.items, &self.budget)?;
        pack.end()?;
        Ok(RoutingCommit {
            items: stats.items,
            runs: stats.runs,
            key_count,
        })
    }
}

/// A parsed filter over the section's bytes, which stay where they are (a mapped Pack, or a body
/// fetched by the router). Generic over the holder so the builder's tests, the Runtime's and the
/// router's all read it with one reader.
#[derive(Debug)]
pub(crate) struct RoutingFilter<B> {
    bytes: B,
    shard_bits: u8,
    /// (offset, len) of each shard blob; len 0 is an empty shard.
    shards: Vec<(usize, usize)>,
    key_count: u64,
}

impl<B: AsRef<[u8]>> RoutingFilter<B> {
    /// Check the header, the shard table and every descriptor. `BinaryFuse8Ref::from_dma` panics
    /// on a bad descriptor length and the lookup indexes by descriptor fields, so a filter that
    /// passes this cannot panic later, whatever bytes a worker sent.
    pub(crate) fn parse(bytes: B) -> Result<Self> {
        let data = bytes.as_ref();
        ensure!(
            data.len() >= HEADER_BYTES + TRAILER_BYTES,
            "routing filter is truncated"
        );
        ensure!(&data[..4] == MAGIC, "not a routing filter");
        ensure!(
            data[4] == FORMAT_BINARY_FUSE8,
            "routing filter format {} is not supported",
            data[4]
        );
        let shard_bits = data[5];
        ensure!(
            shard_bits <= MAX_SHARD_BITS,
            "routing filter shard_bits {shard_bits} is out of range"
        );
        let scheme = u16::from_le_bytes([data[6], data[7]]);
        ensure!(
            scheme == KEY_SCHEME,
            "routing filter uses key scheme {scheme}, this build uses {KEY_SCHEME}"
        );
        let seed = read_u64_le(data, 8).context("routing filter is truncated")?;
        if seed != ROUTING_SEED {
            bail!(
                "routing filter was hashed with seed {seed:#x}, this build uses {ROUTING_SEED:#x}"
            );
        }
        let table_bytes = TABLE_ENTRY_BYTES << shard_bits;
        let table_start = data
            .len()
            .checked_sub(TRAILER_BYTES + table_bytes)
            .filter(|start| *start >= HEADER_BYTES)
            .context("routing filter shard table is truncated")?;
        let mut shards = Vec::with_capacity(1 << shard_bits);
        for index in 0..1usize << shard_bits {
            let at = table_start + index * TABLE_ENTRY_BYTES;
            let offset = read_u64_le(data, at).context("routing filter is truncated")?;
            let len = read_u64_le(data, at + 8).context("routing filter is truncated")?;
            if len == 0 {
                shards.push((0, 0));
                continue;
            }
            let (offset, len) = (
                usize::try_from(offset).context("routing filter shard offset overflows")?,
                usize::try_from(len).context("routing filter shard length overflows")?,
            );
            let end = offset
                .checked_add(len)
                .filter(|end| offset >= HEADER_BYTES && *end <= table_start)
                .with_context(|| format!("routing filter shard {index} is out of bounds"))?;
            validate_descriptor(&data[offset..end])
                .with_context(|| format!("routing filter shard {index} is invalid"))?;
            shards.push((offset, len));
        }
        let key_count =
            read_u64_le(data, data.len() - TRAILER_BYTES).context("routing filter is truncated")?;
        Ok(Self {
            bytes,
            shard_bits,
            shards,
            key_count,
        })
    }

    /// Whether the Pack can hold this key. No false negatives, about 0.4% false positives.
    pub(crate) fn maybe_contains(&self, hash: u64) -> bool {
        let (offset, len) = self.shards[shard_of(hash, self.shard_bits)];
        if len == 0 {
            return false;
        }
        let blob = &self.bytes.as_ref()[offset..offset + len];
        let (descriptor, fingerprints) = blob.split_at(DESCRIPTOR_BYTES);
        BinaryFuse8Ref::from_dma(descriptor, fingerprints).contains(&hash)
    }

    /// Unique keys the build put in the filter.
    pub(crate) fn key_count(&self) -> u64 {
        self.key_count
    }

    #[cfg(test)]
    pub(crate) fn shard_count(&self) -> usize {
        self.shards.len()
    }
}

/// A shard blob whose descriptor cannot make a lookup read outside its fingerprints. The layout is
/// xorf's: seed u64, segment length u32, its mask u32, segment count times length u32. The
/// largest index a lookup computes is below `segment_count_length + 2 * segment_length`.
fn validate_descriptor(blob: &[u8]) -> Result<()> {
    ensure!(blob.len() >= DESCRIPTOR_BYTES, "descriptor is truncated");
    let word = |at: usize| u32::from_le_bytes(blob[at..at + 4].try_into().expect("four bytes"));
    let (segment_length, mask, segments) = (word(8), word(12), word(16));
    ensure!(
        segment_length.is_power_of_two() && mask == segment_length - 1,
        "descriptor segment length is not a power of two"
    );
    // A lookup reads at `h0 + 2 * segment_length` plus a mask below the segment length, with `h0`
    // below `segments`; only whole segments keep that inside `segments + 2 * segment_length`. xorf
    // adds in u32, so the sum has to fit one.
    ensure!(
        segments > 0 && segments % segment_length == 0,
        "descriptor segments are not a whole number of segments"
    );
    let needed = u64::from(segments) + 2 * u64::from(segment_length);
    ensure!(
        needed <= u64::from(u32::MAX),
        "descriptor is larger than a u32 lookup can index"
    );
    ensure!(
        (blob.len() - DESCRIPTOR_BYTES) as u64 >= needed,
        "fingerprints are shorter than the descriptor needs"
    );
    Ok(())
}

/// Parse the filter of a Pack if it has one. A Pack built before this section existed has none,
/// and a corrupt one fails the build before it is published.
pub(crate) fn validate(container: &Container) -> Result<()> {
    if let Some(bytes) =
        container.optional_section(ROUTING_FILTER_SECTION, ROUTING_FILTER_VERSION)?
    {
        RoutingFilter::parse(bytes).context("Pack routing filter is invalid")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pseudo_random(count: usize, mut state: u64) -> Vec<u64> {
        (0..count)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state
            })
            .collect()
    }

    /// Encode `keys` (sorted and distinct here) through the same path the builder takes.
    fn encode(keys: &[u64], shard_bits: u8) -> Vec<u8> {
        let mut keys = keys.to_vec();
        keys.sort_unstable();
        keys.dedup();
        let stream = keys.iter().map(|key| Ok(*key));
        encode_sorted(
            Vec::new(),
            shard_bits,
            stream,
            keys.len() as u64,
            &MemoryBudget::unlimited(),
        )
        .expect("encode")
        .0
    }

    /// Every reading of `text` is a key; the stored street must be among them.
    fn query_holds(text: &str, stored: &str) -> bool {
        query_street_hashes(text).contains(&street_hash(stored).expect("stored street"))
    }

    #[test]
    fn query_and_stored_streets_hash_alike() {
        for text in [
            "12a King St",
            "3/12 King St",
            "Level 3, 100 King St",
            "unit 3 12 King Street",
            "King Street",
            "KING STREET",
            "King St",
        ] {
            assert!(query_holds(text, "King Street"), "{text}");
        }
        assert!(query_holds("12 St Kilda Rd", "St Kilda Road"));
        assert!(query_holds("St Kilda Rd", "St Kilda Road"));
        assert!(query_holds("15 Eruera Street", "Eruera Street"));
        assert!(query_holds("12 BARRINGER STREET", "Barringer St"));
        assert!(query_holds("12 Main St E", "Main Street East"));
        assert!(query_holds("14 8th Street", "8th Street"));
        assert!(!query_holds("12 King Street", "Queen Street"));
        assert!(query_street_hashes("").is_empty());
        assert!(query_street_hashes("15").is_empty());
        assert_eq!(street_hash("  --  "), None);
    }

    /// A street typed with a number in front, or a name that holds a number, must not drift from
    /// the stored street: a drift is a false negative, and a false negative misroutes.
    #[test]
    fn query_streets_do_not_drift_from_stored_streets() {
        let streets = [
            "King Street",
            "St Kilda Road",
            "St John St",
            "Highway 6 North",
            "State Highway 1 South",
            "Number 1 Road",
            "8th Street",
            "5th Ave",
            "Main St E",
            "Peacock St",
            "Queen Street",
            "Māori Road",
            "Église Street",
        ];
        let prefixes = [
            "",
            "12 ",
            "12A ",
            "12-14 ",
            "12a-14b ",
            "2/172 ",
            "3/12 ",
            "U3/12 ",
            "Unit 3, 12 ",
            "Unit 3 12 ",
            "Flat 4, 8 ",
            "Suite 400, 5 ",
            "Level 2, 100 ",
            "Shop 2, 15 ",
            "Lot 5 ",
            "Building B, 12 ",
        ];
        for street in streets {
            for prefix in prefixes {
                let text = format!("{prefix}{street}");
                assert!(query_holds(&text, street), "{text:?} against {street:?}");
            }
        }
        // The readings, most likely first: a name keeps its number, a unit is dropped.
        assert_eq!(
            query_street_hashes("Highway 6 North")[0],
            street_hash("Highway 6 North").unwrap()
        );
        assert_eq!(
            query_street_hashes("Level 3, 100 King St")[0],
            street_hash("Level 3 100 King St").unwrap()
        );
    }

    /// Keys written into Packs already built must keep matching the router's, so these hashes
    /// never change without [`KEY_SCHEME`] being bumped (and the Packs rebuilt).
    #[test]
    fn key_scheme_is_pinned() {
        let street = |text: &str| street_hash(text).expect("street");
        assert_eq!(KEY_SCHEME, 1);
        assert_eq!(street("King Street"), 8_499_246_586_564_395_725);
        assert_eq!(street("King St"), street("king street"));
        assert_eq!(street("St Kilda Rd"), 15_419_769_240_993_744_527);
        assert_eq!(street("Main St E"), 10_572_333_900_476_732_225);
        assert_eq!(town_hash("Toronto"), Some(147_203_219_508_908_857));
        assert_eq!(
            pair_hash(street("King Street"), town_hash("Toronto").unwrap()),
            11_569_134_895_287_790_255
        );
    }

    /// The layout is xorf's descriptor then its fingerprints; this fails loudly if a xorf upgrade
    /// changes either, which would make every published filter unreadable or wrong.
    #[test]
    fn encoded_bytes_are_pinned() {
        let keys: Vec<u64> = (1..=100u64)
            .map(|key| key.wrapping_mul(0x9e37_79b9_7f4a_7c15))
            .collect();
        let bytes = encode(&keys, 0);
        assert_eq!(bytes.len(), 252);
        assert_eq!(xxh3_64_with_seed(&bytes, 0), 11_935_176_968_712_116_154);
    }

    #[test]
    fn expansion_is_idempotent() {
        for street in [
            "St Kilda Rd",
            "Main St E",
            "Queen St",
            "5th Ave",
            "Cuba Mall",
        ] {
            let once = expand_address_abbreviations(&normalize_index_text(street).unwrap());
            assert_eq!(expand_address_abbreviations(&once), once, "{street}");
        }
    }

    #[test]
    fn pair_hash_is_not_the_street_hash_and_depends_on_order() {
        let street = street_hash("King Street").unwrap();
        let town = town_hash("Toronto").unwrap();
        assert_ne!(pair_hash(street, town), street);
        assert_ne!(pair_hash(street, town), pair_hash(town, street));
        assert_eq!(pair_hash(street, town), pair_hash(street, town));
        assert_eq!(town_hash("TORONTO"), town_hash("Toronto"));
    }

    #[test]
    fn filter_round_trips_with_one_and_several_shards() {
        for shard_bits in [0u8, 2] {
            let keys = pseudo_random(10_000, 0x9e37_79b9_7f4a_7c15);
            let bytes = encode(&keys, shard_bits);
            let filter = RoutingFilter::parse(bytes.as_slice()).expect("parse");
            assert_eq!(filter.shard_count(), 1 << shard_bits);
            assert_eq!(filter.key_count(), 10_000);
            assert!(keys.iter().all(|key| filter.maybe_contains(*key)));
            let probes = pseudo_random(10_000, 0x1234_5678_9abc_def1);
            let false_positives = probes
                .iter()
                .filter(|key| filter.maybe_contains(**key))
                .count();
            assert!(false_positives < 200, "{false_positives} of 10000");
        }
    }

    #[test]
    fn tiny_and_empty_filters_work() {
        // xorf's behaviour below a handful of keys is not documented, so pin it: with one shard
        // every size from 1 to 9 is a filter of its own, with four most shards are empty.
        for shard_bits in [0u8, 2] {
            for count in 0..=9 {
                let keys = pseudo_random(count, 77 + count as u64);
                let bytes = encode(&keys, shard_bits);
                let filter = RoutingFilter::parse(bytes.as_slice()).expect("parse");
                assert_eq!(filter.key_count(), count as u64);
                assert!(
                    keys.iter().all(|key| filter.maybe_contains(*key)),
                    "{count}"
                );
            }
        }
        let bytes = encode(&[], 0);
        let empty = RoutingFilter::parse(bytes.as_slice()).expect("parse");
        assert!(!empty.maybe_contains(42));
    }

    #[test]
    fn parse_rejects_what_it_cannot_read() {
        let good = encode(&pseudo_random(100, 5), 1);
        assert!(RoutingFilter::parse(good.as_slice()).is_ok());

        let mut bad = good.clone();
        bad[0] = b'X';
        assert!(
            format!("{:#}", RoutingFilter::parse(bad).unwrap_err())
                .contains("not a routing filter")
        );
        let mut bad = good.clone();
        bad[4] = 9;
        assert!(format!("{:#}", RoutingFilter::parse(bad).unwrap_err()).contains("format 9"));
        let mut bad = good.clone();
        bad[8] ^= 1;
        assert!(format!("{:#}", RoutingFilter::parse(bad).unwrap_err()).contains("seed"));
        let mut bad = good.clone();
        bad[5] = 200;
        assert!(RoutingFilter::parse(bad).is_err());
        // The shard table is cut short, and the whole thing is cut short.
        assert!(RoutingFilter::parse(&good[..good.len() - 30]).is_err());
        assert!(RoutingFilter::parse(&good[..10]).is_err());
        // A descriptor that would index past its fingerprints.
        let mut bad = good.clone();
        let blob = HEADER_BYTES;
        bad[blob + 16..blob + 20].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(RoutingFilter::parse(bad).is_err());
        // Keys hashed by other rules.
        let mut bad = good.clone();
        bad[6] = 9;
        assert!(format!("{:#}", RoutingFilter::parse(bad).unwrap_err()).contains("key scheme 9"));
    }

    /// One filter shard blob made by hand: segment length 4, 13 fingerprint bytes.
    fn hand_built(segment_length: u32, mask: u32, segments: u32, fingerprints: usize) -> Vec<u8> {
        let mut section = Vec::new();
        section.extend_from_slice(MAGIC);
        section.extend_from_slice(&[FORMAT_BINARY_FUSE8, 0]);
        section.extend_from_slice(&KEY_SCHEME.to_le_bytes());
        section.extend_from_slice(&ROUTING_SEED.to_le_bytes());
        section.extend_from_slice(&7u64.to_le_bytes());
        section.extend_from_slice(&segment_length.to_le_bytes());
        section.extend_from_slice(&mask.to_le_bytes());
        section.extend_from_slice(&segments.to_le_bytes());
        section.extend(std::iter::repeat_n(0xA5, fingerprints));
        let len = (DESCRIPTOR_BYTES + fingerprints) as u64;
        section.extend_from_slice(&(HEADER_BYTES as u64).to_le_bytes());
        section.extend_from_slice(&len.to_le_bytes());
        section.extend_from_slice(&1u64.to_le_bytes());
        section
    }

    #[test]
    fn parse_rejects_descriptors_whose_lookup_would_leave_the_fingerprints() {
        // Long enough for `segments + 2 * segment_length`, but not a whole number of segments: a
        // lookup reaches index 13 of 13 bytes. It used to parse and then panic.
        let section = hand_built(4, 3, 5, 13);
        assert!(format!("{:#}", RoutingFilter::parse(section).unwrap_err()).contains("segments"));
        assert!(RoutingFilter::parse(hand_built(4, 3, 0, 64)).is_err());
        assert!(RoutingFilter::parse(hand_built(4, 3, u32::MAX - 3, 64)).is_err());
        // The same shape with whole segments is a filter, and answers without panicking.
        let filter = RoutingFilter::parse(hand_built(4, 3, 8, 16)).expect("whole segments");
        for key in pseudo_random(10_000, 3) {
            filter.maybe_contains(key);
        }
    }

    #[test]
    fn no_corrupted_descriptor_can_make_a_lookup_panic() {
        let good = encode(&pseudo_random(100, 5), 0);
        let keys = pseudo_random(2_000, 11);
        // Every byte of the descriptor, flipped to a few values: parse either refuses it or the
        // filter answers every key.
        for at in HEADER_BYTES..HEADER_BYTES + DESCRIPTOR_BYTES {
            for value in [0u8, 1, 3, 0x7f, 0xff] {
                let mut bad = good.clone();
                bad[at] = value;
                if let Ok(filter) = RoutingFilter::parse(bad) {
                    for key in &keys {
                        filter.maybe_contains(*key);
                    }
                }
            }
        }
    }

    #[test]
    fn record_keys_cover_street_own_locality_and_boundary_towns() {
        use crate::{
            context::AdminContextTuple,
            record::{
                AddressComponents, AddressRecord, LocationPrecision, OsmObjectType,
                SourceProvenance, point_geometry,
            },
        };

        let record = Record::Address(AddressRecord {
            address: AddressComponents {
                number: "10".into(),
                street: Some("King St".into()),
                place: None,
                unit: None,
                locality: Some("Toronto".into()),
                region: None,
                postcode: None,
                country: None,
            },
            geometry: point_geometry(-79.0, 43.0),
            location_precision: LocationPrecision::Point,
            source: SourceProvenance::osm(OsmObjectType::Node, 1),
        });
        let mut places = PlaceTowns::default();
        places.note(3, PlaceLayer::Locality, "Old Toronto");
        places.note(4, PlaceLayer::Region, "Ontario");
        places.note(2, PlaceLayer::District, "out of order is ignored");
        let context = RecordContext {
            admin_context: AdminContextTuple {
                locality_record_id: Some(3),
                region_record_id: Some(4),
                district_record_id: Some(2),
                ..AdminContextTuple::default()
            },
            flags: 0,
        };

        let street = street_hash("King Street").unwrap();
        let mut expected = vec![
            street,
            pair_hash(street, town_hash("Toronto").unwrap()),
            pair_hash(street, town_hash("Old Toronto").unwrap()),
        ];
        let mut keys = record_keys(&record, Some(&context), &places);
        keys.sort_unstable();
        expected.sort_unstable();
        assert_eq!(keys, expected);

        // Without context only the own locality is known; a place record gives nothing.
        assert_eq!(record_keys(&record, None, &places).len(), 2);
    }

    #[test]
    fn validate_refuses_a_garbage_section_and_accepts_none() {
        let path =
            std::env::temp_dir().join(format!("open-geocode-routing-{}", uuid::Uuid::new_v4()));
        let mut writer = ContainerWriter::create(&path).expect("create");
        writer.add("other", 1, b"x").expect("add");
        writer.finish().expect("finish");
        validate(&Container::open(&path).expect("open")).expect("no section is fine");

        let mut writer = ContainerWriter::create(&path).expect("create");
        writer
            .add(
                ROUTING_FILTER_SECTION,
                ROUTING_FILTER_VERSION,
                b"not a filter at all, really",
            )
            .expect("add");
        writer.finish().expect("finish");
        assert!(validate(&Container::open(&path).expect("open")).is_err());
    }
}
