use std::cmp::Ordering;

use anyhow::{Context, Result, bail};

use crate::{
    extsort::Spill,
    record::Record,
    records::quantize,
    spatial_index::haversine_m,
    text_index::normalize_index_text,
    util::codec::{get_i32, get_string, put_i64, put_str},
};

use super::spill::{decode_record, encode_record, record_heap_bytes};

const COORDINATE_SCALE: f64 = 10_000_000.0;
const DUPLICATE_RADIUS_M: f64 = 150.0;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct AddressDedupeKey {
    street: String,
    number: String,
    lon_e7: i32,
    lat_e7: i32,
}

impl AddressDedupeKey {
    pub(super) fn from_record(record: &Record) -> Result<Self> {
        let Record::Address(address) = record else {
            bail!("address dedupe key requires an address record");
        };
        let street = address
            .address
            .street
            .as_deref()
            .or(address.address.place.as_deref())
            .unwrap_or_default();
        let street = normalize_index_text(street)
            .unwrap_or_default()
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect();
        let number = address
            .address
            .number
            .chars()
            .flat_map(char::to_lowercase)
            .filter(|character| !character.is_whitespace())
            .collect();
        let [lon, lat] = record
            .display_point()
            .context("address has no display point")?;
        Ok(Self {
            street,
            number,
            lon_e7: quantize(lon)?,
            lat_e7: quantize(lat)?,
        })
    }

    fn compare_group(&self, other: &Self) -> Ordering {
        (&self.street, &self.number).cmp(&(&other.street, &other.number))
    }

    fn point(&self) -> (f64, f64) {
        (
            f64::from(self.lon_e7) / COORDINATE_SCALE,
            f64::from(self.lat_e7) / COORDINATE_SCALE,
        )
    }
}

impl Spill for AddressDedupeKey {
    fn encode(&self, out: &mut Vec<u8>) {
        put_str(out, &self.street);
        put_str(out, &self.number);
        put_i64(out, i64::from(self.lon_e7));
        put_i64(out, i64::from(self.lat_e7));
    }

    fn decode(input: &mut &[u8]) -> Result<Self> {
        Ok(Self {
            street: get_string(input)?,
            number: get_string(input)?,
            lon_e7: get_i32(input)?,
            lat_e7: get_i32(input)?,
        })
    }

    fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.street.len() + self.number.len()
    }
}

#[derive(Debug)]
pub(super) struct ImportedAddress {
    key: AddressDedupeKey,
    record: Record,
}

impl ImportedAddress {
    pub(super) fn from_record(record: Record) -> Result<Self> {
        let key = AddressDedupeKey::from_record(&record)?;
        Ok(Self { key, record })
    }
}

impl PartialEq for ImportedAddress {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for ImportedAddress {}

impl PartialOrd for ImportedAddress {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ImportedAddress {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl Spill for ImportedAddress {
    fn encode(&self, out: &mut Vec<u8>) {
        self.key.encode(out);
        encode_record(out, &self.record);
    }

    fn decode(input: &mut &[u8]) -> Result<Self> {
        let key = AddressDedupeKey::decode(input)?;
        let record = decode_record(input)?;
        if !matches!(&record, Record::Address(_)) {
            bail!("imported address sorter contained a non-address record");
        }
        Ok(Self { key, record })
    }

    fn resident_bytes(&self) -> usize {
        let source_bytes = match &self.record {
            Record::Address(address) => address.source.dataset.len(),
            _ => 0,
        };
        std::mem::size_of::<Self>()
            + self.key.street.len()
            + self.key.number.len()
            + record_heap_bytes(&self.record)
            + source_bytes
    }
}

pub(super) fn filter_imported(
    osm_keys: impl Iterator<Item = Result<AddressDedupeKey>>,
    imported: impl Iterator<Item = Result<ImportedAddress>>,
    mut sink: impl FnMut(Record) -> Result<()>,
) -> Result<u64> {
    let mut osm_keys = osm_keys.peekable();
    let mut group: Option<(String, String)> = None;
    let mut osm_points = Vec::new();
    let mut duplicates = 0;

    for imported in imported {
        let imported = imported?;
        let next_group = (imported.key.street.clone(), imported.key.number.clone());
        if group.as_ref() != Some(&next_group) {
            group = Some(next_group);
            osm_points.clear();
            loop {
                let ordering = match osm_keys.peek() {
                    Some(Ok(osm_key)) => osm_key.compare_group(&imported.key),
                    Some(Err(_)) => {
                        let _ = osm_keys.next().expect("peeked OSM key")?;
                        unreachable!("an OSM key error is returned above");
                    }
                    None => break,
                };
                match ordering {
                    Ordering::Less => {
                        osm_keys.next().transpose()?;
                    }
                    Ordering::Equal => {
                        let osm_key = osm_keys.next().transpose()?.expect("peeked OSM key");
                        osm_points.push(osm_key.point());
                    }
                    Ordering::Greater => break,
                }
            }
        }

        let imported_point = imported.key.point();
        if osm_points.iter().any(|(lon, lat)| {
            haversine_m(imported_point.0, imported_point.1, *lon, *lat) <= DUPLICATE_RADIUS_M
        }) {
            duplicates += 1;
        } else {
            sink(imported.record)?;
        }
    }

    for key in osm_keys {
        key?;
    }
    Ok(duplicates)
}
