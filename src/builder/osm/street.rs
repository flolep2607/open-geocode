use std::collections::BTreeMap;

use crate::{
    builder::report::CandidateIssue,
    record::{OsmObjectType, Record, SourceProvenance, StreetRecord},
};

use super::{emitted::Emitted, geometry::simplified_line_string_geometry, tags::OsmTags};

/// Tags a street feature keeps after the scan: the name it is indexed under and
/// what the audit needs to explain a rejection.
const STREET_TAG_KEYS: [&str; 3] = ["highway", "name", "ref"];

// About one metre north-south and less east-west at every latitude.
const STREET_LINE_SIMPLIFY_EPSILON: f64 = 9e-6;

pub(crate) fn has_highway_tag(tags: &BTreeMap<String, String>) -> bool {
    tags.has("highway")
}

pub(crate) fn street_name(tags: &BTreeMap<String, String>) -> Option<String> {
    tags.cleaned("name")
}

pub(crate) fn street_tags(tags: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    STREET_TAG_KEYS
        .into_iter()
        .filter_map(|key| Some((key.to_string(), tags.get(key)?.clone())))
        .collect()
}

pub(crate) fn missing_street_name_issue(tags: &BTreeMap<String, String>) -> CandidateIssue {
    if tags.has("ref") {
        CandidateIssue::StreetRefOnlyName
    } else {
        CandidateIssue::StreetMissingName
    }
}

/// Emit a street from its resolved vertices (`None` when any node is missing).
pub(crate) fn emit_street(
    way_id: i64,
    tags: &BTreeMap<String, String>,
    points: Option<&[(f64, f64)]>,
    out: &mut Emitted,
) {
    let record = street_name(tags).zip(points).and_then(|(name, points)| {
        let built = simplified_line_string_geometry(points, STREET_LINE_SIMPLIFY_EPSILON)?;
        Some(StreetRecord {
            name,
            geometry: built.geometry,
            representative_point: built.representative_point,
            source: SourceProvenance::osm(OsmObjectType::Way, way_id),
        })
    });
    match record {
        Some(record) => {
            out.report.accept_street();
            out.records.push(Record::Street(record));
        }
        None => out.reject(
            CandidateIssue::StreetUnresolvedGeometry,
            OsmObjectType::Way,
            way_id,
            tags,
            Some(&BTreeMap::new()),
            Some("street"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use geojson::GeometryValue;

    use super::*;

    fn tags() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("highway".to_string(), "residential".to_string()),
            ("name".to_string(), " King   Street ".to_string()),
            ("surface".to_string(), "asphalt".to_string()),
        ])
    }

    fn street_record(points: &[(f64, f64)]) -> StreetRecord {
        let mut out = Emitted::default();
        emit_street(99, &street_tags(&tags()), Some(points), &mut out);

        let Some(Record::Street(record)) = out.records.pop() else {
            panic!("expected a street record");
        };
        record
    }

    fn street_coordinates(record: &StreetRecord) -> Vec<[f64; 2]> {
        match &record.geometry.value {
            GeometryValue::LineString { coordinates } => coordinates
                .iter()
                .map(|position| [position[0], position[1]])
                .collect(),
            other => panic!("expected LineString, got {}", other.type_name()),
        }
    }

    #[test]
    fn builds_street_record_from_named_highway_way() {
        let mut out = Emitted::default();
        emit_street(
            99,
            &street_tags(&tags()),
            Some(&[(43.64, -79.41), (43.66, -79.36)]),
            &mut out,
        );

        let Some(Record::Street(record)) = out.records.first() else {
            panic!("expected a street record");
        };
        assert_eq!(record.id(), "osm:way:99");
        assert_eq!(record.name, "King Street");
        assert!((record.representative_point[0] - -79.385).abs() < 0.000001);
        assert!((record.representative_point[1] - 43.65).abs() < 0.000001);
        match &record.geometry.value {
            GeometryValue::LineString { coordinates } => {
                assert_eq!(coordinates.len(), 2);
                assert_eq!(coordinates[0].as_slice(), &[-79.41, 43.64]);
            }
            other => panic!("expected LineString, got {}", other.type_name()),
        }
        assert_eq!(out.report.accepted.street_segments, 1);
    }

    #[test]
    fn simplifies_collinear_street_vertices() {
        let record = street_record(&[(0.0, 0.0), (0.0, 0.001), (0.0, 0.002), (0.0, 0.003)]);

        assert_eq!(street_coordinates(&record), vec![[0.0, 0.0], [0.003, 0.0]]);
    }

    #[test]
    fn keeps_street_bends_larger_than_the_tolerance() {
        let record = street_record(&[(0.0, 0.0), (0.001, 0.005), (0.0, 0.01)]);

        assert_eq!(
            street_coordinates(&record),
            vec![[0.0, 0.0], [0.005, 0.001], [0.01, 0.0]]
        );
    }

    #[test]
    fn keeps_the_unsimplified_street_representative_point() {
        let points = [(0.0, 0.0), (0.000002, 0.005), (0.0, 0.01)];
        let expected = super::super::geometry::line_string_geometry(&points)
            .expect("unsimplified line geometry")
            .representative_point;

        assert_eq!(street_record(&points).representative_point, expected);
    }

    #[test]
    fn leaves_two_point_streets_unchanged() {
        let record = street_record(&[(43.64, -79.41), (43.66, -79.36)]);

        assert_eq!(
            street_coordinates(&record),
            vec![[-79.41, 43.64], [-79.36, 43.66]]
        );
    }

    #[test]
    fn rejects_named_highway_without_complete_geometry() {
        let mut out = Emitted::default();
        emit_street(99, &street_tags(&tags()), None, &mut out);

        assert!(out.records.is_empty());
        assert_eq!(out.rejections.len(), 1);
        assert_eq!(out.rejections[0].reason, "street_unresolved_geometry");
        assert_eq!(out.rejections[0].layer_hint.as_deref(), Some("street"));
        assert_eq!(out.report.disposition.unresolved_geometry, 1);
    }

    #[test]
    fn keeps_only_street_tags_and_distinguishes_ref_only_highways() {
        assert_eq!(street_tags(&tags()).len(), 2);
        let ref_only = BTreeMap::from([
            ("highway".to_string(), "primary".to_string()),
            ("ref".to_string(), "401".to_string()),
        ]);
        let unnamed = BTreeMap::from([("highway".to_string(), "path".to_string())]);
        assert_eq!(
            missing_street_name_issue(&ref_only),
            CandidateIssue::StreetRefOnlyName
        );
        assert_eq!(
            missing_street_name_issue(&unnamed),
            CandidateIssue::StreetMissingName
        );
    }
}
