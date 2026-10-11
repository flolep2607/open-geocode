use geo::{Coord, LineString, MultiPolygon, Rect};
use robust::{Coord as RobustCoord, orient2d};

#[derive(Debug, Clone)]
pub(super) struct PreparedBoundaryPip {
    bbox: Rect<f64>,
    bands: usize,
    offsets: Vec<u32>,
    edge_ids: Vec<u32>,
    edges: Vec<PipEdge>,
}

#[derive(Debug, Clone, Copy)]
struct PipEdge {
    ax: f64,
    ay: f64,
    bx: f64,
    by: f64,
    polygon_idx: u32,
    ring_idx: u32,
}

impl PreparedBoundaryPip {
    pub(super) fn new(geometry: &MultiPolygon<f64>, bbox: Rect<f64>) -> Self {
        let mut edges = Vec::new();
        for (polygon_idx, polygon) in geometry.0.iter().enumerate() {
            let polygon_idx =
                u32::try_from(polygon_idx).expect("boundary has more than u32::MAX polygons");
            collect_ring_edges(&mut edges, polygon.exterior(), polygon_idx, 0);
            for (interior_idx, ring) in polygon.interiors().iter().enumerate() {
                let ring_idx = u32::try_from(interior_idx + 1)
                    .expect("boundary polygon has more than u32::MAX rings");
                collect_ring_edges(&mut edges, ring, polygon_idx, ring_idx);
            }
        }

        let bands = (edges.len() / 4).clamp(1, 65_536);
        let mut counts = vec![0u32; bands];
        for edge in &edges {
            for band in edge_bands(edge, bbox, bands) {
                counts[band] = counts[band]
                    .checked_add(1)
                    .expect("boundary band has more than u32::MAX edges");
            }
        }

        let mut offsets = vec![0u32; bands + 1];
        for band in 0..bands {
            offsets[band + 1] = offsets[band]
                .checked_add(counts[band])
                .expect("boundary has more than u32::MAX edge-band references");
        }
        let mut edge_ids = vec![0u32; offsets[bands] as usize];
        let mut next = offsets[..bands].to_vec();
        for (edge_idx, edge) in edges.iter().enumerate() {
            let edge_idx = u32::try_from(edge_idx).expect("boundary has more than u32::MAX edges");
            for band in edge_bands(edge, bbox, bands) {
                edge_ids[next[band] as usize] = edge_idx;
                next[band] += 1;
            }
        }

        Self {
            bbox,
            bands,
            offsets,
            edge_ids,
            edges,
        }
    }

    pub(super) fn bounding_rect(&self) -> Rect<f64> {
        self.bbox
    }

    pub(super) fn covers(&self, px: f64, py: f64) -> bool {
        if !px.is_finite() || !py.is_finite() {
            return false;
        }

        let band = band_for_y(py, self.bbox, self.bands);
        let start = self.offsets[band] as usize;
        let end = self.offsets[band + 1] as usize;
        let mut small_crossings = [(0u32, 0u32); 32];
        let mut small_len = 0;
        let mut overflow_crossings = Vec::new();

        for edge_idx in &self.edge_ids[start..end] {
            let edge = &self.edges[*edge_idx as usize];
            if orient2d(
                RobustCoord {
                    x: edge.ax,
                    y: edge.ay,
                },
                RobustCoord {
                    x: edge.bx,
                    y: edge.by,
                },
                RobustCoord { x: px, y: py },
            ) == 0.0
                && px >= edge.ax.min(edge.bx)
                && px <= edge.ax.max(edge.bx)
                && py >= edge.ay.min(edge.by)
                && py <= edge.ay.max(edge.by)
            {
                return true;
            }

            if (edge.ay > py) != (edge.by > py) {
                let x_intersection =
                    edge.ax + (py - edge.ay) * (edge.bx - edge.ax) / (edge.by - edge.ay);
                if px < x_intersection {
                    let crossing = (edge.polygon_idx, edge.ring_idx);
                    if overflow_crossings.is_empty() && small_len < small_crossings.len() {
                        small_crossings[small_len] = crossing;
                        small_len += 1;
                    } else {
                        if overflow_crossings.is_empty() {
                            overflow_crossings.extend_from_slice(&small_crossings[..small_len]);
                        }
                        overflow_crossings.push(crossing);
                    }
                }
            }
        }

        if overflow_crossings.is_empty() {
            small_crossings[..small_len].sort_unstable();
            covered_by_rings(&small_crossings[..small_len])
        } else {
            overflow_crossings.sort_unstable();
            covered_by_rings(&overflow_crossings)
        }
    }
}

fn collect_ring_edges(
    edges: &mut Vec<PipEdge>,
    ring: &LineString<f64>,
    polygon_idx: u32,
    ring_idx: u32,
) {
    for pair in ring.0.windows(2) {
        push_edge(edges, pair[0], pair[1], polygon_idx, ring_idx);
    }
    if let (Some(first), Some(last)) = (ring.0.first(), ring.0.last())
        && first != last
    {
        push_edge(edges, *last, *first, polygon_idx, ring_idx);
    }
}

fn push_edge(
    edges: &mut Vec<PipEdge>,
    a: Coord<f64>,
    b: Coord<f64>,
    polygon_idx: u32,
    ring_idx: u32,
) {
    if a.x == b.x && a.y == b.y {
        return;
    }
    edges.push(PipEdge {
        ax: a.x,
        ay: a.y,
        bx: b.x,
        by: b.y,
        polygon_idx,
        ring_idx,
    });
}

fn band_for_y(y: f64, bbox: Rect<f64>, bands: usize) -> usize {
    let min_y = bbox.min().y;
    let max_y = bbox.max().y;
    if max_y <= min_y {
        return 0;
    }
    let band = (((y - min_y) / (max_y - min_y)) * bands as f64).floor();
    (band as isize).clamp(0, bands as isize - 1) as usize
}

fn edge_bands(edge: &PipEdge, bbox: Rect<f64>, bands: usize) -> std::ops::RangeInclusive<usize> {
    band_for_y(edge.ay.min(edge.by), bbox, bands)..=band_for_y(edge.ay.max(edge.by), bbox, bands)
}

fn covered_by_rings(crossings: &[(u32, u32)]) -> bool {
    let mut index = 0;
    while index < crossings.len() {
        let polygon_idx = crossings[index].0;
        let mut exterior_inside = false;
        let mut interior_inside = false;
        while index < crossings.len() && crossings[index].0 == polygon_idx {
            let ring_idx = crossings[index].1;
            let start = index;
            while index < crossings.len() && crossings[index] == (polygon_idx, ring_idx) {
                index += 1;
            }
            if (index - start) % 2 == 1 {
                if ring_idx == 0 {
                    exterior_inside = true;
                } else {
                    interior_inside = true;
                }
            }
        }
        if exterior_inside && !interior_inside {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use geo::{BoundingRect, Covers, Point, Polygon};

    use super::*;

    #[test]
    fn matches_covers_for_square_with_hole_and_boundary_points() {
        let geometry = MultiPolygon::new(vec![Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![ring(&[
                (3.0, 3.0),
                (7.0, 3.0),
                (7.0, 7.0),
                (3.0, 7.0),
                (3.0, 3.0),
            ])],
        )]);
        assert_matches(
            &geometry,
            &[
                (1.0, 1.0),
                (5.0, 5.0),
                (11.0, 1.0),
                (0.0, 5.0),
                (10.0, 5.0),
                (0.0, 0.0),
                (10.0, 10.0),
                (5.0, 0.0),
                (5.0, 3.0),
                (3.0, 5.0),
                (3.0, 3.0),
                (7.0, 7.0),
                (5.0, 2.999_999),
                (5.0, 3.0),
            ],
        );
    }

    #[test]
    fn matches_covers_for_multipolygon_islands_and_hole() {
        let geometry = MultiPolygon::new(vec![
            Polygon::new(
                ring(&[
                    (-10.0, -10.0),
                    (-2.0, -10.0),
                    (-2.0, -2.0),
                    (-10.0, -2.0),
                    (-10.0, -10.0),
                ]),
                vec![ring(&[
                    (-8.0, -8.0),
                    (-4.0, -8.0),
                    (-4.0, -4.0),
                    (-8.0, -4.0),
                    (-8.0, -8.0),
                ])],
            ),
            Polygon::new(
                ring(&[
                    (2.0, 2.0),
                    (10.0, 2.0),
                    (10.0, 10.0),
                    (2.0, 10.0),
                    (2.0, 2.0),
                ]),
                Vec::new(),
            ),
        ]);
        assert_matches(
            &geometry,
            &[
                (-9.0, -9.0),
                (-6.0, -6.0),
                (-8.0, -6.0),
                (6.0, 6.0),
                (1.0, 1.0),
                (10.0, 6.0),
                (2.0, 2.0),
            ],
        );
    }

    #[test]
    fn matches_covers_for_concave_comb_with_degenerate_edges() {
        let geometry = MultiPolygon::new(vec![Polygon::new(
            ring(&[
                (0.0, 10.0),
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (9.0, 10.0),
                (9.0, 2.0),
                (8.0, 2.0),
                (8.0, 10.0),
                (7.0, 10.0),
                (7.0, 2.0),
                (6.0, 2.0),
                (6.0, 10.0),
                (5.0, 10.0),
                (5.0, 2.0),
                (4.0, 2.0),
                (4.0, 10.0),
                (3.0, 10.0),
                (3.0, 2.0),
                (2.0, 2.0),
                (2.0, 10.0),
                (1.0, 10.0),
                (1.0, 2.0),
                (0.0, 2.0),
                (0.0, 10.0),
            ]),
            Vec::new(),
        )]);
        assert_matches(
            &geometry,
            &[
                (0.5, 1.0),
                (0.5, 5.0),
                (1.5, 5.0),
                (2.5, 5.0),
                (9.5, 5.0),
                (10.0, 10.0),
                (9.0, 2.0),
                (10.5, 4.0),
            ],
        );
    }

    #[test]
    fn matches_covers_for_twenty_thousand_fixed_seed_points() {
        let vertices = 2_000usize;
        let mut coordinates = Vec::with_capacity(vertices + 1);
        for index in 0..vertices {
            let angle = std::f64::consts::TAU * index as f64 / vertices as f64;
            let noise = ((index * 73 % 1_009) as f64 / 1_009.0 - 0.5) * 0.7;
            let radius = 100.0 + noise;
            coordinates.push((radius * angle.cos(), radius * angle.sin()));
        }
        coordinates.push(coordinates[0]);
        let geometry = MultiPolygon::new(vec![Polygon::new(ring(&coordinates), Vec::new())]);
        let prepared = prepare(&geometry);
        let mut seed = 0x5eed_cafe_f00d_beef;

        for _ in 0..20_000 {
            let px = random_unit(&mut seed) * 204.0 - 102.0;
            let py = random_unit(&mut seed) * 204.0 - 102.0;
            assert_eq!(
                prepared.covers(px, py),
                geometry.covers(&Point::new(px, py)),
                "point ({px}, {py})"
            );
        }
    }

    fn ring(coordinates: &[(f64, f64)]) -> LineString<f64> {
        LineString::from(coordinates.to_vec())
    }

    fn prepare(geometry: &MultiPolygon<f64>) -> PreparedBoundaryPip {
        PreparedBoundaryPip::new(
            geometry,
            geometry
                .bounding_rect()
                .expect("test geometry has a bounding rectangle"),
        )
    }

    fn assert_matches(geometry: &MultiPolygon<f64>, points: &[(f64, f64)]) {
        let prepared = prepare(geometry);
        for &(x, y) in points {
            assert_eq!(
                prepared.covers(x, y),
                geometry.covers(&Point::new(x, y)),
                "point ({x}, {y})"
            );
        }
    }

    fn random_unit(seed: &mut u64) -> f64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        (*seed >> 11) as f64 / ((1u64 << 53) as f64)
    }
}
