//! Runtime request bounds (ADR 0017 Decision 26).
//!
//! Cloudflare reduces bad traffic before it reaches the Runtime, but the Runtime
//! must still defend itself if edge rules change or a route is exposed
//! differently — and, since cloudflared connects directly to the Runtime with no
//! Nginx layer (ADR 0017 amended 2026-06-03b), the Runtime is the only
//! server-side enforcement point. These are deliberately conservative, hardcoded
//! limits for the non-commercial public demo.

use axum::{body::Bytes, extract::rejection::BytesRejection, http::StatusCode};
use serde::Deserialize;
use serde_json::Value;

use crate::http::problem::{InvalidParam, Problem};
use crate::search::MAX_AUTOCOMPLETE_LIMIT;

/// Maximum length (in Unicode scalar values) of a `q` query string.
pub(crate) const MAX_QUERY_CHARS: usize = 256;

/// Maximum `limit` a `/search` request may ask for. `search.rs` has no internal
/// cap of its own (it only defaults a `limit` of 0), so this is the boundary.
pub(crate) const MAX_SEARCH_LIMIT: usize = 50;

/// Most items one `POST /geocode/batch` may carry. The router accepts the same number, so the
/// sub-batch it sends a Runtime (each item at most once) is never larger than this.
pub(crate) const MAX_BATCH_ITEMS: usize = 1000;

/// Largest `POST /geocode/batch` body. An item is at most four 256-character strings plus a few
/// small fields, about 4 KiB in the worst case of 4-byte characters, so 1000 of them fit.
pub(crate) const MAX_BATCH_BODY_BYTES: usize = 4 * 1024 * 1024;

#[derive(Deserialize)]
struct BatchBody {
    items: Vec<Value>,
}

/// The items of a batch body, bounded by size and count. Items stay raw JSON so that one with a
/// wrong type is that item's problem, not the whole request's.
pub(crate) fn validate_batch(body: Result<Bytes, BytesRejection>) -> Result<Vec<Value>, Problem> {
    let body = body.map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Problem::batch_too_large(format!("body must be at most {MAX_BATCH_BODY_BYTES} bytes"))
        } else {
            Problem::malformed_parameter("could not read the request body")
        }
    })?;
    let BatchBody { items } = serde_json::from_slice(&body).map_err(|_| {
        Problem::malformed_parameter("body must be a JSON object with an items array")
    })?;
    if items.len() > MAX_BATCH_ITEMS {
        return Err(Problem::batch_too_large(format!(
            "a batch may carry at most {MAX_BATCH_ITEMS} items"
        )));
    }
    Ok(items)
}

/// Trim and bound a query by length, returning the trimmed string. Empty is
/// allowed (autocomplete treats short/empty queries as "no suggestions").
///
/// Length is counted in Unicode scalar values, not bytes, so multibyte queries
/// are bounded by visible length rather than UTF-8 encoding size.
pub(crate) fn validate_query_length(raw: &str) -> Result<String, Problem> {
    let query = raw.trim();
    if query.chars().count() > MAX_QUERY_CHARS {
        return Err(Problem::invalid_query(format!(
            "q must be at most {MAX_QUERY_CHARS} characters"
        )));
    }
    Ok(query.to_string())
}

/// The Problem for a `/geocode/batch` item that is not an object of lookup parameters. Shared by the
/// Runtime and the router, which answers such an item itself instead of asking every Runtime.
pub(crate) fn malformed_batch_item() -> Problem {
    Problem::malformed_parameter(
        "an item must be an object of address, locality, region, postcode, layer, limit, street_fallback and area_fallback",
    )
}

/// The Problem for a `/geocode` lookup without an address.
pub(crate) fn empty_address() -> Problem {
    Problem::invalid_query("address must not be empty")
}

/// Like [`validate_query_length`] but additionally rejects an empty query, for
/// `/search` where an empty `q` is a client error.
pub(crate) fn validate_query(raw: &str) -> Result<String, Problem> {
    let query = validate_query_length(raw)?;
    if query.is_empty() {
        return Err(Problem::invalid_query("q must not be empty"));
    }
    Ok(query)
}

/// Reject a `/search` `limit` above [`MAX_SEARCH_LIMIT`].
pub(crate) fn validate_search_limit(limit: usize) -> Result<(), Problem> {
    if limit > MAX_SEARCH_LIMIT {
        return Err(Problem::limit_too_large(vec![InvalidParam::new(
            "limit",
            format!("must be at most {MAX_SEARCH_LIMIT}"),
        )]));
    }
    Ok(())
}

/// Reject a `/autocomplete` `limit` above the searcher's hard cap.
pub(crate) fn validate_autocomplete_limit(limit: usize) -> Result<(), Problem> {
    if limit > MAX_AUTOCOMPLETE_LIMIT {
        return Err(Problem::limit_too_large(vec![InvalidParam::new(
            "limit",
            format!("must be at most {MAX_AUTOCOMPLETE_LIMIT}"),
        )]));
    }
    Ok(())
}

/// Validate WGS84 coordinate ranges for reverse geocoding. Out-of-range values,
/// NaN, and infinities are all rejected (`(-180..=180).contains` is false for NaN).
pub(crate) fn validate_coords(lon: f64, lat: f64) -> Result<(), Problem> {
    let mut invalid = Vec::new();
    if !(-180.0..=180.0).contains(&lon) {
        invalid.push(InvalidParam::new("lon", "must be within [-180, 180]"));
    }
    if !(-90.0..=90.0).contains(&lat) {
        invalid.push(InvalidParam::new("lat", "must be within [-90, 90]"));
    }
    if invalid.is_empty() {
        Ok(())
    } else {
        Err(Problem::invalid_coordinate(invalid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_and_over_length_queries() {
        assert!(validate_query("   ").is_err());
        let long = "a".repeat(MAX_QUERY_CHARS + 1);
        assert!(validate_query(&long).is_err());
        assert_eq!(validate_query("  king street  ").unwrap(), "king street");
    }

    #[test]
    fn query_length_counts_chars_not_bytes() {
        // Each 'é' is two UTF-8 bytes but one scalar value; MAX_QUERY_CHARS of
        // them must be accepted, one more rejected.
        let at_limit = "é".repeat(MAX_QUERY_CHARS);
        assert!(validate_query(&at_limit).is_ok());
        let over = "é".repeat(MAX_QUERY_CHARS + 1);
        assert!(validate_query(&over).is_err());
    }

    #[test]
    fn search_limit_boundary() {
        assert!(validate_search_limit(MAX_SEARCH_LIMIT).is_ok());
        let problem = validate_search_limit(MAX_SEARCH_LIMIT + 1).unwrap_err();
        assert_eq!(problem.status(), 400);
        assert_eq!(problem.invalid_params()[0].name, "limit");
    }

    #[test]
    fn coordinate_ranges() {
        assert!(validate_coords(-180.0, -90.0).is_ok());
        assert!(validate_coords(180.0, 90.0).is_ok());

        let lon_bad = validate_coords(999.0, 0.0).unwrap_err();
        assert_eq!(lon_bad.invalid_params()[0].name, "lon");

        let lat_bad = validate_coords(0.0, 999.0).unwrap_err();
        assert_eq!(lat_bad.invalid_params()[0].name, "lat");

        assert!(validate_coords(f64::NAN, 0.0).is_err());
        assert!(validate_coords(0.0, f64::INFINITY).is_err());
    }
}
