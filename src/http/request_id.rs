//! Request-id propagation (ADR 0017 Decision 30).
//!
//! Every public-demo request gets an id that flows through Cloudflare, the
//! Runtime, and Problem Details responses. Prefer an upstream id (Cloudflare
//! `cf-ray`, then a generic `x-request-id`) if present; otherwise generate one.
//! Incoming values are sanitized because clients can spoof ordinary headers
//! (Decision 24). (cloudflared connects directly to the Runtime; there is no
//! Nginx hop — ADR 0017 amended 2026-06-03b.)

use axum::{body::Body, extract::Request, http::HeaderValue, middleware::Next, response::Response};
use tower_http::{
    LatencyUnit,
    classify::{ServerErrorsAsFailures, SharedClassifier},
    trace::{DefaultOnBodyChunk, DefaultOnEos, DefaultOnRequest, DefaultOnResponse, TraceLayer},
};
use tracing::{Level, Span};

/// Typed request id stored as a request extension so handlers and fallbacks can
/// echo it into Problem Details bodies.
#[derive(Debug, Clone)]
pub(crate) struct RequestId(pub String);

const MAX_REQUEST_ID_LEN: usize = 128;

/// Middleware: resolve (or generate) the request id, store it as an extension,
/// and set it on the response `x-request-id` header.
pub(crate) async fn propagate(mut request: Request, next: Next) -> Response {
    let incoming = request
        .headers()
        .get("cf-ray")
        .or_else(|| request.headers().get("x-request-id"))
        .and_then(|value| value.to_str().ok())
        .and_then(sanitize);
    let id = incoming.unwrap_or_else(generate);

    request.extensions_mut().insert(RequestId(id.clone()));

    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

/// The per-request log layer: a span carrying the request id, method and path, and one line per
/// response with its status and latency. No separate failure line: the response line has the
/// status, and internal errors are logged with their cause by the handler.
pub(crate) type RequestTraceLayer = TraceLayer<
    SharedClassifier<ServerErrorsAsFailures>,
    fn(&axum::http::Request<Body>) -> Span,
    DefaultOnRequest,
    DefaultOnResponse,
    DefaultOnBodyChunk,
    DefaultOnEos,
    (),
>;

/// Must sit inside [`propagate`] so the [`RequestId`] extension is already set. The query string
/// is left out of the span: it holds the searched addresses.
pub(crate) fn trace_layer() -> RequestTraceLayer {
    TraceLayer::new_for_http()
        .make_span_with(request_span as fn(&axum::http::Request<Body>) -> Span)
        .on_response(
            DefaultOnResponse::new()
                .level(Level::INFO)
                .latency_unit(LatencyUnit::Millis),
        )
        .on_failure(())
}

fn request_span(request: &axum::http::Request<Body>) -> Span {
    let request_id = request
        .extensions()
        .get::<RequestId>()
        .map_or("-", |RequestId(id)| id.as_str());
    tracing::info_span!(
        "request",
        request_id,
        method = %request.method(),
        path = request.uri().path(),
    )
}

/// Keep only ASCII graphic characters and cap the length, so a hostile or
/// malformed upstream header can never inject control characters or unbounded
/// data into logs and response headers.
fn sanitize(raw: &str) -> Option<String> {
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_graphic())
        .take(MAX_REQUEST_ID_LEN)
        .collect();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn generate() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_strips_control_chars_and_caps_length() {
        assert_eq!(sanitize("abc-123").as_deref(), Some("abc-123"));
        // Whitespace and control characters are dropped.
        assert_eq!(sanitize("a b\nc").as_deref(), Some("abc"));
        // Empty after cleaning -> None so we fall back to a generated id.
        assert_eq!(sanitize("   "), None);
        let long = "x".repeat(MAX_REQUEST_ID_LEN + 50);
        assert_eq!(sanitize(&long).unwrap().len(), MAX_REQUEST_ID_LEN);
    }

    #[test]
    fn generate_produces_distinct_ids() {
        assert_ne!(generate(), generate());
    }
}
