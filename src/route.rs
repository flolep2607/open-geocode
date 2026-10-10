//! `open-geocode route`: one HTTP entry point in front of one Runtime per Pack.
//!
//! A Pack is built from one extract, and a Runtime serves one Pack, so a deployment covering
//! several countries runs several Runtimes. The router gives clients a single URL:
//!
//! - `/search`, `/autocomplete` with `country=XX` go, unchanged, to that country's Runtime.
//!   Without `country` they go to every Runtime at once; the results are merged, cut to `limit`,
//!   and each merged result carries the `country` it came from. BM25 scores from different Packs
//!   are not comparable, so the merge ranks first by how much of the query a label covers, then
//!   by rank within its own Runtime's list (see [`merge`]).
//! - `/reverse` needs `country`: a coordinate's country is only known to the Runtimes themselves.
//! - `/readyz` is ready only when every Runtime is.
//!
//! The router holds no Pack and does no geocoding. It keeps the Runtime's boundary policy: GET
//! only, Problem Details for errors, request-id propagation. Its request id is sent to every
//! Runtime it calls as `x-request-id`, so router and Runtime logs share one id per request.

use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use axum::{
    Extension, Router,
    body::Body,
    extract::{RawQuery, State},
    handler::{Handler, HandlerWithoutStateExt},
    http::{HeaderMap, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::{MethodFilter, MethodRouter, on},
};
use futures_util::future::join_all;
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use crate::{
    http::{
        method,
        problem::Problem,
        request_id::{self, RequestId},
        shutdown::shutdown_signal,
    },
    search::DEFAULT_SEARCH_LIMIT,
    text_index::normalize_index_text,
};

/// How long the router waits for one Runtime before answering 502 (or leaving it out of a merge).
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct RouteOptions {
    /// (country code, Runtime base URL) pairs, e.g. ("NZ", "http://127.0.0.1:8081").
    pub workers: Vec<(String, String)>,
    pub bind: SocketAddr,
}

#[derive(Clone)]
struct RouteState {
    /// Country code (upper case) -> Runtime base URL without a trailing slash.
    workers: Arc<BTreeMap<String, String>>,
    client: Client<HttpConnector, Empty<Bytes>>,
}

impl RouteState {
    fn new(workers: Vec<(String, String)>) -> Result<Self> {
        let mut map = BTreeMap::new();
        for (country, url) in workers {
            let country = country.trim().to_ascii_uppercase();
            if country.is_empty()
                || map
                    .insert(country.clone(), url.trim_end_matches('/').to_string())
                    .is_some()
            {
                bail!(
                    "each --worker needs a distinct non-empty country, got {country:?} twice or empty"
                );
            }
        }
        if map.is_empty() {
            bail!("route needs at least one --worker COUNTRY=URL");
        }
        Ok(Self {
            workers: Arc::new(map),
            client: Client::builder(TokioExecutor::new()).build_http(),
        })
    }

    fn known(&self) -> String {
        self.workers.keys().cloned().collect::<Vec<_>>().join(", ")
    }

    /// GET `url` under `request_id`: (status, the headers worth passing on, body), or None when
    /// the Runtime did not answer in time.
    async fn get(&self, url: &str, request_id: Option<&str>) -> Option<Upstream> {
        let answer = self.try_get(url, request_id).await;
        if let Err(error) = &answer {
            // The query string is the searched address; keep it out of the logs.
            let url = url.split_once('?').map_or(url, |(path, _)| path);
            tracing::warn!(
                request_id = request_id.unwrap_or("-"),
                url,
                "Runtime did not answer: {error}"
            );
        }
        answer.ok()
    }

    async fn try_get(&self, url: &str, request_id: Option<&str>) -> Result<Upstream> {
        let mut request = hyper::Request::get(url);
        if let Some(id) = request_id {
            request = request.header("x-request-id", id);
        }
        let request = request.body(Empty::new())?;
        let response = tokio::time::timeout(UPSTREAM_TIMEOUT, self.client.request(request))
            .await
            .context("timed out")??;
        let status = response.status();
        let mut headers = HeaderMap::new();
        // Retry-After keeps a Runtime's load-shedding 503 actionable through the router.
        for name in [header::CONTENT_TYPE, header::RETRY_AFTER] {
            if let Some(value) = response.headers().get(&name) {
                headers.insert(name, value.clone());
            }
        }
        let body = tokio::time::timeout(UPSTREAM_TIMEOUT, response.into_body().collect())
            .await
            .context("timed out reading the body")??
            .to_bytes();
        Ok((status, headers, body))
    }
}

/// An upstream answer: status, the headers passed on to the client, body.
type Upstream = (StatusCode, HeaderMap, Bytes);

/// Parse `country=` and `limit=` out of a raw query string; the query itself is forwarded as is.
fn query_params(raw: Option<&str>) -> (Option<String>, usize) {
    let pairs: Vec<(String, String)> =
        serde_urlencoded::from_str(raw.unwrap_or_default()).unwrap_or_default();
    let mut country = None;
    let mut limit = DEFAULT_SEARCH_LIMIT;
    for (name, value) in pairs {
        match name.as_str() {
            "country" if !value.is_empty() => country = Some(value.to_ascii_uppercase()),
            "limit" => limit = value.parse().unwrap_or(DEFAULT_SEARCH_LIMIT),
            _ => {}
        }
    }
    (country, limit)
}

/// The distinct normalised tokens of `text`, in order of first appearance.
fn tokens(text: &str) -> Vec<String> {
    let mut tokens: Vec<String> = Vec::new();
    for token in normalize_index_text(text)
        .unwrap_or_default()
        .split_whitespace()
    {
        if !tokens.iter().any(|seen| seen == token) {
            tokens.push(token.to_string());
        }
    }
    tokens
}

/// The fraction of the distinct `query_tokens` found among the tokens of `label`. With
/// `prefix_last`, the query's last token (`last`) also matches as a prefix, since the user is
/// still typing it.
fn coverage(query_tokens: &[String], last: Option<&str>, label: &str, prefix_last: bool) -> f64 {
    if query_tokens.is_empty() {
        return 0.0;
    }
    let label = tokens(label);
    let matched = query_tokens
        .iter()
        .filter(|token| {
            label.contains(token)
                || (prefix_last
                    && last == Some(token.as_str())
                    && label.iter().any(|word| word.starts_with(token.as_str())))
        })
        .count();
    matched as f64 / query_tokens.len() as f64
}

/// Merge each Runtime's `key` array into one, cut to `limit`, each tagged with its country.
/// `answers` are (country, parsed response body) pairs.
///
/// Raw scores are BM25 over each Pack's own corpus, so they do not compare across countries.
/// Results are ranked by the share of the query their label covers, then by rank in their own
/// Runtime's list (so equal matches interleave across countries), then by score, then by country.
/// `prefix_last` is for autocomplete, where the last query token may be a partial word.
fn merge(answers: Vec<(String, Value)>, key: &str, limit: usize, prefix_last: bool) -> Value {
    let query = answers
        .iter()
        .find_map(|(_, body)| body.get("query").cloned())
        .unwrap_or(Value::Null);
    let query_text = query.as_str().unwrap_or_default();
    let query_tokens = tokens(query_text);
    let last = normalize_index_text(query_text)
        .and_then(|text| text.split_whitespace().last().map(str::to_string));

    struct Ranked {
        coverage: f64,
        rank: usize,
        score: f64,
        country: String,
        item: Value,
    }
    let mut merged: Vec<Ranked> = answers
        .into_iter()
        .flat_map(|(country, body)| {
            let items = body
                .get(key)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let query_tokens = &query_tokens;
            let last = last.as_deref();
            items.into_iter().enumerate().map(move |(rank, mut item)| {
                let label = item
                    .get("label")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let coverage = coverage(query_tokens, last, label, prefix_last);
                let score = item
                    .get("score")
                    .and_then(Value::as_f64)
                    .unwrap_or(f64::MIN);
                if let Some(object) = item.as_object_mut() {
                    object.insert("country".to_string(), Value::String(country.clone()));
                }
                Ranked {
                    coverage,
                    rank,
                    score,
                    country: country.clone(),
                    item,
                }
            })
        })
        .collect();
    merged.sort_by(|a, b| {
        b.coverage
            .total_cmp(&a.coverage)
            .then(a.rank.cmp(&b.rank))
            .then(b.score.total_cmp(&a.score))
            .then_with(|| a.country.cmp(&b.country))
    });
    let merged: Vec<Value> = merged
        .into_iter()
        .take(limit)
        .map(|ranked| ranked.item)
        .collect();
    json!({ "query": query, key: merged })
}

/// An upstream answer passed on to the client as is.
fn pass_through((status, headers, body): Upstream) -> Response {
    let mut response = (status, Body::from(body)).into_response();
    response.headers_mut().extend(headers);
    response
}

fn with_id(problem: Problem, request_id: &Option<Extension<RequestId>>) -> Response {
    match request_id {
        Some(Extension(RequestId(id))) => problem.with_request_id(id.clone()).into_response(),
        None => problem.into_response(),
    }
}

/// Forward `path?raw` to one country's Runtime, or fan out and merge `key` when no country is given.
/// `prefix_last` is passed to [`merge`].
async fn forward_or_merge(
    state: &RouteState,
    path: &str,
    key: Option<&str>,
    prefix_last: bool,
    raw: Option<String>,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    let id = request_id
        .as_ref()
        .map(|Extension(RequestId(id))| id.as_str());
    let (country, limit) = query_params(raw.as_deref());
    let suffix = raw.map(|q| format!("?{q}")).unwrap_or_default();

    if let Some(country) = country {
        let Some(base) = state.workers.get(&country) else {
            return with_id(
                Problem::unknown_country(&country, &state.known()),
                &request_id,
            );
        };
        return match state.get(&format!("{base}{path}{suffix}"), id).await {
            Some(answer) => pass_through(answer),
            None => with_id(Problem::upstream_unavailable(&country), &request_id),
        };
    }

    let Some(key) = key else {
        return with_id(Problem::country_required(path), &request_id);
    };
    let calls = state.workers.iter().map(|(country, base)| {
        let url = format!("{base}{path}{suffix}");
        async move { (country.clone(), state.get(&url, id).await) }
    });
    let mut answers = Vec::new();
    for (country, answer) in join_all(calls).await {
        match answer {
            // A Runtime's 4xx (a bad query) is every Runtime's: hand the first one back as is.
            Some(answer) if answer.0.is_client_error() => return pass_through(answer),
            Some((status, _, body)) if status.is_success() => {
                if let Ok(value) = serde_json::from_slice::<Value>(&body) {
                    answers.push((country, value));
                }
            }
            // A Runtime that failed or did not answer is left out of the merge.
            _ => {}
        }
    }
    if answers.is_empty() {
        return with_id(Problem::upstream_unavailable("any"), &request_id);
    }
    axum::Json(merge(answers, key, limit, prefix_last)).into_response()
}

async fn search(
    State(state): State<RouteState>,
    request_id: Option<Extension<RequestId>>,
    RawQuery(raw): RawQuery,
) -> Response {
    forward_or_merge(&state, "/search", Some("results"), false, raw, request_id).await
}

async fn autocomplete(
    State(state): State<RouteState>,
    request_id: Option<Extension<RequestId>>,
    RawQuery(raw): RawQuery,
) -> Response {
    forward_or_merge(
        &state,
        "/autocomplete",
        Some("suggestions"),
        true,
        raw,
        request_id,
    )
    .await
}

async fn reverse(
    State(state): State<RouteState>,
    request_id: Option<Extension<RequestId>>,
    RawQuery(raw): RawQuery,
) -> Response {
    forward_or_merge(&state, "/reverse", None, false, raw, request_id).await
}

/// `/geocode` answers with one best match, so like `/reverse` it needs `country`.
async fn geocode(
    State(state): State<RouteState>,
    request_id: Option<Extension<RequestId>>,
    RawQuery(raw): RawQuery,
) -> Response {
    forward_or_merge(&state, "/geocode", None, false, raw, request_id).await
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}

/// Ready only when every Runtime answers its own `/readyz` with 200.
async fn readyz(
    State(state): State<RouteState>,
    request_id: Option<Extension<RequestId>>,
) -> StatusCode {
    let id = request_id
        .as_ref()
        .map(|Extension(RequestId(id))| id.as_str());
    let calls = state.workers.values().map(|base| {
        let url = format!("{base}/readyz");
        let state = &state;
        async move { state.get(&url, id).await.map(|(status, _, _)| status) }
    });
    let all_ready = join_all(calls)
        .await
        .into_iter()
        .all(|status| status == Some(StatusCode::OK));
    if all_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

fn get_only<H, T>(handler: H) -> MethodRouter<RouteState>
where
    H: Handler<T, RouteState>,
    T: 'static,
{
    on(MethodFilter::GET, handler).on(MethodFilter::HEAD, method::method_not_allowed)
}

fn build_router(state: RouteState) -> Router {
    Router::new()
        .route("/search", get_only(search))
        .route("/autocomplete", get_only(autocomplete))
        .route("/reverse", get_only(reverse))
        .route("/geocode", get_only(geocode))
        .route("/healthz", get_only(healthz))
        .route("/readyz", get_only(readyz))
        .method_not_allowed_fallback(method::method_not_allowed)
        .fallback_service(method::not_found.into_service())
        .layer(request_id::trace_layer())
        .layer(middleware::from_fn(request_id::propagate))
        .with_state(state)
}

pub async fn route(options: RouteOptions) -> Result<()> {
    let state = RouteState::new(options.workers)?;
    tracing::info!(
        workers = %state.known(),
        "routing at http://{}",
        options.bind
    );
    let listener = TcpListener::bind(options.bind)
        .await
        .with_context(|| format!("failed to bind {}", options.bind))?;
    axum::serve(listener, build_router(state))
        .with_graceful_shutdown(async {
            shutdown_signal().await;
            tracing::info!("shutdown signal received, draining in-flight requests");
        })
        .await
        .context("router failed")?;
    tracing::info!("router stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in Runtime: answers /search with fixed results and /readyz with `ready`.
    async fn fake_runtime(results: Value, ready: StatusCode) -> String {
        let app = Router::new()
            .route(
                "/search",
                axum::routing::get(move || async move {
                    axum::Json(json!({ "query": "q", "results": results }))
                }),
            )
            .route("/readyz", axum::routing::get(move || async move { ready }));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        url
    }

    async fn router(workers: Vec<(&str, String)>) -> String {
        let state = RouteState::new(
            workers
                .into_iter()
                .map(|(c, u)| (c.to_string(), u))
                .collect(),
        )
        .expect("state");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move {
            axum::serve(listener, build_router(state))
                .await
                .expect("serve")
        });
        url
    }

    async fn get(url: &str) -> (StatusCode, Value) {
        let client: Client<HttpConnector, Empty<Bytes>> =
            Client::builder(TokioExecutor::new()).build_http();
        let response = client
            .get(url.parse().expect("uri"))
            .await
            .expect("response");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
    }

    #[tokio::test]
    async fn country_goes_to_its_runtime_and_none_merges() {
        let au = fake_runtime(
            json!([{ "label": "au-a", "score": 5.0 }, { "label": "au-b", "score": 1.0 }]),
            StatusCode::OK,
        )
        .await;
        let nz = fake_runtime(json!([{ "label": "nz-a", "score": 3.0 }]), StatusCode::OK).await;
        let base = router(vec![("AU", au), ("nz", nz)]).await;

        let (status, body) = get(&format!("{base}/search?q=x&country=NZ")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["label"], "nz-a");
        assert!(
            body["results"][0].get("country").is_none(),
            "a forwarded answer is passed through untouched"
        );

        let (status, body) = get(&format!("{base}/search?q=x&limit=2")).await;
        assert_eq!(status, StatusCode::OK);
        let labels: Vec<_> = body["results"]
            .as_array()
            .expect("results")
            .iter()
            .map(|r| r["label"].clone())
            .collect();
        assert_eq!(labels, vec![json!("au-a"), json!("nz-a")]);
        assert_eq!(body["results"][1]["country"], "NZ");
    }

    #[tokio::test]
    async fn unknown_country_and_reverse_without_country_are_problems() {
        let au = fake_runtime(json!([]), StatusCode::OK).await;
        let base = router(vec![("AU", au)]).await;

        let (status, body) = get(&format!("{base}/search?q=x&country=FR")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error_code"], "unknown_country");

        let (status, body) = get(&format!("{base}/reverse?lon=1&lat=2")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error_code"], "country_required");

        let (status, body) = get(&format!("{base}/geocode?address=1+King+St")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error_code"], "country_required");
    }

    #[tokio::test]
    async fn ready_only_when_every_runtime_is() {
        let up = fake_runtime(json!([]), StatusCode::OK).await;
        let down = fake_runtime(json!([]), StatusCode::SERVICE_UNAVAILABLE).await;
        let (status, _) = get(&format!(
            "{}/readyz",
            router(vec![("AU", up.clone())]).await
        ))
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = get(&format!(
            "{}/readyz",
            router(vec![("AU", up), ("NZ", down)]).await
        ))
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn request_id_reaches_every_upstream_call() {
        // Echoes the x-request-id it was sent, in the body and on /readyz as a status.
        let echo = |headers: HeaderMap| async move {
            let id = headers
                .get("x-request-id")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            axum::Json(json!({ "query": "q", "results": [{ "label": id, "score": 1.0 }] }))
        };
        let app = Router::new()
            .route("/search", axum::routing::get(echo))
            .route(
                "/readyz",
                axum::routing::get(|headers: HeaderMap| async move {
                    if headers.get("x-request-id").is_some_and(|v| v == "ray-42") {
                        StatusCode::OK
                    } else {
                        StatusCode::IM_A_TEAPOT
                    }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let upstream = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        let base = router(vec![("AU", upstream)]).await;

        let client: Client<HttpConnector, Empty<Bytes>> =
            Client::builder(TokioExecutor::new()).build_http();
        let call = |path: &str| {
            let request = hyper::Request::get(format!("{base}{path}"))
                .header("cf-ray", "ray-42")
                .body(Empty::new())
                .expect("request");
            client.request(request)
        };

        for path in ["/search?q=x&country=au", "/search?q=x"] {
            let response = call(path).await.expect("response");
            assert_eq!(response.headers()["x-request-id"], "ray-42");
            let body = response.into_body().collect().await.expect("body");
            let body: Value = serde_json::from_slice(&body.to_bytes()).expect("json");
            assert_eq!(body["results"][0]["label"], "ray-42", "{path}");
        }
        let response = call("/readyz").await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn query_params_read_country_and_limit() {
        assert_eq!(
            query_params(Some("q=x&country=nz&limit=3")),
            (Some("NZ".to_string()), 3)
        );
        assert_eq!(query_params(None), (None, DEFAULT_SEARCH_LIMIT));
        // Values are percent-decoded.
        assert_eq!(
            query_params(Some("country=n%5A&limit=%31%30")),
            (Some("NZ".to_string()), 10)
        );
        // An empty country means none; a bad limit falls back to the default.
        assert_eq!(
            query_params(Some("q=a%26b&country=&limit=many")),
            (None, DEFAULT_SEARCH_LIMIT)
        );
    }

    fn answer(query: &str, items: Value) -> Value {
        json!({ "query": query, "results": items })
    }

    fn labels_and_countries(merged: &Value) -> Vec<(String, String)> {
        merged["results"]
            .as_array()
            .expect("results")
            .iter()
            .map(|r| {
                (
                    r["label"].as_str().unwrap_or_default().to_string(),
                    r["country"].as_str().unwrap_or_default().to_string(),
                )
            })
            .collect()
    }

    #[test]
    fn merge_prefers_full_coverage_over_a_larger_score() {
        let merged = merge(
            vec![
                (
                    "AU".to_string(),
                    answer(
                        "10 King Street",
                        json!([{ "label": "King Street Mall", "score": 900.0 }]),
                    ),
                ),
                (
                    "NZ".to_string(),
                    answer(
                        "10 King Street",
                        json!([{ "label": "10 King Street, Auckland", "score": 0.5 }]),
                    ),
                ),
            ],
            "results",
            10,
            false,
        );
        assert_eq!(
            labels_and_countries(&merged),
            vec![
                ("10 King Street, Auckland".to_string(), "NZ".to_string()),
                ("King Street Mall".to_string(), "AU".to_string()),
            ]
        );
        assert_eq!(merged["query"], "10 King Street");
    }

    #[test]
    fn merge_interleaves_equal_coverage_by_rank_and_cuts_to_limit() {
        let street = |score: f64| json!({ "label": "Main Street", "score": score });
        let merged = merge(
            vec![
                (
                    "AU".to_string(),
                    answer(
                        "main street",
                        json!([street(9.0), street(8.0), street(7.0)]),
                    ),
                ),
                (
                    "NZ".to_string(),
                    answer("main street", json!([street(1.0), street(0.5)])),
                ),
            ],
            "results",
            4,
            false,
        );
        let order: Vec<_> = merged["results"]
            .as_array()
            .expect("results")
            .iter()
            .map(|r| (r["country"].clone(), r["score"].clone()))
            .collect();
        assert_eq!(
            order,
            vec![
                (json!("AU"), json!(9.0)),
                (json!("NZ"), json!(1.0)),
                (json!("AU"), json!(8.0)),
                (json!("NZ"), json!(0.5)),
            ]
        );
    }

    #[test]
    fn merge_matches_a_prefix_only_for_the_last_autocomplete_token() {
        let answers = |query: &str| {
            vec![
                (
                    "AU".to_string(),
                    answer(query, json!([{ "label": "Main Road", "score": 50.0 }])),
                ),
                (
                    "NZ".to_string(),
                    answer(query, json!([{ "label": "Main Street", "score": 1.0 }])),
                ),
            ]
        };
        let first = |merged: Value| labels_and_countries(&merged)[0].1.clone();

        // "str" is the last token and a prefix of "street": NZ covers the whole query.
        assert_eq!(first(merge(answers("main str"), "results", 10, true)), "NZ");
        // Search matches whole tokens only, so both cover half and the score decides.
        assert_eq!(
            first(merge(answers("main str"), "results", 10, false)),
            "AU"
        );
        // A prefix that is not the last token does not count.
        assert_eq!(first(merge(answers("str main"), "results", 10, true)), "AU");
    }

    #[test]
    fn coverage_is_zero_for_an_empty_query() {
        assert_eq!(coverage(&tokens("  -- "), None, "Main Street", true), 0.0);
    }
}
