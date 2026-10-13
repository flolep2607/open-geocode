//! `open-geocode route`: one HTTP entry point in front of one Runtime per Pack.
//!
//! A Pack is built from one extract, and a Runtime serves one Pack, so a deployment covering
//! several countries runs several Runtimes. The router gives clients a single URL:
//!
//! - `/search`, `/autocomplete` and `/geocode` with `country=XX` go, unchanged, to that country's
//!   Runtime.
//! - Without `country` the router picks the country by street. Every Runtime serves a membership
//!   filter of the streets (and street|town pairs) in its Pack at `/routing-filter`; the router
//!   keeps them, refreshed on a timer and when a Runtime comes back ready, and routes a request
//!   to the one Runtime whose filter holds its street, first by street|town, then by street
//!   alone. Raw search scores of different Packs are not comparable, which is why scores are not
//!   used to choose. The street comes from `address` (`/geocode`) or `q` (`/search`,
//!   `/autocomplete`) with the house number and unit dropped, the town from `locality`. The street
//!   alone routes `/geocode`, and `/search` with a house number or a `locality`; `/autocomplete`
//!   routes only with a `locality`, because a name with no number may be a place, and a street
//!   typed so far may be the start of another. A Runtime without a filter might hold anything,
//!   so it blocks a single winner but never wins by default. The routed answer carries
//!   `x-route` (`filter-street-town` or `filter-street`) and `x-route-country`, and its results
//!   a `country`. A routed Runtime that fails, or finds nothing, does not end the request: the
//!   filters can be wrong, so it goes on to every Runtime.
//! - When the filters cannot decide (zero or several Runtimes hold the street, or a request with
//!   no street) the request goes to every Runtime at once, as before (`x-route: fanout`).
//!   `/search` and `/autocomplete` results are merged by score and cut to `limit`, each carrying
//!   the `country` it came from. `/geocode` answers one best match or `null`. A single non-null
//!   answer wins. Several do not pick a winner by score, which is not comparable across Packs,
//!   and a request with only an address gives the Runtimes no locality, region or postcode to
//!   check, so each answers for any street it has: unless exactly one of the answering Runtimes
//!   has a filter that holds the street, the router answers `country_required`, as it did
//!   before it could fan out `/geocode`.
//! - `/reverse` needs `country`: a coordinate carries no street to route by.
//! - `/readyz` is ready only when every Runtime is.
//!
//! The router holds no Pack and does no geocoding. It keeps the Runtime's boundary policy: GET
//! only, Problem Details for errors, request-id propagation.

use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use axum::{
    Extension, Router,
    body::Body,
    extract::{Query, RawQuery, State, rejection::QueryRejection},
    handler::{Handler, HandlerWithoutStateExt},
    http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode, header},
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
    },
    search::DEFAULT_SEARCH_LIMIT,
    street_filter::{RoutingFilter, pair_hash, query_street_hashes, town_hash},
};

/// How long the router waits for one Runtime before answering 502 (or leaving it out of a merge).
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(10);
/// A routing filter is megabytes to hundreds of them, so it gets longer than an answer does.
const FILTER_TIMEOUT: Duration = Duration::from_secs(60);

/// How soon a Runtime that has no filter is asked for one again, when the refresh interval is
/// longer.
const FILTER_RETRY: Duration = Duration::from_secs(15);

const ROUTE_HEADER: HeaderName = HeaderName::from_static("x-route");
const ROUTE_COUNTRY_HEADER: HeaderName = HeaderName::from_static("x-route-country");

#[derive(Debug, Clone)]
pub struct RouteOptions {
    /// (country code, Runtime base URL) pairs, e.g. ("NZ", "http://127.0.0.1:8081").
    pub workers: Vec<(String, String)>,
    pub bind: SocketAddr,
    /// How often every Runtime's routing filter is fetched again, so a swapped Pack is picked up
    /// without restarting the router. Zero turns the timer off: the filters are still fetched at
    /// startup and when a Runtime comes back ready.
    pub filter_refresh: Duration,
}

/// One Runtime behind the router.
struct Worker {
    /// Runtime base URL without a trailing slash.
    base: String,
    /// Held only for a read or a swap, never across an await.
    slot: RwLock<Slot>,
    /// Held for the whole of a refresh, so two of them (the timer and a Runtime coming back
    /// ready) cannot finish out of order and put an older filter over a newer one.
    refreshing: tokio::sync::Mutex<()>,
    /// What the last `/readyz` said, to spot the Runtime coming back. Only `/readyz` writes it.
    ready: AtomicBool,
}

#[derive(Default)]
struct Slot {
    /// ETag of the last filter body, so an unchanged filter is a 304.
    etag: Option<String>,
    /// `None` when the Runtime has no filter, or sent one this build cannot read.
    filter: Option<Arc<RoutingFilter<Bytes>>>,
}

impl Worker {
    /// A Runtime without a filter might hold anything, so it never lets another win.
    fn may_hold(&self, key: u64) -> bool {
        self.slot
            .read()
            .expect("filter slot")
            .filter
            .as_ref()
            .is_none_or(|filter| filter.maybe_contains(key))
    }

    /// Only a filter that says yes counts: a missing one proves nothing.
    fn confirms(&self, key: u64) -> bool {
        self.slot
            .read()
            .expect("filter slot")
            .filter
            .as_ref()
            .is_some_and(|filter| filter.maybe_contains(key))
    }
}

/// Where a request without a country goes.
#[derive(Debug, PartialEq, Eq)]
enum Decision {
    /// Only this country's filter holds the street with this town.
    StreetTown(String),
    /// Only this country's filter holds the street.
    Street(String),
    Fanout,
}

#[derive(Clone)]
struct RouteState {
    /// Country code (upper case) -> Runtime.
    workers: Arc<BTreeMap<String, Worker>>,
    client: Client<HttpConnector, Empty<Bytes>>,
}

/// A Runtime's answer.
struct Upstream {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl RouteState {
    fn new(workers: Vec<(String, String)>) -> Result<Self> {
        let mut map = BTreeMap::new();
        for (country, url) in workers {
            let country = country.trim().to_ascii_uppercase();
            let worker = Worker {
                base: url.trim_end_matches('/').to_string(),
                slot: RwLock::default(),
                refreshing: tokio::sync::Mutex::new(()),
                ready: AtomicBool::new(false),
            };
            if country.is_empty() || map.insert(country.clone(), worker).is_some() {
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

    /// GET `url`, or None when the Runtime did not answer in time.
    async fn fetch(
        &self,
        url: &str,
        if_none_match: Option<&str>,
        timeout: Duration,
    ) -> Option<Upstream> {
        let mut request = Request::get(url);
        if let Some(etag) = if_none_match {
            request = request.header(header::IF_NONE_MATCH, etag);
        }
        let request = request.body(Empty::new()).ok()?;
        let response = tokio::time::timeout(timeout, self.client.request(request))
            .await
            .ok()?
            .ok()?;
        let (parts, body) = response.into_parts();
        let body = tokio::time::timeout(timeout, body.collect())
            .await
            .ok()?
            .ok()?
            .to_bytes();
        Some(Upstream {
            status: parts.status,
            headers: parts.headers,
            body,
        })
    }

    /// GET `url`: (status, content type, body), or None when the Runtime did not answer in time.
    async fn get(&self, url: &str) -> Option<(StatusCode, Option<HeaderValue>, Bytes)> {
        let upstream = self.fetch(url, None, UPSTREAM_TIMEOUT).await?;
        let content_type = upstream.headers.get(header::CONTENT_TYPE).cloned();
        Some((upstream.status, content_type, upstream.body))
    }

    /// Fetch one Runtime's routing filter again. A held filter is kept when the Runtime does not
    /// answer (a stale filter routes better than none); one it no longer has, or sends in a
    /// format this build cannot read, is dropped, which only means the Runtime might hold
    /// anything.
    async fn refresh_worker(&self, country: &str) {
        let Some(worker) = self.workers.get(country) else {
            return;
        };
        let _refreshing = worker.refreshing.lock().await;
        let etag = worker.slot.read().expect("filter slot").etag.clone();
        let url = format!("{}/routing-filter", worker.base);
        let answer = self.fetch(&url, etag.as_deref(), FILTER_TIMEOUT).await;
        let Some(answer) = answer.filter(|answer| !answer.status.is_server_error()) else {
            return;
        };
        let slot = match answer.status {
            StatusCode::NOT_MODIFIED => return,
            StatusCode::OK => {
                let etag = answer
                    .headers
                    .get(header::ETAG)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                let filter = RoutingFilter::parse(answer.body)
                    .map(Arc::new)
                    .inspect_err(|error| {
                        // Once per body: the same bytes come back as a 304.
                        eprintln!("{country}: ignoring its routing filter: {error:#}");
                    })
                    .ok();
                Slot { etag, filter }
            }
            // 404 and anything else: this Runtime has no filter.
            _ => Slot::default(),
        };
        *worker.slot.write().expect("filter slot") = slot;
    }

    async fn refresh_filters(&self) {
        join_all(
            self.workers
                .keys()
                .map(|country| self.refresh_worker(country)),
        )
        .await;
    }

    /// Record what the last `/readyz` said. A Runtime that was not ready and now is has probably
    /// been restarted or given a new Pack, so its filter is fetched at once.
    fn note_ready(&self, country: &str, ready: bool) {
        let Some(worker) = self.workers.get(country) else {
            return;
        };
        if worker.ready.swap(ready, Ordering::AcqRel) || !ready {
            return;
        }
        let state = self.clone();
        let country = country.to_string();
        tokio::spawn(async move { state.refresh_worker(&country).await });
    }

    /// Whether some Runtime has no filter now: down at startup, a Pack without one, or one this
    /// build cannot read.
    fn any_without_filter(&self) -> bool {
        self.workers
            .values()
            .any(|worker| worker.slot.read().expect("filter slot").filter.is_none())
    }

    /// The only country whose filter can hold `key`, and whose filter says it does. A Runtime
    /// without a filter might hold anything, so it blocks a single winner, but it never wins by
    /// default: when every filter that exists says no, nothing was confirmed.
    fn sole_holder(&self, key: u64) -> Option<String> {
        let mut holders = self
            .workers
            .iter()
            .filter(|(_, worker)| worker.may_hold(key));
        let (first, worker) = holders.next()?;
        (holders.next().is_none() && worker.confirms(key)).then(|| first.clone())
    }

    /// For each reading of the street, street with town first, then (when `street_alone`) the
    /// street alone. Several holders, or none, decide nothing: a Runtime is never chosen because
    /// another one does not know the street, only because it is the one that does.
    fn decide(&self, keys: &RouteKeys) -> Decision {
        for &street in &keys.streets {
            if let Some(town) = keys.town
                && let Some(country) = self.sole_holder(pair_hash(street, town))
            {
                return Decision::StreetTown(country);
            }
            if keys.street_alone
                && let Some(country) = self.sole_holder(street)
            {
                return Decision::Street(country);
            }
        }
        Decision::Fanout
    }

    /// One line per Runtime for the startup log.
    fn filter_summary(&self) -> String {
        self.workers
            .iter()
            .map(
                |(country, worker)| match &worker.slot.read().expect("filter slot").filter {
                    Some(filter) => {
                        format!("{country}: routing filter of {} keys", filter.key_count())
                    }
                    None => format!("{country}: no routing filter"),
                },
            )
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Parse `country=` and `limit=` out of a raw query string; the query itself is forwarded as is.
fn query_params(raw: Option<&str>) -> (Option<String>, usize) {
    let mut country = None;
    let mut limit = DEFAULT_SEARCH_LIMIT;
    for pair in raw.unwrap_or_default().split('&') {
        match pair.split_once('=') {
            Some(("country", value)) if !value.is_empty() => {
                country = Some(value.to_ascii_uppercase())
            }
            Some(("limit", value)) => limit = value.parse().unwrap_or(DEFAULT_SEARCH_LIMIT),
            _ => {}
        }
    }
    (country, limit)
}

/// The decoded query, to read the street and town of a request from. A query that cannot be
/// decoded routes nowhere: the Runtimes will answer it as the bad request it is.
type Params = Result<Query<HashMap<String, String>>, QueryRejection>;

/// What a request's text is, which decides how far its street can be trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// `/geocode`'s `address`: the caller says it is an address.
    Address,
    /// `/search`'s `q`: anything. A name with no number could be a place or a POI that is also a
    /// street somewhere, so alone it only routes with a house number or a `locality`.
    Query,
    /// `/autocomplete`'s `q`: what is typed so far. "Broadway" is a street while someone is still
    /// typing "Broadwater Road", so the street alone never routes, only with a `locality`.
    Typing,
}

/// Hashes of the streets (from the `text` parameter) and the town (`locality`) of a request.
#[derive(Debug, Clone, Default)]
struct RouteKeys {
    /// The readings of the street, most likely first.
    streets: Vec<u64>,
    town: Option<u64>,
    /// Whether the street alone, without the town, is enough to route.
    street_alone: bool,
}

impl RouteKeys {
    fn new(params: &Params, text: &str, source: Source) -> Self {
        let Ok(Query(params)) = params else {
            return Self::default();
        };
        let text = params.get(text).map(String::as_str).unwrap_or_default();
        let town = params.get("locality").and_then(|town| town_hash(town));
        let has_number = text
            .split_whitespace()
            .any(|token| token.chars().any(|c| c.is_ascii_digit()));
        Self {
            streets: query_street_hashes(text),
            town,
            street_alone: match source {
                Source::Address => true,
                Source::Query => has_number || town.is_some(),
                Source::Typing => false,
            },
        }
    }
}

/// How an endpoint answers: a list of results, or one best match.
#[derive(Clone, Copy)]
enum Shape {
    /// `/search`, `/autocomplete`: the array under this key.
    List(&'static str),
    /// `/geocode`: `result`, an object or null.
    Best,
}

impl Shape {
    fn key(self) -> &'static str {
        match self {
            Self::List(key) => key,
            Self::Best => "result",
        }
    }
}

/// Tag every result of `body[key]` with the `country` it came from.
fn tag_country(body: &mut Value, key: &str, country: &str) {
    let tag = |item: &mut Value| {
        if let Some(object) = item.as_object_mut() {
            object.insert("country".to_string(), Value::String(country.to_string()));
        }
    };
    match body.get_mut(key) {
        Some(Value::Array(items)) => items.iter_mut().for_each(tag),
        Some(item @ Value::Object(_)) => tag(item),
        _ => {}
    }
}

/// Merge each Runtime's `key` array into one, best score first, cut to `limit`, each tagged with
/// its country. `answers` are (country, parsed response body) pairs.
fn merge(answers: Vec<(String, Value)>, key: &str, limit: usize) -> Value {
    let query = answers
        .iter()
        .find_map(|(_, body)| body.get("query").cloned())
        .unwrap_or(Value::Null);
    let mut merged: Vec<Value> = answers
        .into_iter()
        .flat_map(|(country, mut body)| {
            tag_country(&mut body, key, &country);
            match body.get_mut(key).map(Value::take) {
                Some(Value::Array(items)) => items,
                _ => Vec::new(),
            }
        })
        .collect();
    let score = |item: &Value| {
        item.get("score")
            .and_then(Value::as_f64)
            .unwrap_or(f64::MIN)
    };
    merged.sort_by(|a, b| score(b).total_cmp(&score(a)));
    merged.truncate(limit);
    json!({ "query": query, key: merged })
}

/// What a `/geocode` fan-out came to.
enum Picked {
    /// One Runtime's non-null answer, and its country.
    One(Value, String),
    /// Every Runtime said null: the first one's body.
    Null(Value),
    /// Several Runtimes found an address and nothing tells them apart; the countries.
    Ambiguous(Vec<String>),
}

/// The one answer of a `/geocode` fan-out. A non-null `result` beats `null` from the others. With
/// several, the Runtimes whose filter holds the street are the ones to believe: if exactly one of
/// them answered, it wins. Otherwise the request does not say which country it means, and the
/// raw score of one Pack against another is not evidence of that: a street held by several
/// countries makes every Runtime answer, and a request with only an address has no locality,
/// region or postcode for the Runtime to check, so a non-null answer proves little. That is
/// `Ambiguous`, which the caller turns into `country_required`.
fn pick_geocode(state: &RouteState, answers: Vec<(String, Value)>, streets: &[u64]) -> Picked {
    let holds = |country: &str| {
        state
            .workers
            .get(country)
            .is_some_and(|worker| streets.iter().any(|street| worker.confirms(*street)))
    };
    let mut found: Vec<(String, Value)> = Vec::new();
    let mut first_body = None;
    for (country, body) in answers {
        if body.get("result").is_some_and(|result| !result.is_null()) {
            found.push((country, body));
        } else if first_body.is_none() {
            first_body = Some(body);
        }
    }
    if found.len() > 1 {
        let holders = found.iter().filter(|(country, _)| holds(country)).count();
        if holders == 1 {
            found.retain(|(country, _)| holds(country));
        }
    }
    match found.len() {
        0 => Picked::Null(first_body.unwrap_or(Value::Null)),
        1 => {
            let (country, mut body) = found.remove(0);
            tag_country(&mut body, "result", &country);
            Picked::One(body, country)
        }
        _ => Picked::Ambiguous(found.into_iter().map(|(country, _)| country).collect()),
    }
}

/// Whether a Runtime's answer has anything in it: a non-empty list, a non-null result.
fn has_answer(body: &Value, shape: Shape) -> bool {
    match body.get(shape.key()) {
        Some(Value::Array(items)) => !items.is_empty(),
        Some(Value::Null) | None => false,
        Some(_) => true,
    }
}

fn with_id(problem: Problem, request_id: &Option<Extension<RequestId>>) -> Response {
    match request_id {
        Some(Extension(RequestId(id))) => problem.with_request_id(id.clone()).into_response(),
        None => problem.into_response(),
    }
}

/// A Runtime's answer handed back as it came.
fn passthrough(status: StatusCode, content_type: Option<HeaderValue>, body: Bytes) -> Response {
    let mut response = (status, Body::from(body)).into_response();
    if let Some(content_type) = content_type {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    response
}

/// Say how the request was routed: `x-route`, and `x-route-country` when one country answered.
fn tagged(mut response: Response, route: &'static str, country: Option<&str>) -> Response {
    response
        .headers_mut()
        .insert(ROUTE_HEADER, HeaderValue::from_static(route));
    if let Some(country) = country
        && let Ok(value) = HeaderValue::from_str(country)
    {
        response.headers_mut().insert(ROUTE_COUNTRY_HEADER, value);
    }
    response
}

/// Forward `path?suffix` to one country's Runtime, answer untouched.
async fn forward_to_country(
    state: &RouteState,
    path: &str,
    suffix: &str,
    country: &str,
    request_id: &Option<Extension<RequestId>>,
) -> Response {
    let Some(worker) = state.workers.get(country) else {
        return with_id(
            Problem::unknown_country(country, &state.known()),
            request_id,
        );
    };
    match state.get(&format!("{}{path}{suffix}", worker.base)).await {
        Some((status, content_type, body)) => tagged(
            passthrough(status, content_type, body),
            "country",
            Some(country),
        ),
        None => with_id(Problem::upstream_unavailable(country), request_id),
    }
}

/// Ask every Runtime at once. A Runtime's 4xx (a bad query) is every Runtime's, so the first one
/// is handed back as the `Err`; one that failed or did not answer is left out.
async fn fan_out(
    state: &RouteState,
    path: &str,
    suffix: &str,
) -> Result<Vec<(String, Value)>, Box<Response>> {
    let calls = state.workers.iter().map(|(country, worker)| {
        let url = format!("{}{path}{suffix}", worker.base);
        async move { (country.clone(), state.get(&url).await) }
    });
    let mut answers = Vec::new();
    for (country, answer) in join_all(calls).await {
        match answer {
            Some((status, content_type, body)) if status.is_client_error() => {
                return Err(Box::new(passthrough(status, content_type, body)));
            }
            Some((status, _, body)) if status.is_success() => {
                if let Ok(value) = serde_json::from_slice::<Value>(&body) {
                    answers.push((country, value));
                }
            }
            _ => {}
        }
    }
    Ok(answers)
}

/// Answer `path?raw` from the one Runtime the request names (`country=XX`) or the filters pick,
/// else from all of them.
async fn forward_or_merge(
    state: &RouteState,
    path: &str,
    shape: Shape,
    raw: Option<String>,
    keys: RouteKeys,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    let (country, limit) = query_params(raw.as_deref());
    let suffix = raw.map(|q| format!("?{q}")).unwrap_or_default();

    if let Some(country) = country {
        return forward_to_country(state, path, &suffix, &country, &request_id).await;
    }

    let decision = state.decide(&keys);
    let routed = match &decision {
        Decision::StreetTown(country) => Some((country, "filter-street-town")),
        Decision::Street(country) => Some((country, "filter-street")),
        Decision::Fanout => None,
    };
    if let Some((country, route)) = routed {
        let url = format!("{}{path}{suffix}", state.workers[country].base);
        match state.get(&url).await {
            Some((status, content_type, body)) if status.is_client_error() => {
                return tagged(
                    passthrough(status, content_type, body),
                    route,
                    Some(country),
                );
            }
            Some((status, content_type, body)) if status.is_success() => {
                match serde_json::from_slice::<Value>(&body) {
                    // A filter can be wrong (a false positive, a town spelled another way), and
                    // an empty answer from the wrong country must not end the search.
                    Ok(value) if !has_answer(&value, shape) => {}
                    Ok(mut value) => {
                        tag_country(&mut value, shape.key(), country);
                        return tagged(axum::Json(value).into_response(), route, Some(country));
                    }
                    Err(_) => {
                        return tagged(
                            passthrough(status, content_type, body),
                            route,
                            Some(country),
                        );
                    }
                }
            }
            // The chosen Runtime failed, did not answer or found nothing: the others may still
            // know the street.
            _ => {}
        }
    }

    let answers = match fan_out(state, path, &suffix).await {
        Ok(answers) if !answers.is_empty() => answers,
        Ok(_) => return with_id(Problem::upstream_unavailable("any"), &request_id),
        Err(response) => return *response,
    };
    match shape {
        Shape::List(key) => tagged(
            axum::Json(merge(answers, key, limit)).into_response(),
            "fanout",
            None,
        ),
        Shape::Best => match pick_geocode(state, answers, &keys.streets) {
            Picked::One(body, country) => {
                tagged(axum::Json(body).into_response(), "fanout", Some(&country))
            }
            Picked::Null(body) => tagged(axum::Json(body).into_response(), "fanout", None),
            Picked::Ambiguous(countries) => tagged(
                with_id(
                    Problem::country_ambiguous("/geocode", &countries),
                    &request_id,
                ),
                "fanout",
                None,
            ),
        },
    }
}

async fn search(
    State(state): State<RouteState>,
    request_id: Option<Extension<RequestId>>,
    RawQuery(raw): RawQuery,
    params: Params,
) -> Response {
    let keys = RouteKeys::new(&params, "q", Source::Query);
    forward_or_merge(
        &state,
        "/search",
        Shape::List("results"),
        raw,
        keys,
        request_id,
    )
    .await
}

async fn autocomplete(
    State(state): State<RouteState>,
    request_id: Option<Extension<RequestId>>,
    RawQuery(raw): RawQuery,
    params: Params,
) -> Response {
    let keys = RouteKeys::new(&params, "q", Source::Typing);
    forward_or_merge(
        &state,
        "/autocomplete",
        Shape::List("suggestions"),
        raw,
        keys,
        request_id,
    )
    .await
}

/// `/geocode` answers with one best match: routed by `address` and `locality`, else every Runtime
/// is asked and the one non-null answer wins.
async fn geocode(
    State(state): State<RouteState>,
    request_id: Option<Extension<RequestId>>,
    RawQuery(raw): RawQuery,
    params: Params,
) -> Response {
    let keys = RouteKeys::new(&params, "address", Source::Address);
    forward_or_merge(&state, "/geocode", Shape::Best, raw, keys, request_id).await
}

/// `/reverse` has no street to route by, so like before it needs `country`.
async fn reverse(
    State(state): State<RouteState>,
    request_id: Option<Extension<RequestId>>,
    RawQuery(raw): RawQuery,
) -> Response {
    let (country, _) = query_params(raw.as_deref());
    let Some(country) = country else {
        return with_id(Problem::country_required("/reverse"), &request_id);
    };
    let suffix = raw.map(|q| format!("?{q}")).unwrap_or_default();
    forward_to_country(&state, "/reverse", &suffix, &country, &request_id).await
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}

/// Ready only when every Runtime answers its own `/readyz` with 200.
async fn readyz(State(state): State<RouteState>) -> StatusCode {
    let calls = state.workers.iter().map(|(country, worker)| {
        let url = format!("{}/readyz", worker.base);
        let state = &state;
        async move {
            let ready = state.get(&url).await.map(|(status, _, _)| status) == Some(StatusCode::OK);
            state.note_ready(country, ready);
            ready
        }
    });
    let all_ready = join_all(calls).await.into_iter().all(|ready| ready);
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
        .layer(middleware::from_fn(request_id::propagate))
        .with_state(state)
}

pub async fn route(options: RouteOptions) -> Result<()> {
    let state = RouteState::new(options.workers)?;
    println!("Routing {} at http://{}", state.known(), options.bind);
    let listener = TcpListener::bind(options.bind)
        .await
        .with_context(|| format!("failed to bind {}", options.bind))?;
    // The listener comes up first: a Runtime that is slow or down now only means "might hold
    // anything" until its filter arrives.
    let refresh = options.filter_refresh;
    let background = state.clone();
    tokio::spawn(async move {
        background.refresh_filters().await;
        println!("{}", background.filter_summary());
        // Zero turns the timer off.
        while !refresh.is_zero() {
            // A Runtime without a filter is asked again soon: it may only have been starting.
            let wait = if background.any_without_filter() {
                refresh.min(FILTER_RETRY)
            } else {
                refresh
            };
            tokio::time::sleep(wait).await;
            background.refresh_filters().await;
        }
    });
    axum::serve(listener, build_router(state))
        .await
        .context("router failed")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU16, AtomicU64, AtomicUsize};

    use xxhash_rust::xxh3::xxh3_64;

    use super::*;
    use crate::street_filter::{FilterEncoder, street_hash};

    /// A stand-in Runtime. It answers `/search` and `/autocomplete` with fixed results, `/geocode`
    /// with a fixed result, `/readyz` with a status the test can change, and `/routing-filter`
    /// with bytes the test can swap, with an ETag and a 304 like the real one.
    #[derive(Clone)]
    struct Fake {
        url: String,
        /// The filter served; `None` is a Runtime whose Pack has none (404).
        filter: Arc<RwLock<Option<Vec<u8>>>>,
        ready: Arc<AtomicU16>,
        /// Requests to /search, /autocomplete and /geocode.
        hits: Arc<AtomicUsize>,
        /// /routing-filter answered 200 and 304.
        sent: Arc<AtomicUsize>,
        not_modified: Arc<AtomicUsize>,
        /// Status /search, /autocomplete and /geocode answer instead of their result; 0 for none.
        fail_with: Arc<AtomicU16>,
        /// Make /routing-filter answer 500.
        filter_broken: Arc<AtomicBool>,
        /// Milliseconds /routing-filter waits after it has read its bytes, before it answers.
        filter_delay: Arc<AtomicU64>,
    }

    impl Fake {
        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }
    }

    async fn fake(
        results: Value,
        geocode: Value,
        filter: Option<Vec<u8>>,
        ready: StatusCode,
    ) -> Fake {
        let fake = Fake {
            url: String::new(),
            filter: Arc::new(RwLock::new(filter)),
            ready: Arc::new(AtomicU16::new(ready.as_u16())),
            hits: Arc::default(),
            sent: Arc::default(),
            not_modified: Arc::default(),
            fail_with: Arc::default(),
            filter_broken: Arc::default(),
            filter_delay: Arc::default(),
        };
        let answer = |fake: &Fake, body: Value| {
            let (hits, fail_with) = (fake.hits.clone(), fake.fail_with.clone());
            move || {
                let (hits, fail_with, body) = (hits.clone(), fail_with.clone(), body.clone());
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    match fail_with.load(Ordering::SeqCst) {
                        0 => axum::Json(body).into_response(),
                        status => StatusCode::from_u16(status)
                            .expect("status")
                            .into_response(),
                    }
                }
            }
        };
        let routing_filter = {
            let fake = fake.clone();
            move |headers: HeaderMap| {
                let fake = fake.clone();
                async move {
                    if fake.filter_broken.load(Ordering::SeqCst) {
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    }
                    let Some(bytes) = fake.filter.read().unwrap().clone() else {
                        return StatusCode::NOT_FOUND.into_response();
                    };
                    let delay = fake.filter_delay.load(Ordering::SeqCst);
                    if delay > 0 {
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                    let etag = format!("\"{:016x}\"", xxh3_64(&bytes));
                    let mut response = if headers
                        .get(header::IF_NONE_MATCH)
                        .and_then(|value| value.to_str().ok())
                        == Some(etag.as_str())
                    {
                        fake.not_modified.fetch_add(1, Ordering::SeqCst);
                        StatusCode::NOT_MODIFIED.into_response()
                    } else {
                        fake.sent.fetch_add(1, Ordering::SeqCst);
                        bytes.into_response()
                    };
                    response
                        .headers_mut()
                        .insert(header::ETAG, HeaderValue::from_str(&etag).unwrap());
                    response
                }
            }
        };
        let status = fake.ready.clone();
        let app =
            Router::new()
                .route(
                    "/search",
                    axum::routing::get(answer(
                        &fake,
                        json!({ "query": "q", "results": results.clone() }),
                    )),
                )
                .route(
                    "/autocomplete",
                    axum::routing::get(answer(
                        &fake,
                        json!({ "query": "q", "suggestions": results }),
                    )),
                )
                .route(
                    "/geocode",
                    axum::routing::get(answer(&fake, json!({ "query": "q", "result": geocode }))),
                )
                .route("/routing-filter", axum::routing::get(routing_filter))
                .route(
                    "/readyz",
                    axum::routing::get(move || {
                        let status = status.clone();
                        async move {
                            StatusCode::from_u16(status.load(Ordering::SeqCst)).expect("status")
                        }
                    }),
                );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        Fake { url, ..fake }
    }

    /// The Runtime of the first tests: fixed search results, no filter.
    async fn fake_runtime(results: Value, ready: StatusCode) -> String {
        fake(results, Value::Null, None, ready).await.url
    }

    /// A routing filter holding these streets, each with the towns given.
    fn filter_of(streets: &[(&str, &[&str])]) -> Vec<u8> {
        let mut keys = Vec::new();
        for (street, towns) in streets {
            let hash = street_hash(street).expect("street");
            keys.push(hash);
            for town in *towns {
                keys.push(pair_hash(hash, town_hash(town).expect("town")));
            }
        }
        keys.sort_unstable();
        keys.dedup();
        let mut encoder = FilterEncoder::new(Vec::new(), 0).expect("encoder");
        encoder.push_shard(&keys).expect("shard");
        encoder.finish(keys.len() as u64).expect("finish")
    }

    /// Serve the router like `route` does: filters fetched before the first request.
    async fn router_state(workers: Vec<(&str, String)>) -> (String, RouteState) {
        let state = RouteState::new(
            workers
                .into_iter()
                .map(|(c, u)| (c.to_string(), u))
                .collect(),
        )
        .expect("state");
        state.refresh_filters().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        let app = build_router(state.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        (url, state)
    }

    async fn router(workers: Vec<(&str, String)>) -> String {
        router_state(workers).await.0
    }

    async fn get_full(url: &str) -> (StatusCode, HeaderMap, Value) {
        let client: Client<HttpConnector, Empty<Bytes>> =
            Client::builder(TokioExecutor::new()).build_http();
        let response = client
            .get(url.parse().expect("uri"))
            .await
            .expect("response");
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        (
            status,
            headers,
            serde_json::from_slice(&body).unwrap_or(Value::Null),
        )
    }

    async fn get(url: &str) -> (StatusCode, Value) {
        let (status, _, body) = get_full(url).await;
        (status, body)
    }

    fn route_of(headers: &HeaderMap) -> (Option<&str>, Option<&str>) {
        let header = |name: &HeaderName| headers.get(name).and_then(|value| value.to_str().ok());
        (header(&ROUTE_HEADER), header(&ROUTE_COUNTRY_HEADER))
    }

    /// The keys of a request that names this street, and maybe a town.
    fn keys(street: &str, town: Option<&str>, street_alone: bool) -> RouteKeys {
        RouteKeys {
            streets: vec![street_hash(street).expect("street")],
            town: town.map(|town| town_hash(town).expect("town")),
            street_alone,
        }
    }

    /// Three countries that share the street "King Street" and hold others alone.
    struct Countries {
        base: String,
        au: Fake,
        nz: Fake,
        ca: Fake,
    }

    async fn countries(geocode: [Value; 3]) -> Countries {
        let [au_geocode, nz_geocode, ca_geocode] = geocode;
        let au = fake(
            json!([{ "label": "au", "score": 4.0 }]),
            au_geocode,
            Some(filter_of(&[
                ("King Street", &["Sydney"]),
                ("George Street", &["Sydney"]),
            ])),
            StatusCode::OK,
        )
        .await;
        let nz = fake(
            json!([{ "label": "nz", "score": 3.0 }]),
            nz_geocode,
            Some(filter_of(&[
                ("King Street", &["Auckland"]),
                ("Eruera Street", &["Rotorua"]),
            ])),
            StatusCode::OK,
        )
        .await;
        let ca = fake(
            json!([{ "label": "ca", "score": 2.0 }]),
            ca_geocode,
            Some(filter_of(&[("King Street", &["Toronto"])])),
            StatusCode::OK,
        )
        .await;
        let base = router(vec![
            ("AU", au.url.clone()),
            ("NZ", nz.url.clone()),
            ("CA", ca.url.clone()),
        ])
        .await;
        Countries { base, au, nz, ca }
    }

    #[tokio::test]
    async fn country_goes_to_its_runtime_and_none_merges_by_score() {
        let au = fake_runtime(
            json!([{ "label": "au-a", "score": 5.0 }, { "label": "au-b", "score": 1.0 }]),
            StatusCode::OK,
        )
        .await;
        let nz = fake_runtime(json!([{ "label": "nz-a", "score": 3.0 }]), StatusCode::OK).await;
        let base = router(vec![("AU", au), ("nz", nz)]).await;

        let (status, headers, body) = get_full(&format!("{base}/search?q=x&country=NZ")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["label"], "nz-a");
        assert!(
            body["results"][0].get("country").is_none(),
            "a forwarded answer is passed through untouched"
        );
        assert_eq!(route_of(&headers), (Some("country"), Some("NZ")));

        // No Runtime has a filter, so none can be told apart: every one is asked.
        let (status, headers, body) = get_full(&format!("{base}/search?q=x&limit=2")).await;
        assert_eq!(status, StatusCode::OK);
        let labels: Vec<_> = body["results"]
            .as_array()
            .expect("results")
            .iter()
            .map(|r| r["label"].clone())
            .collect();
        assert_eq!(labels, vec![json!("au-a"), json!("nz-a")]);
        assert_eq!(body["results"][1]["country"], "NZ");
        assert_eq!(route_of(&headers), (Some("fanout"), None));
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

        let (status, body) = get(&format!("{base}/geocode?address=1+King+St&country=FR")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error_code"], "unknown_country");
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

    #[test]
    fn query_params_read_country_and_limit() {
        assert_eq!(
            query_params(Some("q=x&country=nz&limit=3")),
            (Some("NZ".to_string()), 3)
        );
        assert_eq!(query_params(None), (None, DEFAULT_SEARCH_LIMIT));
    }

    #[tokio::test]
    async fn a_street_and_town_held_by_one_country_routes_there() {
        let nz_hit = json!({ "label": "15 Eruera Street, Rotorua", "score": 9.0 });
        let au_hit = json!({ "label": "12 King Street, Sydney", "score": 9.0 });
        let c = countries([au_hit, nz_hit, Value::Null]).await;

        let (status, headers, body) = get_full(&format!(
            "{}/geocode?address=15+Eruera+Street&locality=Rotorua",
            c.base
        ))
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(route_of(&headers), (Some("filter-street-town"), Some("NZ")));
        assert_eq!(body["result"]["country"], "NZ");
        assert_eq!((c.au.hits(), c.nz.hits(), c.ca.hits()), (0, 1, 0));

        // The street is in three countries, the street in this town in one.
        let (_, headers, _) = get_full(&format!(
            "{}/geocode?address=3%2F12+King+St&locality=Sydney",
            c.base
        ))
        .await;
        assert_eq!(route_of(&headers), (Some("filter-street-town"), Some("AU")));
        assert_eq!((c.au.hits(), c.nz.hits(), c.ca.hits()), (1, 1, 0));
    }

    #[tokio::test]
    async fn a_street_held_by_one_country_routes_there_without_a_town() {
        let hit = json!({ "label": "1 George Street", "score": 1.0 });
        let c = countries([hit, Value::Null, Value::Null]).await;

        // The town is held nowhere, but the street only by AU.
        for query in [
            "address=1+George+St",
            "address=1+George+St&locality=Nowhere",
        ] {
            let (_, headers, _) = get_full(&format!("{}/geocode?{query}", c.base)).await;
            assert_eq!(
                route_of(&headers),
                (Some("filter-street"), Some("AU")),
                "{query}"
            );
        }
        assert_eq!((c.nz.hits(), c.ca.hits()), (0, 0));
    }

    #[tokio::test]
    async fn a_street_held_by_several_or_none_fans_out() {
        let c = countries([Value::Null, Value::Null, Value::Null]).await;

        for query in [
            // Three countries hold it and the town is in none of them.
            "address=1+King+St",
            "address=1+King+Street&locality=Wellington",
            // None holds it (a false positive could only add a holder, not remove one).
            "address=1+Unknown+Road&locality=Rotorua",
            // No street at all.
            "address=Level+3",
        ] {
            let (status, headers, _) = get_full(&format!("{}/geocode?{query}", c.base)).await;
            assert_eq!(status, StatusCode::OK, "{query}");
            assert_eq!(route_of(&headers).0, Some("fanout"), "{query}");
        }
        assert_eq!((c.au.hits(), c.nz.hits(), c.ca.hits()), (4, 4, 4));
    }

    #[tokio::test]
    async fn search_and_autocomplete_route_by_the_street_of_the_query() {
        let c = countries([Value::Null, Value::Null, Value::Null]).await;

        // With a town, both route on street and town.
        for path in ["search", "autocomplete"] {
            let key = if path == "search" {
                "results"
            } else {
                "suggestions"
            };
            let (status, headers, body) = get_full(&format!(
                "{}/{path}?q=15+Eruera+Street&locality=Rotorua",
                c.base
            ))
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(
                route_of(&headers),
                (Some("filter-street-town"), Some("NZ")),
                "{path}"
            );
            assert_eq!(body[key][0]["label"], "nz");
            assert_eq!(body[key][0]["country"], "NZ", "{path}");
        }
        assert_eq!((c.au.hits(), c.nz.hits(), c.ca.hits()), (0, 2, 0));

        // A search with a house number routes on the street alone.
        let (_, headers, _) = get_full(&format!("{}/search?q=1+George+Street", c.base)).await;
        assert_eq!(route_of(&headers), (Some("filter-street"), Some("AU")));

        // The same street in three countries merges by score as before, every item tagged.
        let (_, headers, body) = get_full(&format!("{}/search?q=1+King+St", c.base)).await;
        assert_eq!(route_of(&headers), (Some("fanout"), None));
        let countries: Vec<_> = body["results"]
            .as_array()
            .expect("results")
            .iter()
            .map(|item| item["country"].clone())
            .collect();
        assert_eq!(countries, vec![json!("AU"), json!("NZ"), json!("CA")]);
    }

    #[tokio::test]
    async fn text_that_may_be_a_place_or_half_a_word_does_not_route_on_the_street_alone() {
        let c = countries([Value::Null, Value::Null, Value::Null]).await;

        // Only AU holds George Street, but a name with no number may be a place, and "George
        // Street" typed so far may be the start of another street, so these ask everyone.
        for query in [
            "search?q=George+Street",
            "autocomplete?q=George+Street",
            "autocomplete?q=15+George+Street",
        ] {
            let (_, headers, _) = get_full(&format!("{}/{query}", c.base)).await;
            assert_eq!(route_of(&headers).0, Some("fanout"), "{query}");
        }
        // A town makes the name an address, in the one place that has it.
        let (_, headers, _) = get_full(&format!(
            "{}/search?q=George+Street&locality=Sydney",
            c.base
        ))
        .await;
        assert_eq!(route_of(&headers), (Some("filter-street-town"), Some("AU")));
        // Sydney has no Eruera Street, so a search names the street alone, in NZ only.
        let (_, headers, _) = get_full(&format!(
            "{}/search?q=Eruera+Street&locality=Sydney",
            c.base
        ))
        .await;
        assert_eq!(route_of(&headers), (Some("filter-street"), Some("NZ")));
    }

    #[tokio::test]
    async fn a_runtime_without_a_usable_filter_might_hold_anything() {
        let street = filter_of(&[("Eruera Street", &["Rotorua"])]);
        let query = "address=15+Eruera+Street&locality=Rotorua";
        let hit = json!({ "label": "15 Eruera Street", "score": 1.0 });
        let holds = fake(json!([]), hit, Some(street.clone()), StatusCode::OK).await;
        let none = fake(json!([]), Value::Null, None, StatusCode::OK).await;
        let bogus = fake(
            json!([]),
            Value::Null,
            Some(b"this is not a filter".to_vec()),
            StatusCode::OK,
        )
        .await;

        // The other Runtime proves it cannot hold the street, so the one that does wins.
        let other = fake(
            json!([]),
            Value::Null,
            Some(filter_of(&[("Elm Street", &[])])),
            StatusCode::OK,
        )
        .await;
        let base = router(vec![("NZ", holds.url.clone()), ("AU", other.url.clone())]).await;
        let (_, headers, _) = get_full(&format!("{base}/geocode?{query}")).await;
        assert_eq!(route_of(&headers), (Some("filter-street-town"), Some("NZ")));

        // No filter, or one nobody can read, proves nothing: it blocks a single winner.
        for blocker in [&none, &bogus] {
            let base = router(vec![("NZ", holds.url.clone()), ("AU", blocker.url.clone())]).await;
            let (_, headers, _) = get_full(&format!("{base}/geocode?{query}")).await;
            assert_eq!(route_of(&headers).0, Some("fanout"));
        }

        // And it does not win by default either: when every filter that exists says no, nothing
        // confirmed the street, so the request is not called routed.
        for unknown in [&none, &bogus] {
            let base = router(vec![("NZ", other.url.clone()), ("AU", unknown.url.clone())]).await;
            let (_, headers, _) = get_full(&format!("{base}/geocode?{query}")).await;
            assert_eq!(route_of(&headers).0, Some("fanout"));
        }
    }

    #[tokio::test]
    async fn geocode_fan_out_picks_the_one_runtime_that_found_the_address() {
        let hit = |score: f64| json!({ "label": "somewhere", "score": score });

        // Exactly one non-null answer wins, whatever the others said.
        let c = countries([Value::Null, hit(1.0), Value::Null]).await;
        let (_, headers, body) = get_full(&format!("{}/geocode?address=1+King+St", c.base)).await;
        assert_eq!(route_of(&headers), (Some("fanout"), Some("NZ")));
        assert_eq!(body["result"]["country"], "NZ");

        // All null: the answer is a null result.
        let c = countries([Value::Null, Value::Null, Value::Null]).await;
        let (status, headers, body) =
            get_full(&format!("{}/geocode?address=1+King+St", c.base)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(route_of(&headers), (Some("fanout"), None));
        assert!(body["result"].is_null());

        // Several, all holding the street: nothing tells them apart, and the score of one Pack
        // against another's is not evidence, so the caller is asked for a country.
        let c = countries([hit(2.0), hit(9.0), hit(5.0)]).await;
        let (status, headers, body) =
            get_full(&format!("{}/geocode?address=1+King+St", c.base)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(route_of(&headers), (Some("fanout"), None));
        assert_eq!(body["error_code"], "country_required");
        assert!(body["detail"].as_str().unwrap().contains("AU, CA, NZ"));

        // Of two answers, the one from the Runtime whose filter holds the street is believed over
        // one from a Runtime that has no filter and so proves nothing, whatever their scores.
        let holder = fake(
            json!([]),
            hit(1.0),
            Some(filter_of(&[("King Street", &[])])),
            StatusCode::OK,
        )
        .await;
        let unknown = fake(json!([]), hit(9.0), None, StatusCode::OK).await;
        let base = router(vec![
            ("AU", unknown.url.clone()),
            ("NZ", holder.url.clone()),
        ])
        .await;
        let (_, headers, body) = get_full(&format!("{base}/geocode?address=1+King+St")).await;
        assert_eq!(route_of(&headers), (Some("fanout"), Some("NZ")));
        assert_eq!(body["result"]["score"], 1.0);
    }

    #[tokio::test]
    async fn a_failing_chosen_runtime_falls_back_to_every_runtime() {
        let hit = json!({ "label": "found", "score": 1.0 });
        let c = countries([hit, Value::Null, Value::Null]).await;
        c.nz.fail_with.store(500, Ordering::SeqCst);

        // The filters say NZ, NZ answers 500, so the others are asked.
        let (status, headers, body) = get_full(&format!(
            "{}/geocode?address=15+Eruera+Street&locality=Rotorua",
            c.base
        ))
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(route_of(&headers), (Some("fanout"), Some("AU")));
        assert_eq!(body["result"]["label"], "found");

        // Every Runtime failing is a 502 problem.
        c.au.fail_with.store(500, Ordering::SeqCst);
        c.ca.fail_with.store(503, Ordering::SeqCst);
        let (status, body) = get(&format!("{}/geocode?address=1+King+St", c.base)).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body["error_code"], "upstream_unavailable");
    }

    #[tokio::test]
    async fn a_routed_runtime_with_nothing_to_say_falls_back_to_every_runtime() {
        let hit = json!({ "label": "found", "score": 1.0 });
        // The filters name NZ for Eruera Street in Rotorua, but NZ finds nothing (a filter false
        // positive, or a town spelled another way) and AU does.
        let c = countries([hit, Value::Null, Value::Null]).await;
        let (status, headers, body) = get_full(&format!(
            "{}/geocode?address=15+Eruera+Street&locality=Rotorua",
            c.base
        ))
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(route_of(&headers), (Some("fanout"), Some("AU")));
        assert_eq!(body["result"]["label"], "found");
        // NZ was asked twice: as the routed Runtime, then again with the others.
        assert_eq!((c.au.hits(), c.nz.hits(), c.ca.hits()), (1, 2, 1));

        // Lists too: NZ answers an empty list, AU has results.
        let nz = fake(
            json!([]),
            Value::Null,
            Some(filter_of(&[("Eruera Street", &["Rotorua"])])),
            StatusCode::OK,
        )
        .await;
        let au = fake(
            json!([{ "label": "au", "score": 1.0 }]),
            Value::Null,
            Some(filter_of(&[("Elm Street", &[])])),
            StatusCode::OK,
        )
        .await;
        let base = router(vec![("NZ", nz.url.clone()), ("AU", au.url.clone())]).await;
        let (_, headers, body) = get_full(&format!(
            "{base}/search?q=15+Eruera+Street&locality=Rotorua"
        ))
        .await;
        assert_eq!(route_of(&headers).0, Some("fanout"));
        assert_eq!(body["results"][0]["country"], "AU");
    }

    #[tokio::test]
    async fn a_client_error_is_passed_through_routed_or_not() {
        let c = countries([Value::Null, Value::Null, Value::Null]).await;

        // The bad query is the client's, so the chosen Runtime's 400 is not retried elsewhere.
        c.nz.fail_with.store(400, Ordering::SeqCst);
        let (status, headers, _) = get_full(&format!(
            "{}/geocode?address=15+Eruera+Street&locality=Rotorua",
            c.base
        ))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(route_of(&headers), (Some("filter-street-town"), Some("NZ")));
        assert_eq!((c.au.hits(), c.ca.hits()), (0, 0));

        // Fanned out, the first 400 is handed back.
        let (status, _, _) = get_full(&format!("{}/geocode?address=1+King+St", c.base)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn refreshing_picks_up_a_swapped_filter_and_a_304_keeps_the_held_one() {
        let nz = fake(
            json!([]),
            Value::Null,
            Some(filter_of(&[("Eruera Street", &["Rotorua"])])),
            StatusCode::OK,
        )
        .await;
        let au = fake(
            json!([]),
            Value::Null,
            Some(filter_of(&[("Elm Street", &[])])),
            StatusCode::OK,
        )
        .await;
        let (_, state) = router_state(vec![("NZ", nz.url.clone()), ("AU", au.url.clone())]).await;
        let nz_decision = Decision::StreetTown("NZ".to_string());
        assert_eq!(
            state.decide(&keys("Eruera Street", Some("Rotorua"), true)),
            nz_decision
        );
        assert_eq!(nz.sent.load(Ordering::SeqCst), 1);

        // Nothing changed: the Runtime answers 304 and the filter stays.
        state.refresh_filters().await;
        assert_eq!(nz.sent.load(Ordering::SeqCst), 1);
        assert_eq!(nz.not_modified.load(Ordering::SeqCst), 1);
        assert_eq!(
            state.decide(&keys("Eruera Street", Some("Rotorua"), true)),
            nz_decision
        );

        // The Pack is swapped: the street now lives in AU. Until a refresh the router still
        // believes the old filter.
        *nz.filter.write().unwrap() = Some(filter_of(&[("Elm Street", &[])]));
        *au.filter.write().unwrap() = Some(filter_of(&[("Eruera Street", &["Rotorua"])]));
        assert_eq!(
            state.decide(&keys("Eruera Street", Some("Rotorua"), true)),
            nz_decision
        );
        state.refresh_filters().await;
        assert_eq!(
            state.decide(&keys("Eruera Street", Some("Rotorua"), true)),
            Decision::StreetTown("AU".to_string())
        );

        // A Runtime that stops answering keeps the filter it had; one whose Pack lost its
        // filter becomes "might hold anything", so it no longer lets NZ win alone.
        au.filter_broken.store(true, Ordering::SeqCst);
        state.refresh_filters().await;
        assert_eq!(
            state.decide(&keys("Eruera Street", Some("Rotorua"), true)),
            Decision::StreetTown("AU".to_string())
        );
        au.filter_broken.store(false, Ordering::SeqCst);
        *au.filter.write().unwrap() = None;
        state.refresh_filters().await;
        assert_eq!(
            state.decide(&keys("Elm Street", None, true)),
            Decision::Fanout
        );
    }

    #[tokio::test]
    async fn a_runtime_coming_back_ready_has_its_filter_fetched_again() {
        let nz = fake(
            json!([]),
            Value::Null,
            Some(filter_of(&[("Elm Street", &[])])),
            StatusCode::OK,
        )
        .await;
        let au = fake(
            json!([]),
            Value::Null,
            Some(filter_of(&[("Elm Street", &[])])),
            StatusCode::OK,
        )
        .await;
        let (base, state) =
            router_state(vec![("NZ", nz.url.clone()), ("AU", au.url.clone())]).await;
        assert_eq!(
            state.decide(&keys("Eruera Street", None, true)),
            Decision::Fanout
        );

        // The router sees both ready, then NZ goes down for a Pack swap.
        let (status, _) = get(&format!("{base}/readyz")).await;
        assert_eq!(status, StatusCode::OK);
        nz.ready.store(503, Ordering::SeqCst);
        let (status, _) = get(&format!("{base}/readyz")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

        // It comes back with a new Pack: the next /readyz sees it flip and refreshes it, with no
        // timer involved.
        *nz.filter.write().unwrap() = Some(filter_of(&[("Eruera Street", &[])]));
        nz.ready.store(200, Ordering::SeqCst);
        let (status, _) = get(&format!("{base}/readyz")).await;
        assert_eq!(status, StatusCode::OK);
        let mut decision = Decision::Fanout;
        for _ in 0..100 {
            decision = state.decide(&keys("Eruera Street", None, true));
            if decision != Decision::Fanout {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(decision, Decision::Street("NZ".to_string()));
    }

    #[tokio::test]
    async fn refreshes_that_overlap_cannot_leave_the_older_filter_behind() {
        let nz = fake(
            json!([]),
            Value::Null,
            Some(filter_of(&[("Elm Street", &[])])),
            StatusCode::OK,
        )
        .await;
        let au = fake(
            json!([]),
            Value::Null,
            Some(filter_of(&[("Elm Street", &[])])),
            StatusCode::OK,
        )
        .await;
        let (_, state) = router_state(vec![("NZ", nz.url.clone()), ("AU", au.url.clone())]).await;
        let at_nz = keys("Eruera Street", None, true);
        assert_eq!(state.decide(&at_nz), Decision::Fanout);

        // The first refresh reads the filter as it is now (one without the street) and is slow to
        // deliver it. Meanwhile the Pack is swapped and a second refresh asks, and would be done
        // first.
        let old = filter_of(&[("Elm Street", &[]), ("Oak Street", &[])]);
        *nz.filter.write().unwrap() = Some(old);
        nz.filter_delay.store(300, Ordering::SeqCst);
        let slow = {
            let state = state.clone();
            tokio::spawn(async move { state.refresh_worker("NZ").await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        *nz.filter.write().unwrap() = Some(filter_of(&[("Eruera Street", &[])]));
        nz.filter_delay.store(0, Ordering::SeqCst);
        state.refresh_worker("NZ").await;
        slow.await.expect("refresh");

        // The newer filter wins however the two finished.
        assert_eq!(state.decide(&at_nz), Decision::Street("NZ".to_string()));
    }
}
