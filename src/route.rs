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
//! - `prefer=XX` (a hint taken from an invoice currency, which is not a fact) changes the order
//!   when `country` is absent: a unique street|town holder still wins, even over `prefer`, since
//!   the currency can be wrong; then `prefer` wins if it may hold the street; then a unique
//!   street holder; then `prefer` anyway (`x-route`: `prefer`, `prefer-fallback`). A `prefer`
//!   that is not a configured Runtime is ignored. See [`RouteState::decide`].
//! - When the filters cannot decide (zero or several Runtimes hold the street, or a request with
//!   no street) the request goes to every Runtime at once, as before (`x-route: fanout`).
//!   `/search` and `/autocomplete` results are merged and cut to `limit`, each carrying the
//!   `country` it came from. BM25 scores from different Packs are not comparable, so the merge
//!   ranks first by how much of the query a label covers, then by rank within its own Runtime's
//!   list (see [`merge`]). `/geocode` answers one best match or `null`. A single non-null
//!   answer wins. Several do not pick a winner by score, which is not comparable across Packs,
//!   and a request with only an address gives the Runtimes no locality, region or postcode to
//!   check, so each answers for any street it has: unless exactly one of the answering Runtimes
//!   has a filter that holds the street, the router answers `country_required`, as it did
//!   before it could fan out `/geocode`.
//! - `POST /geocode/batch` is `/geocode` for many addresses in one request. Each item is decided as
//!   a `/geocode` request is (its own `country` and `prefer` included), the items for one Runtime
//!   go to it as one sub-batch, and the answers come back in the order asked, each with its
//!   `route` in place of the `x-route` header. See [`geocode_batch`].
//! - `/reverse` needs `country`: a coordinate carries no street to route by.
//! - `/readyz` is ready only when every Runtime is.
//!
//! The router holds no Pack and does no geocoding. It keeps the Runtime's boundary policy: GET
//! only (POST only for the batch), Problem Details for errors, request-id propagation. Its request id is sent to every
//! Runtime it calls as `x-request-id`, so router and Runtime logs share one id per request.

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
    extract::{
        DefaultBodyLimit, Query, RawQuery, State,
        rejection::{BytesRejection, QueryRejection},
    },
    handler::{Handler, HandlerWithoutStateExt},
    http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::{MethodFilter, MethodRouter, on},
};
use futures_util::future::join_all;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use crate::{
    http::{
        bounds, method,
        problem::Problem,
        request_id::{self, RequestId},
        shutdown::shutdown_signal,
    },
    search::DEFAULT_SEARCH_LIMIT,
    street_filter::{RoutingFilter, pair_hash, query_street_hashes, town_hash},
    text_index::normalize_index_text,
};

/// How long the router waits for one Runtime before answering 502 (or leaving it out of a merge).
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(10);
/// A routing filter is megabytes to hundreds of them, so it gets longer than an answer does.
const FILTER_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the router waits for a Runtime to answer a sub-batch: up to a thousand searches, on a
/// Runtime that may be busy with others, so longer than one answer gets.
const BATCH_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// `prefer` may hold the street, and no filter showed the town is elsewhere.
    Prefer(String),
    /// Nothing else decided, so `prefer` is asked rather than every Runtime.
    PreferFallback(String),
    Fanout,
}

impl Decision {
    /// The one Runtime this decision names, and the `x-route` that says why.
    fn routed(&self) -> Option<(&str, &'static str)> {
        match self {
            Self::StreetTown(country) => Some((country, "filter-street-town")),
            Self::Street(country) => Some((country, "filter-street")),
            Self::Prefer(country) => Some((country, "prefer")),
            Self::PreferFallback(country) => Some((country, "prefer-fallback")),
            Self::Fanout => None,
        }
    }
}

#[derive(Clone)]
struct RouteState {
    /// Country code (upper case) -> Runtime.
    workers: Arc<BTreeMap<String, Worker>>,
    client: Client<HttpConnector, Full<Bytes>>,
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

    /// Where a batch item goes, and the keys it routes by (also what [`pick_geocode`] needs when
    /// it fans out). `country` wins, then the filters and `prefer` through [`Self::decide`]: the
    /// same decision `GET /geocode` makes, so the two cannot drift.
    fn plan_item(&self, item: &Value) -> (Plan, RouteKeys) {
        let text = |name: &str| item.get(name).and_then(Value::as_str).unwrap_or_default();
        let keys = RouteKeys::of(
            text("address"),
            text("locality"),
            text("prefer"),
            Source::Address,
        );
        let country = text("country").to_ascii_uppercase();
        let plan = if !country.is_empty() {
            if self.workers.contains_key(&country) {
                Plan::One {
                    country,
                    route: "country",
                    named: true,
                }
            } else {
                Plan::Unknown(country)
            }
        } else {
            match self.decide(&keys).routed() {
                Some((country, route)) => Plan::One {
                    country: country.to_string(),
                    route,
                    named: false,
                },
                None => Plan::Fanout,
            }
        };
        // An item no Runtime can answer is answered here, as the Runtime would, rather than sent
        // to each one to be refused. Its route is where it would have gone.
        let invalid = match (item, item.get("address")) {
            (Value::Object(_), Some(Value::String(address))) if !address.trim().is_empty() => None,
            (Value::Object(_), None | Some(Value::Null | Value::String(_))) => {
                Some(bounds::empty_address())
            }
            _ => Some(bounds::malformed_batch_item()),
        };
        let plan = match (invalid, plan) {
            (Some(problem), Plan::One { route, .. }) => Plan::Invalid { problem, route },
            (Some(problem), Plan::Fanout) => Plan::Invalid {
                problem,
                route: "fanout",
            },
            (_, plan) => plan,
        };
        (plan, keys)
    }

    /// Send each Runtime of `asks` its items (indexes into `items`) as one sub-batch, all at
    /// once, and file what it said about each under `replies[index]`. A sub-batch is never
    /// larger than a batch may be, since `asks` holds each item at most once per Runtime and the
    /// router accepts no more items than a Runtime does.
    async fn ask_batch(
        &self,
        asks: &BTreeMap<String, Vec<usize>>,
        items: &[Value],
        replies: &mut [Vec<(String, Reply)>],
        id: Option<&str>,
    ) {
        #[derive(Serialize)]
        struct Body<'a> {
            items: Vec<&'a Value>,
        }
        let calls = asks.iter().map(|(country, indexes)| async move {
            let body = Body {
                items: indexes.iter().map(|&index| &items[index]).collect(),
            };
            let body = serde_json::to_vec(&body).expect("JSON values serialize");
            (
                country,
                indexes,
                self.post_batch(country, body, indexes.len(), id).await,
            )
        });
        for (country, indexes, answer) in join_all(calls).await {
            match answer {
                Ok(results) => {
                    for (&index, result) in indexes.iter().zip(results) {
                        replies[index].push((country.clone(), Reply::of(result)));
                    }
                }
                Err(shed) => {
                    for &index in indexes {
                        replies[index].push((country.clone(), Reply::Failed { shed }));
                    }
                }
            }
        }
    }

    /// One Runtime's `count` per-item results, or `Err(shed)` when it gave none: it did not
    /// answer, refused the batch, or answered something that is not a result per item. `shed` is
    /// whether it asked to be retried later.
    async fn post_batch(
        &self,
        country: &str,
        body: Vec<u8>,
        count: usize,
        id: Option<&str>,
    ) -> Result<Vec<Value>, bool> {
        #[derive(Deserialize)]
        struct Answer {
            results: Vec<Value>,
        }
        let url = format!("{}/geocode/batch", self.workers[country].base);
        let answer = self
            .post(&url, body, BATCH_TIMEOUT, id)
            .await
            .ok_or(false)?;
        if !answer.status.is_success() {
            tracing::warn!(
                request_id = id.unwrap_or("-"),
                country,
                status = %answer.status,
                "Runtime refused a geocode batch"
            );
            return Err(is_shed(&answer));
        }
        match serde_json::from_slice::<Answer>(&answer.body) {
            Ok(Answer { results }) if results.len() == count => Ok(results),
            _ => {
                tracing::warn!(
                    request_id = id.unwrap_or("-"),
                    country,
                    "Runtime answered a geocode batch with something other than a result per item"
                );
                Err(false)
            }
        }
    }

    fn known(&self) -> String {
        self.workers.keys().cloned().collect::<Vec<_>>().join(", ")
    }

    /// GET `url` under `request_id`, or None when the Runtime did not answer in time. The answer
    /// keeps all its headers; [`pass_through`] chooses the ones worth handing on.
    async fn fetch(
        &self,
        url: &str,
        if_none_match: Option<&str>,
        timeout: Duration,
        request_id: Option<&str>,
    ) -> Option<Upstream> {
        let answer = self
            .try_fetch(url, if_none_match, timeout, request_id)
            .await;
        log_unanswered(&answer, url, request_id);
        answer.ok()
    }

    async fn try_fetch(
        &self,
        url: &str,
        if_none_match: Option<&str>,
        timeout: Duration,
        request_id: Option<&str>,
    ) -> Result<Upstream> {
        let mut request = Request::get(url);
        if let Some(etag) = if_none_match {
            request = request.header(header::IF_NONE_MATCH, etag);
        }
        if let Some(id) = request_id {
            request = request.header("x-request-id", id);
        }
        self.send(request.body(Full::default())?, timeout).await
    }

    /// POST `body` (JSON) to `url` under `request_id`, or None when the Runtime did not answer in
    /// time.
    async fn post(
        &self,
        url: &str,
        body: Vec<u8>,
        timeout: Duration,
        request_id: Option<&str>,
    ) -> Option<Upstream> {
        let mut request = Request::post(url).header(header::CONTENT_TYPE, "application/json");
        if let Some(id) = request_id {
            request = request.header("x-request-id", id);
        }
        let answer = match request.body(Full::new(Bytes::from(body))) {
            Ok(request) => self.send(request, timeout).await,
            Err(error) => Err(error.into()),
        };
        log_unanswered(&answer, url, request_id);
        answer.ok()
    }

    async fn send(&self, request: Request<Full<Bytes>>, timeout: Duration) -> Result<Upstream> {
        let response = tokio::time::timeout(timeout, self.client.request(request))
            .await
            .context("timed out")??;
        let (parts, body) = response.into_parts();
        let body = tokio::time::timeout(timeout, body.collect())
            .await
            .context("timed out reading the body")??
            .to_bytes();
        Ok(Upstream {
            status: parts.status,
            headers: parts.headers,
            body,
        })
    }

    /// GET `url` under `request_id` with the answer timeout.
    async fn get(&self, url: &str, request_id: Option<&str>) -> Option<Upstream> {
        self.fetch(url, None, UPSTREAM_TIMEOUT, request_id).await
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
        // Not fetch(): a Runtime that is down is retried every FILTER_RETRY, which must not warn
        // each time. Searches that reach it say so.
        let answer = match self
            .try_fetch(&url, etag.as_deref(), FILTER_TIMEOUT, None)
            .await
        {
            Ok(answer) if !answer.status.is_server_error() => answer,
            Ok(_) => return,
            Err(error) => {
                tracing::debug!("{country}: routing filter not fetched: {error:#}");
                return;
            }
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
                        tracing::warn!("{country}: ignoring its routing filter: {error:#}");
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
    ///
    /// With a `prefer` that is a configured Runtime the order is: a sole street|town holder, even
    /// when it is not `prefer`, because a currency can be wrong and a town named in the address
    /// cannot; `prefer` when its filter holds the street (or it has none); a sole street holder;
    /// else `prefer` all the same, since it was asked for and nothing contradicts it. Without
    /// `prefer` nothing changes.
    fn decide(&self, keys: &RouteKeys) -> Decision {
        let prefer = keys
            .prefer
            .as_deref()
            .filter(|country| self.workers.contains_key(*country));
        if let Some(prefer) = prefer {
            return self.decide_preferring(keys, prefer);
        }
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

    /// [`Self::decide`] for a `prefer` that names a configured Runtime.
    fn decide_preferring(&self, keys: &RouteKeys, prefer: &str) -> Decision {
        if let Some(town) = keys.town
            && let Some(country) = keys
                .streets
                .iter()
                .find_map(|&street| self.sole_holder(pair_hash(street, town)))
        {
            return Decision::StreetTown(country);
        }
        let preferred = &self.workers[prefer];
        if keys
            .streets
            .iter()
            .any(|&street| preferred.may_hold(street))
        {
            return Decision::Prefer(prefer.to_string());
        }
        if keys.street_alone
            && let Some(country) = keys
                .streets
                .iter()
                .find_map(|&street| self.sole_holder(street))
        {
            return Decision::Street(country);
        }
        Decision::PreferFallback(prefer.to_string())
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

fn log_unanswered(answer: &Result<Upstream>, url: &str, request_id: Option<&str>) {
    if let Err(error) = answer {
        // The query string is the searched address; keep it out of the logs.
        let url = url.split_once('?').map_or(url, |(path, _)| path);
        tracing::warn!(
            request_id = request_id.unwrap_or("-"),
            url,
            "Runtime did not answer: {error:#}"
        );
    }
}

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
    /// `prefer`, upper case: a country the caller guesses (from a currency) but does not insist
    /// on, unlike `country`.
    prefer: Option<String>,
}

impl RouteKeys {
    fn new(params: &Params, text: &str, source: Source) -> Self {
        let Ok(Query(params)) = params else {
            return Self::default();
        };
        let get = |name: &str| params.get(name).map(String::as_str).unwrap_or_default();
        Self::of(get(text), get("locality"), get("prefer"), source)
    }

    /// The keys of a request from its three texts, which `GET` takes from the query string and a
    /// batch item from its fields, so both route by the same rules.
    fn of(text: &str, town: &str, prefer: &str, source: Source) -> Self {
        let town = town_hash(town);
        let prefer = Some(prefer.trim().to_ascii_uppercase()).filter(|prefer| !prefer.is_empty());
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
            prefer,
        }
    }
}

/// How an endpoint answers: a list of results, or one best match.
#[derive(Clone, Copy)]
enum Shape {
    /// `/search`, `/autocomplete`: the array under this key. `prefix_last` is passed to
    /// [`merge`]: autocomplete's last query token may be a partial word.
    List {
        key: &'static str,
        prefix_last: bool,
    },
    /// `/geocode`: `result`, an object or null.
    Best,
}

impl Shape {
    fn key(self) -> &'static str {
        match self {
            Self::List { key, .. } => key,
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
        .flat_map(|(country, mut body)| {
            tag_country(&mut body, key, &country);
            let items = match body.get_mut(key).map(Value::take) {
                Some(Value::Array(items)) => items,
                _ => Vec::new(),
            };
            let query_tokens = &query_tokens;
            let last = last.as_deref();
            items.into_iter().enumerate().map(move |(rank, item)| {
                let label = item
                    .get("label")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let coverage = coverage(query_tokens, last, label, prefix_last);
                let score = item
                    .get("score")
                    .and_then(Value::as_f64)
                    .unwrap_or(f64::MIN);
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

/// A Runtime's answer handed back as it came. Of its headers only the content type and
/// `Retry-After` go on, which keeps a Runtime's load-shedding 503 actionable through the router.
fn pass_through(upstream: Upstream) -> Response {
    let mut response = (upstream.status, Body::from(upstream.body)).into_response();
    for name in [header::CONTENT_TYPE, header::RETRY_AFTER] {
        if let Some(value) = upstream.headers.get(&name) {
            response.headers_mut().insert(name, value.clone());
        }
    }
    response
}

/// A Runtime that asks to be retried later is shedding load; asking it again or its neighbours
/// would only add to it.
fn is_shed(upstream: &Upstream) -> bool {
    upstream.headers.contains_key(header::RETRY_AFTER)
}

/// The id of the router's own request, to send on to the Runtimes it calls.
fn id_of(request_id: &Option<Extension<RequestId>>) -> Option<&str> {
    request_id
        .as_ref()
        .map(|Extension(RequestId(id))| id.as_str())
}

/// Say how the request was routed: `x-route`, and `x-route-country` when one country answered.
fn tagged(mut response: Response, route: &'static str, country: Option<&str>) -> Response {
    tracing::debug!(route, country = country.unwrap_or("-"), "routed");
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
    let url = format!("{}{path}{suffix}", worker.base);
    match state.get(&url, id_of(request_id)).await {
        Some(answer) => tagged(pass_through(answer), "country", Some(country)),
        None => with_id(Problem::upstream_unavailable(country), request_id),
    }
}

/// Ask every Runtime at once. A Runtime's 4xx (a bad query) is every Runtime's, so the first one
/// is handed back as the `Err`; one that failed or did not answer is left out. When nothing
/// answered and one Runtime asked to be retried later, its answer is the `Err`, `Retry-After`
/// kept.
async fn fan_out(
    state: &RouteState,
    path: &str,
    suffix: &str,
    id: Option<&str>,
) -> Result<Vec<(String, Value)>, Box<Response>> {
    let calls = state.workers.iter().map(|(country, worker)| {
        let url = format!("{}{path}{suffix}", worker.base);
        async move { (country.clone(), state.get(&url, id).await) }
    });
    let mut answers = Vec::new();
    let mut shed = None;
    for (country, answer) in join_all(calls).await {
        match answer {
            Some(answer) if answer.status.is_client_error() => {
                return Err(Box::new(pass_through(answer)));
            }
            Some(answer) if answer.status.is_success() => {
                if let Ok(value) = serde_json::from_slice::<Value>(&answer.body) {
                    answers.push((country, value));
                }
            }
            Some(answer) if is_shed(&answer) && shed.is_none() => shed = Some(answer),
            _ => {}
        }
    }
    match shed {
        Some(answer) if answers.is_empty() => Err(Box::new(pass_through(answer))),
        _ => Ok(answers),
    }
}

/// Answer `path?raw` from the one Runtime the request names (`country=XX`) or the filters and
/// `prefer` pick, else from all of them.
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
    if let Some((country, route)) = decision.routed() {
        let url = format!("{}{path}{suffix}", state.workers[country].base);
        match state.get(&url, id_of(&request_id)).await {
            Some(answer) if answer.status.is_client_error() => {
                return tagged(pass_through(answer), route, Some(country));
            }
            Some(answer) if answer.status.is_success() => {
                match serde_json::from_slice::<Value>(&answer.body) {
                    // A filter can be wrong (a false positive, a town spelled another way), and
                    // an empty answer from the wrong country must not end the search.
                    Ok(value) if !has_answer(&value, shape) => {}
                    Ok(mut value) => {
                        tag_country(&mut value, shape.key(), country);
                        return tagged(axum::Json(value).into_response(), route, Some(country));
                    }
                    Err(_) => return tagged(pass_through(answer), route, Some(country)),
                }
            }
            Some(answer) if is_shed(&answer) => {
                return tagged(pass_through(answer), route, Some(country));
            }
            // The chosen Runtime failed, did not answer or found nothing: the others may still
            // know the street.
            _ => {}
        }
        tracing::debug!(route, country = %country, "routed Runtime had no answer, asking every Runtime");
    }

    let answers = match fan_out(state, path, &suffix, id_of(&request_id)).await {
        Ok(answers) if !answers.is_empty() => answers,
        Ok(_) => return with_id(Problem::upstream_unavailable("any"), &request_id),
        Err(response) => return *response,
    };
    match shape {
        Shape::List { key, prefix_last } => tagged(
            axum::Json(merge(answers, key, limit, prefix_last)).into_response(),
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

/// Where the first request for a batch item goes.
enum Plan {
    /// A `country` the router has no Runtime for: nobody is asked.
    Unknown(String),
    /// One Runtime: the one the item named, or the one the filters and `prefer` chose.
    One {
        country: String,
        route: &'static str,
        named: bool,
    },
    /// Every Runtime.
    Fanout,
    /// An item that is not a lookup: nobody is asked, and `route` is where it would have gone.
    Invalid {
        problem: Problem,
        route: &'static str,
    },
}

/// What one Runtime said about one batch item.
enum Reply {
    /// `{ query, result }`.
    Answer(Value),
    /// The item's Problem object.
    Problem(Value),
    /// The Runtime gave no result for the item. `shed` is whether it asked to be retried later.
    Failed { shed: bool },
}

impl Reply {
    fn of(mut result: Value) -> Self {
        // An entry that is not an object is no answer, whatever else the Runtime said.
        if !result.is_object() {
            return Self::Failed { shed: false };
        }
        match result.get_mut("problem").map(Value::take) {
            Some(problem) => Self::Problem(problem),
            None => Self::Answer(result),
        }
    }
}

/// What a batch item's first Runtime settled, or not.
enum Step {
    Done(Value),
    /// Ask the others. Holds the answer the first one gave, a null result, when it gave one.
    Elsewhere(Option<Value>),
}

/// A Problem as the batch item that answers with it.
fn problem_item(problem: Problem, id: Option<&str>) -> Value {
    let problem = match id {
        Some(id) => problem.with_request_id(id),
        None => problem,
    };
    json!({ "problem": problem })
}

/// The item with how it was routed, the per-item `x-route`.
fn with_route(mut item: Value, route: &str) -> Value {
    if let Some(item) = item.as_object_mut() {
        item.insert("route".to_string(), route.into());
    }
    item
}

/// What `country`, asked alone for a batch item, makes of it, as `GET /geocode` does: its answer
/// stands, tagged with the country, unless the item did not name it and the answer is null (the
/// filters can be wrong, so the others are asked); the item's own Problem or a shedding Runtime
/// stands; a failure is retried elsewhere unless the item named the country.
fn settle(country: &str, named: bool, reply: Reply, id: Option<&str>) -> Step {
    match reply {
        Reply::Answer(body) if !named && !has_answer(&body, Shape::Best) => {
            Step::Elsewhere(Some(body))
        }
        Reply::Answer(mut body) => {
            tag_country(&mut body, "result", country);
            Step::Done(body)
        }
        Reply::Problem(problem) if named || problem["status"] != 500 => {
            Step::Done(json!({ "problem": problem }))
        }
        Reply::Failed { shed: true } => Step::Done(problem_item(Problem::overloaded(), id)),
        Reply::Failed { .. } if named => {
            Step::Done(problem_item(Problem::upstream_unavailable(country), id))
        }
        Reply::Problem(_) | Reply::Failed { .. } => Step::Elsewhere(None),
    }
}

/// The one answer for a batch item asked of several Runtimes, as the fan-out of `GET /geocode`
/// gives it: a Problem that is the item's own is every Runtime's, so the first one is the answer;
/// Runtimes that failed are left out; [`pick_geocode`] chooses among the rest.
fn pick_item(
    state: &RouteState,
    replies: Vec<(String, Reply)>,
    streets: &[u64],
    id: Option<&str>,
) -> Value {
    let mut answers = Vec::new();
    let mut shed = false;
    for (country, reply) in replies {
        match reply {
            Reply::Answer(body) => answers.push((country, body)),
            Reply::Problem(problem) => match problem["status"].as_u64() {
                Some(400..500) => return json!({ "problem": problem }),
                status => shed |= status == Some(503),
            },
            Reply::Failed { shed: failed_shed } => shed |= failed_shed,
        }
    }
    if answers.is_empty() {
        let problem = if shed {
            Problem::overloaded()
        } else {
            Problem::upstream_unavailable("any")
        };
        return problem_item(problem, id);
    }
    match pick_geocode(state, answers, streets) {
        Picked::One(body, _) | Picked::Null(body) => body,
        Picked::Ambiguous(countries) => {
            problem_item(Problem::country_ambiguous("/geocode", &countries), id)
        }
    }
}

/// `POST /geocode/batch`: `GET /geocode` for many addresses in one request, answered in the order
/// asked as `{ "results": [...] }`. An item is the Runtime's (`address`, `locality`, `region`,
/// `postcode`, `limit`, `layer`, `street_fallback`) plus an optional `country` and `prefer`, and
/// each is routed as `GET /geocode` would route it. Items for the same Runtime travel together,
/// one sub-batch per Runtime, all at once; an item that goes to every Runtime is in each one's.
///
/// An item answers `{ query, result, route }` or `{ problem, route }`; `route` is the `x-route` of
/// the GET, and a result carries its `country` as routed `/geocode` results do (also when the
/// item named the country, which GET leaves untagged). An item that is not an object, or has no non-empty string `address`, is answered here with the
/// Problem a Runtime would give, and asked of nobody. A Runtime that does not answer a sub-batch
/// fails its items only, as `upstream_unavailable` (`overloaded` when it shed load). As on GET,
/// an item whose routed Runtime has no answer, or fails, is then asked of the others.
async fn geocode_batch(
    State(state): State<RouteState>,
    request_id: Option<Extension<RequestId>>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let id = id_of(&request_id);
    let items = match bounds::validate_batch(body) {
        Ok(items) => items,
        Err(problem) => return with_id(problem, &request_id),
    };
    let count = items.len();
    let (plans, keys): (Vec<Plan>, Vec<RouteKeys>) =
        items.iter().map(|item| state.plan_item(item)).unzip();
    let none = || -> Vec<Vec<(String, Reply)>> { (0..count).map(|_| Vec::new()).collect() };

    let mut results: Vec<Option<Value>> = vec![None; count];
    let mut asks: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, plan) in plans.iter().enumerate() {
        match plan {
            Plan::Unknown(country) => {
                let problem = Problem::unknown_country(country, &state.known());
                results[index] = Some(problem_item(problem, id));
            }
            Plan::Invalid { problem, route } => {
                results[index] = Some(with_route(problem_item(problem.clone(), id), route));
            }
            Plan::One { country, .. } => asks.entry(country.clone()).or_default().push(index),
            Plan::Fanout => {
                for country in state.workers.keys() {
                    asks.entry(country.clone()).or_default().push(index);
                }
            }
        }
    }
    let mut first = none();
    state.ask_batch(&asks, &items, &mut first, id).await;

    // What each item still needs the others for, with what they have said so far.
    let mut others = none();
    let mut again: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut fanned = Vec::new();
    let mut retried = 0;
    for (index, plan) in plans.iter().enumerate() {
        match plan {
            Plan::Unknown(_) | Plan::Invalid { .. } => {}
            Plan::One {
                country,
                route,
                named,
            } => {
                let (_, reply) = first[index]
                    .pop()
                    .expect("the item was asked of its Runtime");
                match settle(country, *named, reply, id) {
                    Step::Done(item) => results[index] = Some(with_route(item, route)),
                    Step::Elsewhere(null) => {
                        // The Runtime already said what it had: it is not asked again.
                        for other in state.workers.keys().filter(|other| *other != country) {
                            again.entry(other.clone()).or_default().push(index);
                        }
                        others[index]
                            .extend(null.map(|body| (country.clone(), Reply::Answer(body))));
                        fanned.push(index);
                        retried += 1;
                    }
                }
            }
            Plan::Fanout => {
                others[index] = std::mem::take(&mut first[index]);
                fanned.push(index);
            }
        }
    }
    state.ask_batch(&again, &items, &mut others, id).await;
    for index in fanned {
        let replies = std::mem::take(&mut others[index]);
        let item = pick_item(&state, replies, &keys[index].streets, id);
        results[index] = Some(with_route(item, "fanout"));
    }

    let results: Vec<Value> = results
        .into_iter()
        .map(|item| item.expect("every item was settled"))
        .collect();
    tracing::info!(
        request_id = id.unwrap_or("-"),
        items = count,
        fanned_out = fanned_out(&plans),
        retried,
        problems = results
            .iter()
            .filter(|item| item.get("problem").is_some())
            .count(),
        "geocode batch"
    );
    axum::Json(json!({ "results": results })).into_response()
}

/// How many items of a batch were asked of every Runtime from the start.
fn fanned_out(plans: &[Plan]) -> usize {
    plans
        .iter()
        .filter(|plan| matches!(plan, Plan::Fanout))
        .count()
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
        Shape::List {
            key: "results",
            prefix_last: false,
        },
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
        Shape::List {
            key: "suggestions",
            prefix_last: true,
        },
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
async fn readyz(
    State(state): State<RouteState>,
    request_id: Option<Extension<RequestId>>,
) -> StatusCode {
    let id = id_of(&request_id);
    let calls = state.workers.iter().map(|(country, worker)| {
        let url = format!("{}/readyz", worker.base);
        let state = &state;
        async move {
            let ready =
                state.get(&url, id).await.map(|answer| answer.status) == Some(StatusCode::OK);
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

/// A POST-only route. Every other method, HEAD included, is a 405 that says `Allow: POST`.
fn post_only<H, T>(handler: H) -> MethodRouter<RouteState>
where
    H: Handler<T, RouteState>,
    T: 'static,
{
    on(MethodFilter::POST, handler).fallback(method::post_only_method_not_allowed)
}

fn build_router(state: RouteState) -> Router {
    Router::new()
        .route("/search", get_only(search))
        .route("/autocomplete", get_only(autocomplete))
        .route("/reverse", get_only(reverse))
        .route("/geocode", get_only(geocode))
        .route(
            "/geocode/batch",
            post_only(geocode_batch).layer(DefaultBodyLimit::max(bounds::MAX_BATCH_BODY_BYTES)),
        )
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
    // The listener comes up first: a Runtime that is slow or down now only means "might hold
    // anything" until its filter arrives.
    let refresh = options.filter_refresh;
    let background = state.clone();
    tokio::spawn(async move {
        background.refresh_filters().await;
        tracing::info!("{}", background.filter_summary());
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
    use std::sync::{
        Mutex,
        atomic::{AtomicU16, AtomicU64, AtomicUsize},
    };

    use http_body_util::Empty;

    use xxhash_rust::xxh3::xxh3_64;

    use super::*;
    use crate::street_filter::{FilterEncoder, street_hash};

    /// A batch a stand-in Runtime received: its `x-request-id` and the addresses in it.
    type BatchSeen = (Option<String>, Vec<String>);

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
        /// Send `Retry-After` with a `fail_with` status, like a Runtime shedding load.
        retry_after: Arc<AtomicBool>,
        /// Make /routing-filter answer 500.
        filter_broken: Arc<AtomicBool>,
        /// Milliseconds /routing-filter waits after it has read its bytes, before it answers.
        filter_delay: Arc<AtomicU64>,
        /// Every `POST /geocode/batch` received: its `x-request-id` and the addresses in it.
        batches: Arc<Mutex<Vec<BatchSeen>>>,
    }

    impl Fake {
        /// The addresses of each batch received, in the order received.
        fn batches(&self) -> Vec<Vec<String>> {
            let batches = self.batches.lock().unwrap();
            batches
                .iter()
                .map(|(_, addresses)| addresses.clone())
                .collect()
        }

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
            retry_after: Arc::default(),
            filter_broken: Arc::default(),
            filter_delay: Arc::default(),
            batches: Arc::default(),
        };
        let answer = |fake: &Fake, body: Value| {
            let (hits, fail_with) = (fake.hits.clone(), fake.fail_with.clone());
            let retry_after = fake.retry_after.clone();
            move || {
                let (hits, fail_with, body) = (hits.clone(), fail_with.clone(), body.clone());
                let retry_after = retry_after.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    match fail_with.load(Ordering::SeqCst) {
                        0 => axum::Json(body).into_response(),
                        status => {
                            let mut response = StatusCode::from_u16(status)
                                .expect("status")
                                .into_response();
                            if retry_after.load(Ordering::SeqCst) {
                                response
                                    .headers_mut()
                                    .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
                            }
                            response
                        }
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
        // Answers each item of a batch like /geocode would, but for the addresses `bad` (a 400
        // Problem), `shed` (a 503 Problem), `nothing` (a null result) and `garbage` (an entry that
        // is not an object). Fails like /geocode does.
        let batch = {
            let fake = fake.clone();
            let geocode = geocode.clone();
            move |headers: HeaderMap, axum::Json(body): axum::Json<Value>| {
                let fake = fake.clone();
                let geocode = geocode.clone();
                async move {
                    let addresses: Vec<String> = body["items"]
                        .as_array()
                        .expect("items")
                        .iter()
                        .map(|item| item["address"].as_str().unwrap_or_default().to_string())
                        .collect();
                    let id = headers
                        .get("x-request-id")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_string);
                    fake.batches.lock().unwrap().push((id, addresses.clone()));
                    let status = fake.fail_with.load(Ordering::SeqCst);
                    if status != 0 {
                        let mut response = StatusCode::from_u16(status)
                            .expect("status")
                            .into_response();
                        if fake.retry_after.load(Ordering::SeqCst) {
                            response
                                .headers_mut()
                                .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
                        }
                        return response;
                    }
                    let results: Vec<Value> = addresses
                        .iter()
                        .map(|address| match address.as_str() {
                            "bad" => json!({ "problem": { "status": 400, "error_code": "invalid_query" } }),
                            "shed" => json!({ "problem": { "status": 503, "error_code": "overloaded" } }),
                            "nothing" => json!({ "query": address, "result": null }),
                            "garbage" => json!("garbage"),
                            _ => json!({ "query": address, "result": geocode }),
                        })
                        .collect();
                    axum::Json(json!({ "results": results })).into_response()
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
                .route("/geocode/batch", axum::routing::post(batch))
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
            prefer: None,
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
    async fn country_goes_to_its_runtime_and_none_merges() {
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

        // The same street in three countries is merged, every item tagged.
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

    /// `x-route` and `x-route-country` of a request to `base`, whatever its status: a fan-out of
    /// several found addresses is a 400.
    async fn routed(base: &str, query: &str) -> (String, Option<String>) {
        let (_, headers, _) = get_full(&format!("{base}/{query}")).await;
        let (route, country) = route_of(&headers);
        (
            route.expect("x-route").to_string(),
            country.map(str::to_string),
        )
    }

    /// Three countries that each find any address.
    async fn finding_countries() -> Countries {
        let hit = |label: &str| json!({ "label": label, "score": 1.0 });
        countries([hit("au"), hit("nz"), hit("ca")]).await
    }

    fn is(route: &str, country: Option<&str>) -> (String, Option<String>) {
        (route.to_string(), country.map(str::to_string))
    }

    #[tokio::test]
    async fn a_street_and_town_held_by_one_country_beats_prefer() {
        let c = finding_countries().await;

        // An NZ supplier billing in AUD: prefer=AU is wrong, the town says Rotorua.
        for path in [
            "geocode?address=15+Eruera+Street&locality=Rotorua",
            "search?q=15+Eruera+Street&locality=Rotorua",
            "autocomplete?q=Eruera+Street&locality=Rotorua",
        ] {
            assert_eq!(
                routed(&c.base, &format!("{path}&prefer=AU")).await,
                is("filter-street-town", Some("NZ")),
                "{path}"
            );
        }
        // Held by all three, but only AU has it in Sydney.
        assert_eq!(
            routed(
                &c.base,
                "geocode?address=1+King+St&locality=Sydney&prefer=NZ"
            )
            .await,
            is("filter-street-town", Some("AU"))
        );
        assert_eq!((c.au.hits(), c.nz.hits(), c.ca.hits()), (1, 3, 0));
    }

    #[tokio::test]
    async fn prefer_keeps_a_street_held_by_several_countries() {
        let c = finding_countries().await;

        // Without prefer these three ask everyone.
        for query in [
            "geocode?address=1+King+St",
            "geocode?address=1+King+Street&locality=Wellington",
            "search?q=1+King+St",
        ] {
            assert_eq!(routed(&c.base, query).await.0, "fanout", "{query}");
            assert_eq!(
                routed(&c.base, &format!("{query}&prefer=NZ")).await,
                is("prefer", Some("NZ")),
                "{query}"
            );
        }
        // The same street held by prefer and one other: prefer keeps it.
        assert_eq!(
            routed(&c.base, "geocode?address=1+George+St&prefer=AU").await,
            is("prefer", Some("AU"))
        );
        assert_eq!(c.ca.hits(), 3, "only the fan-outs reached CA");
    }

    #[tokio::test]
    async fn a_sole_street_holder_beats_a_prefer_that_does_not_hold_it() {
        let c = finding_countries().await;

        for query in [
            "geocode?address=1+George+St&prefer=NZ",
            // The town is in no Pack, so the street is all there is.
            "geocode?address=1+George+St&locality=Nowhere&prefer=NZ",
            "search?q=1+George+Street&prefer=CA",
        ] {
            assert_eq!(
                routed(&c.base, query).await,
                is("filter-street", Some("AU")),
                "{query}"
            );
        }
        assert_eq!((c.nz.hits(), c.ca.hits()), (0, 0));
    }

    #[tokio::test]
    async fn prefer_is_asked_when_nothing_else_decides() {
        let c = finding_countries().await;

        for query in [
            // Held nowhere.
            "geocode?address=1+Unknown+Road&prefer=NZ",
            // No street at all.
            "geocode?address=Level+3&prefer=NZ",
            // A name with no number or town may be a place, so its street alone does not
            // route, but prefer is still asked.
            "search?q=George+Street&prefer=NZ",
            "autocomplete?q=George+Street&prefer=NZ",
        ] {
            assert_eq!(
                routed(&c.base, query).await,
                is("prefer-fallback", Some("NZ")),
                "{query}"
            );
        }
        // Typed so far, "King Street" is held by prefer: no locality needed.
        assert_eq!(
            routed(&c.base, "autocomplete?q=King+Street&prefer=NZ").await,
            is("prefer", Some("NZ"))
        );
        assert_eq!((c.au.hits(), c.nz.hits(), c.ca.hits()), (0, 5, 0));
    }

    #[tokio::test]
    async fn a_prefer_that_is_not_a_worker_is_ignored() {
        let c = finding_countries().await;

        for prefer in ["EU", "xx", "%20", ""] {
            assert_eq!(
                routed(
                    &c.base,
                    &format!("geocode?address=1+King+St&prefer={prefer}")
                )
                .await
                .0,
                "fanout",
                "{prefer:?}"
            );
            assert_eq!(
                routed(
                    &c.base,
                    &format!("geocode?address=1+George+St&prefer={prefer}")
                )
                .await,
                is("filter-street", Some("AU")),
                "{prefer:?}"
            );
        }
        // Case does not matter for a worker.
        assert_eq!(
            routed(&c.base, "geocode?address=1+King+St&prefer=nz").await,
            is("prefer", Some("NZ"))
        );
    }

    #[tokio::test]
    async fn country_beats_prefer() {
        let c = finding_countries().await;

        assert_eq!(
            routed(&c.base, "search?q=1+George+St&country=CA&prefer=NZ").await,
            is("country", Some("CA"))
        );
    }

    #[tokio::test]
    async fn a_prefer_without_a_filter_might_hold_the_street() {
        let hit = json!({ "label": "1 George Street", "score": 1.0 });
        let au = fake(
            json!([]),
            hit.clone(),
            Some(filter_of(&[("George Street", &["Sydney"])])),
            StatusCode::OK,
        )
        .await;
        let nz = fake(json!([]), hit, None, StatusCode::OK).await;
        let base = router(vec![("AU", au.url.clone()), ("NZ", nz.url.clone())]).await;

        // NZ blocks a single winner, so without prefer this asks everyone (and AU, the only
        // confirmed holder, wins the fan-out).
        assert_eq!(
            routed(&base, "geocode?address=1+George+St").await,
            is("fanout", Some("AU"))
        );
        // NZ has no filter, so it might hold the street; the town cannot be unique either.
        for query in [
            "geocode?address=1+George+St&prefer=NZ",
            "geocode?address=1+George+St&locality=Sydney&prefer=NZ",
        ] {
            assert_eq!(
                routed(&base, query).await,
                is("prefer", Some("NZ")),
                "{query}"
            );
        }
    }

    #[tokio::test]
    async fn a_prefer_with_nothing_to_say_falls_back_to_every_runtime() {
        let hit = json!({ "label": "1 King Street, Sydney", "score": 1.0 });
        let c = countries([hit, Value::Null, Value::Null]).await;

        let (status, headers, body) =
            get_full(&format!("{}/geocode?address=1+King+St&prefer=NZ", c.base)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(route_of(&headers), (Some("fanout"), Some("AU")));
        assert_eq!(body["result"]["country"], "AU");
        // NZ was asked first as prefer, found nothing, and is asked again with the rest.
        assert_eq!((c.au.hits(), c.nz.hits(), c.ca.hits()), (1, 2, 1));
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
    async fn a_shedding_runtime_is_not_asked_again_and_keeps_its_retry_after() {
        let hit = json!({ "label": "found", "score": 1.0 });
        let c = countries([hit, Value::Null, Value::Null]).await;
        c.nz.fail_with.store(503, Ordering::SeqCst);
        c.nz.retry_after.store(true, Ordering::SeqCst);

        // Routed to NZ, which sheds: the client gets that 503 and nobody is asked twice.
        let (status, headers, _) = get_full(&format!(
            "{}/geocode?address=15+Eruera+Street&locality=Rotorua",
            c.base
        ))
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(headers[header::RETRY_AFTER], "2");
        assert_eq!(route_of(&headers), (Some("filter-street-town"), Some("NZ")));
        assert_eq!((c.au.hits(), c.nz.hits(), c.ca.hits()), (0, 1, 0));

        // Fanned out and every Runtime shedding: the first one's 503 and Retry-After, not a 502.
        for fake in [&c.au, &c.ca] {
            fake.fail_with.store(503, Ordering::SeqCst);
            fake.retry_after.store(true, Ordering::SeqCst);
        }
        let (status, headers, _) = get_full(&format!("{}/geocode?address=1+King+St", c.base)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(headers[header::RETRY_AFTER], "2");
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

    async fn post_full(url: &str, body: impl Into<Bytes>) -> (StatusCode, HeaderMap, Value) {
        post_with(url, body, None).await
    }

    async fn post_with(
        url: &str,
        body: impl Into<Bytes>,
        cf_ray: Option<&str>,
    ) -> (StatusCode, HeaderMap, Value) {
        let client: Client<HttpConnector, Full<Bytes>> =
            Client::builder(TokioExecutor::new()).build_http();
        let mut request = Request::post(url).header(header::CONTENT_TYPE, "application/json");
        if let Some(ray) = cf_ray {
            request = request.header("cf-ray", ray);
        }
        let response = client
            .request(request.body(Full::new(body.into())).expect("request"))
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

    /// POST these items to `/geocode/batch` and return the `results`.
    async fn batch(base: &str, items: Value) -> Vec<Value> {
        let (status, _, body) = post_full(
            &format!("{base}/geocode/batch"),
            json!({ "items": items }).to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body["results"].as_array().expect("results").clone()
    }

    fn batch_route(item: &Value) -> Option<&str> {
        item["route"].as_str()
    }

    fn found(label: &str) -> Value {
        json!({ "label": label, "score": 1.0 })
    }

    #[tokio::test]
    async fn a_batch_is_grouped_by_runtime_and_answered_in_the_order_asked() {
        let c = countries([found("au"), found("nz"), found("ca")]).await;

        let results = batch(
            &c.base,
            json!([
                // The street with the town is NZ's alone.
                { "address": "15 Eruera Street", "locality": "Rotorua" },
                { "address": "3/12 George Street", "locality": "Sydney" },
                // The street alone is NZ's.
                { "address": "7 Eruera Street" },
                // The item names its country, case aside.
                { "address": "1 Toronto Road", "country": "ca" },
                // King Street is in all three, so every Runtime answers and nothing tells them apart.
                { "address": "1 King St" },
            ]),
        )
        .await;

        assert_eq!(results.len(), 5);
        assert_eq!(results[0]["result"]["label"], "nz");
        assert_eq!(results[0]["result"]["country"], "NZ");
        assert_eq!(batch_route(&results[0]), Some("filter-street-town"));
        assert_eq!(results[0]["query"], "15 Eruera Street");
        assert_eq!(results[1]["result"]["country"], "AU");
        assert_eq!(batch_route(&results[1]), Some("filter-street-town"));
        assert_eq!(results[2]["result"]["country"], "NZ");
        assert_eq!(batch_route(&results[2]), Some("filter-street"));
        assert_eq!(results[3]["result"]["country"], "CA");
        assert_eq!(batch_route(&results[3]), Some("country"));
        assert_eq!(results[4]["problem"]["error_code"], "country_required");
        assert_eq!(results[4]["problem"]["status"], 400);
        assert!(
            results[4]["problem"]["detail"]
                .as_str()
                .unwrap()
                .contains("AU, CA, NZ"),
        );
        assert_eq!(batch_route(&results[4]), Some("fanout"));

        // One sub-batch per Runtime, holding its items in the order given, and the fan-out item
        // in every one.
        assert_eq!(
            c.au.batches(),
            vec![vec!["3/12 George Street", "1 King St"]]
        );
        assert_eq!(
            c.nz.batches(),
            vec![vec!["15 Eruera Street", "7 Eruera Street", "1 King St"]]
        );
        assert_eq!(c.ca.batches(), vec![vec!["1 Toronto Road", "1 King St"]]);
        assert_eq!((c.au.hits(), c.nz.hits(), c.ca.hits()), (0, 0, 0));
    }

    #[tokio::test]
    async fn a_batch_item_that_goes_to_every_runtime_is_picked_as_geocode_picks() {
        let c = countries([Value::Null, found("nz"), Value::Null]).await;
        let results = batch(
            &c.base,
            json!([
                { "address": "1 King St" },
                { "address": "nothing" },
                { "address": "bad" },
                { "address": "2 King St" },
            ]),
        )
        .await;

        // The one non-null answer wins, whatever the others said.
        assert_eq!(results[0]["result"]["country"], "NZ");
        assert_eq!(batch_route(&results[0]), Some("fanout"));
        // All null: a null result and no country.
        assert!(results[1]["result"].is_null());
        assert!(results[1]["result"].get("country").is_none());
        assert_eq!(results[1]["query"], "nothing");
        // A Runtime's 400 for an item is every Runtime's, and it does not fail the others.
        assert_eq!(results[2]["problem"]["error_code"], "invalid_query");
        assert_eq!(results[3]["result"]["country"], "NZ");
        for fake in [&c.au, &c.nz, &c.ca] {
            assert_eq!(fake.batches().len(), 1);
            assert_eq!(fake.batches()[0].len(), 4);
        }
    }

    #[tokio::test]
    async fn a_batch_item_follows_geocode_when_its_routed_runtime_finds_nothing() {
        // The filters name NZ for Eruera Street in Rotorua, NZ finds nothing, and AU does.
        let c = countries([found("au"), Value::Null, Value::Null]).await;
        let results = batch(
            &c.base,
            json!([
                { "address": "15 Eruera Street", "locality": "Rotorua" },
                { "address": "15 Eruera Street", "locality": "Rotorua", "country": "NZ" },
            ]),
        )
        .await;
        assert_eq!(results[0]["result"]["country"], "AU");
        assert_eq!(batch_route(&results[0]), Some("fanout"));
        // The item that named NZ is not asked of the others.
        assert!(results[1]["result"].is_null());
        assert_eq!(batch_route(&results[1]), Some("country"));
        // NZ was asked once for each, and only the first item went on to the others.
        assert_eq!(
            c.nz.batches(),
            vec![vec!["15 Eruera Street", "15 Eruera Street"]]
        );
        assert_eq!(c.au.batches(), vec![vec!["15 Eruera Street"]]);
        assert_eq!(c.ca.batches(), vec![vec!["15 Eruera Street"]]);

        // With one Runtime there is nobody else to ask: its null answer is the answer.
        let nz = fake(
            json!([]),
            Value::Null,
            Some(filter_of(&[("Eruera Street", &["Rotorua"])])),
            StatusCode::OK,
        )
        .await;
        let base = router(vec![("NZ", nz.url.clone())]).await;
        let results = batch(
            &base,
            json!([{ "address": "15 Eruera Street", "locality": "Rotorua" }]),
        )
        .await;
        assert!(results[0]["result"].is_null());
        assert_eq!(results[0]["query"], "15 Eruera Street");
        assert_eq!(nz.batches().len(), 1);
    }

    #[tokio::test]
    async fn a_failing_runtime_fails_only_its_own_batch_items() {
        let c = countries([found("au"), found("nz"), Value::Null]).await;
        c.nz.fail_with.store(500, Ordering::SeqCst);

        let results = batch(
            &c.base,
            json!([
                // Routed to NZ, which fails: the others are asked, as for GET.
                { "address": "15 Eruera Street", "locality": "Rotorua" },
                // Named NZ: nobody else may answer.
                { "address": "1 Toronto Road", "country": "NZ" },
                { "address": "3/12 George Street", "locality": "Sydney" },
                // To every Runtime, one of which failed: it is left out.
                { "address": "1 King St" },
            ]),
        )
        .await;
        assert_eq!(results[0]["result"]["country"], "AU");
        assert_eq!(results[1]["problem"]["error_code"], "upstream_unavailable");
        assert_eq!(results[1]["problem"]["status"], 502);
        assert_eq!(results[2]["result"]["country"], "AU");
        assert_eq!(results[3]["result"]["country"], "AU");

        // Every Runtime failing leaves an item without any answer to give.
        c.au.fail_with.store(500, Ordering::SeqCst);
        c.ca.fail_with.store(500, Ordering::SeqCst);
        let items = json!([
            { "address": "1 King St" },
            { "address": "3/12 George Street", "locality": "Sydney" },
        ]);
        let results = batch(&c.base, items.clone()).await;
        assert_eq!(results[0]["problem"]["error_code"], "upstream_unavailable");
        assert_eq!(results[1]["problem"]["error_code"], "upstream_unavailable");

        // One of them shedding load says so, as GET does when nothing else answered.
        c.ca.fail_with.store(503, Ordering::SeqCst);
        c.ca.retry_after.store(true, Ordering::SeqCst);
        let results = batch(&c.base, items).await;
        assert_eq!(results[0]["problem"]["error_code"], "overloaded");
        assert_eq!(results[1]["problem"]["error_code"], "overloaded");
    }

    #[tokio::test]
    async fn a_result_entry_that_is_not_an_object_fails_its_item_and_no_other() {
        let c = countries([found("au"), found("nz"), found("ca")]).await;
        let results = batch(
            &c.base,
            json!([
                { "address": "garbage", "country": "NZ" },
                { "address": "1 Toronto Road", "country": "CA" },
                // Routed to NZ, which gave nothing usable: the others are asked, and they too.
                { "address": "garbage", "prefer": "NZ" },
                { "address": "garbage" },
            ]),
        )
        .await;
        assert_eq!(results[0]["problem"]["error_code"], "upstream_unavailable");
        assert_eq!(results[0]["problem"]["status"], 502);
        assert_eq!(batch_route(&results[0]), Some("country"));
        assert_eq!(results[1]["result"]["country"], "CA");
        assert_eq!(results[2]["problem"]["error_code"], "upstream_unavailable");
        assert_eq!(batch_route(&results[2]), Some("fanout"));
        assert_eq!(results[3]["problem"]["error_code"], "upstream_unavailable");
        assert_eq!(batch_route(&results[3]), Some("fanout"));
    }

    #[tokio::test]
    async fn a_shed_item_stands_and_is_not_asked_elsewhere() {
        let c = countries([found("au"), found("nz"), found("ca")]).await;
        let results = batch(
            &c.base,
            json!([{ "address": "shed", "country": "NZ" }, { "address": "1 Toronto Road", "country": "CA" }]),
        )
        .await;
        assert_eq!(results[0]["problem"]["error_code"], "overloaded");
        assert_eq!(results[1]["result"]["country"], "CA");
        assert_eq!(c.au.batches().len(), 0);
    }

    #[tokio::test]
    async fn an_item_without_an_address_is_answered_by_the_router_and_asked_of_nobody() {
        let c = countries([found("au"), found("nz"), found("ca")]).await;
        let results = batch(
            &c.base,
            json!([
                "10 King Street",
                [1, 2],
                { "locality": "Rotorua" },
                { "address": null },
                { "address": "  " },
                { "address": 5 },
                { "address": "", "country": "nz" },
                { "address": "", "prefer": "ca" },
                { "address": "", "country": "FR" },
                { "address": "15 Eruera Street", "locality": "Rotorua" },
            ]),
        )
        .await;
        let problem = |i: usize, code: &str, route: Option<&str>| {
            assert_eq!(results[i]["problem"]["error_code"], code, "item {i}");
            assert_eq!(results[i]["problem"]["status"], 400, "item {i}");
            assert_eq!(batch_route(&results[i]), route, "item {i}");
        };
        problem(0, "malformed_parameter", Some("fanout"));
        problem(1, "malformed_parameter", Some("fanout"));
        problem(2, "invalid_query", Some("fanout"));
        problem(3, "invalid_query", Some("fanout"));
        problem(4, "invalid_query", Some("fanout"));
        problem(5, "malformed_parameter", Some("fanout"));
        problem(6, "invalid_query", Some("country"));
        problem(7, "invalid_query", Some("prefer-fallback"));
        // An unknown country is still the router's own Problem, without a route.
        problem(8, "unknown_country", None);
        assert_eq!(results[9]["result"]["country"], "NZ");
        // Only the valid item reached a Runtime.
        assert_eq!(c.nz.batches(), vec![vec!["15 Eruera Street"]]);
        assert_eq!(c.au.batches().len(), 0);
        assert_eq!(c.ca.batches().len(), 0);
    }

    #[tokio::test]
    async fn country_and_prefer_apply_to_each_item() {
        let c = countries([found("au"), found("nz"), found("ca")]).await;
        let results = batch(
            &c.base,
            json!([
                // `prefer` holds the street, so it wins over the fan-out.
                { "address": "1 King St", "prefer": "nz" },
                // `country` beats `prefer`.
                { "address": "1 King St", "country": "AU", "prefer": "NZ" },
                // A `prefer` that is not a Runtime is ignored: back to every Runtime.
                { "address": "1 King St", "prefer": "EU" },
                // A street and town held by one Runtime beats `prefer`.
                { "address": "3/12 George Street", "locality": "Sydney", "prefer": "CA" },
                { "address": "1 King St", "country": "FR" },
                { "address": "1 King St", "country": "" },
            ]),
        )
        .await;
        assert_eq!(results[0]["result"]["country"], "NZ");
        assert_eq!(batch_route(&results[0]), Some("prefer"));
        assert_eq!(results[1]["result"]["country"], "AU");
        assert_eq!(batch_route(&results[1]), Some("country"));
        assert_eq!(results[2]["problem"]["error_code"], "country_required");
        assert_eq!(batch_route(&results[2]), Some("fanout"));
        assert_eq!(results[3]["result"]["country"], "AU");
        assert_eq!(batch_route(&results[3]), Some("filter-street-town"));
        // An unknown country is that item's Problem, and nobody is asked for it.
        assert_eq!(results[4]["problem"]["error_code"], "unknown_country");
        assert!(
            results[4]["problem"]["detail"]
                .as_str()
                .unwrap()
                .contains("configured: AU, CA, NZ")
        );
        assert!(results[4].get("route").is_none());
        // An empty country is no country.
        assert_eq!(results[5]["problem"]["error_code"], "country_required");
        assert_eq!(
            c.nz.batches(),
            vec![vec!["1 King St", "1 King St", "1 King St"]]
        );
    }

    #[tokio::test]
    async fn a_batch_keeps_the_boundary_policy() {
        let c = countries([found("au"), found("nz"), found("ca")]).await;
        let url = format!("{}/geocode/batch", c.base);

        // The request id goes to every Runtime asked.
        let (status, headers, _) = post_with(
            &url,
            json!({ "items": [{ "address": "1 King St" }] }).to_string(),
            Some("ray-batch-9"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["x-request-id"], "ray-batch-9");
        for fake in [&c.au, &c.nz, &c.ca] {
            let batches = fake.batches.lock().unwrap();
            assert_eq!(batches[0].0.as_deref(), Some("ray-batch-9"));
        }

        // A body that is not a batch fails the request, as a Problem with the request id.
        for body in ["", "nope", "{}", r#"{"items": 3}"#] {
            let (status, headers, problem) = post_with(&url, body, Some("ray-bad")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
            assert_eq!(headers[header::CONTENT_TYPE], "application/problem+json");
            assert_eq!(problem["error_code"], "malformed_parameter");
            assert_eq!(problem["request_id"], "ray-bad");
        }
        let items = |count: usize| {
            json!({ "items": vec![json!({ "address": "nothing" }); count] }).to_string()
        };
        let (status, _, problem) = post_full(&url, items(bounds::MAX_BATCH_ITEMS + 1)).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(problem["error_code"], "batch_too_large");
        let (status, _, body) = post_full(&url, items(bounds::MAX_BATCH_ITEMS)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["results"].as_array().map(Vec::len),
            Some(bounds::MAX_BATCH_ITEMS)
        );
        let padding = " ".repeat(bounds::MAX_BATCH_BODY_BYTES);
        let (status, _, problem) = post_full(&url, format!(r#"{{"items": []{padding}}}"#)).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(problem["error_code"], "batch_too_large");

        // POST only.
        let (status, headers, problem) = get_full(&url).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(headers[header::ALLOW], "POST");
        assert_eq!(problem["error_code"], "method_not_allowed");
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
