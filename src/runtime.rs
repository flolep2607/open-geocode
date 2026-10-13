use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    Extension, Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Query, State, rejection::BytesRejection},
    handler::{Handler, HandlerWithoutStateExt},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::{MethodFilter, MethodRouter, on},
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
    task,
};
use tower_http::services::{ServeDir, ServeFile};

use crate::{
    container::Bytes,
    http::{
        bounds, health, method,
        problem::{Problem, classify_search_error},
        request_id::{self, RequestId},
        shutdown::shutdown_signal,
    },
    pack::{PackReader, RecordPoint, RecordPointPrecision, RecordSource},
    record::OsmObjectType,
    reverse::{PackReverseGeocoder, ReverseGeocodeOptions, ReverseGeocodeResponse},
    search::{
        AddressGeocodeOptions, PackTextSearcher, TextAutocompleteOptions, TextSearchHit,
        TextSearchOptions,
    },
    street_filter::{ROUTING_FILTER_SECTION, ROUTING_FILTER_VERSION},
};

#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub pack: PathBuf,
    pub demo: PathBuf,
    pub bind: SocketAddr,
    pub basemap: PathBuf,
    /// API requests allowed on the blocking pool at once; at least 1.
    pub max_concurrency: usize,
}

/// How long an API request waits for a blocking-pool slot before it is shed with 503.
const QUEUE_TIMEOUT: Duration = Duration::from_secs(2);

/// Items of a `/geocode/batch` that share one blocking-pool slot. Small, so that a batch takes its
/// slots one chunk at a time and requests that arrive meanwhile queue behind a chunk (a few
/// milliseconds) rather than behind the batch; large enough that the slot and the task cost
/// nothing next to the searches.
const BATCH_CHUNK: usize = 16;

/// The default for `serve --max-concurrency`: one request per core.
pub fn default_max_concurrency() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN)
}

#[derive(Clone)]
pub(crate) struct AppState {
    searcher: Arc<PackTextSearcher>,
    reverse_geocoder: Arc<PackReverseGeocoder>,
    /// The Pack's routing filter, when it has one, for `/routing-filter`.
    routing_filter: Option<Arc<RoutingFilterSection>>,
    ready: Arc<AtomicBool>,
    /// Bounds the API work on `spawn_blocking`, whose pool would otherwise grow to 512 threads
    /// under a burst and queue requests without limit.
    permits: Arc<Semaphore>,
    queue_timeout: Duration,
}

/// The `routing/filter` section as the Pack holds it. `bytes` is a slice of the mapped file, so
/// serving it copies nothing.
struct RoutingFilterSection {
    bytes: Bytes,
    /// The section's own checksum, so an unchanged filter costs the router a 304.
    etag: String,
}

impl AppState {
    fn open(path: &Path) -> Result<Self> {
        let pack = Arc::new(PackReader::open(path)?);
        // A filter this build cannot read (a newer format) only costs the router its
        // street-based routing for this Runtime; it must not stop the Pack serving.
        let routing_filter = match pack
            .container()
            .optional_section(ROUTING_FILTER_SECTION, ROUTING_FILTER_VERSION)
        {
            Ok(bytes) => bytes.map(|bytes| {
                let checksum = pack.container().sections()[ROUTING_FILTER_SECTION].checksum;
                Arc::new(RoutingFilterSection {
                    bytes,
                    etag: format!("\"{checksum:016x}\""),
                })
            }),
            Err(error) => {
                tracing::warn!("not serving a routing filter: {error:#}");
                None
            }
        };
        let searcher = PackTextSearcher::from_pack(Arc::clone(&pack))?;
        let reverse_geocoder = PackReverseGeocoder::from_pack(pack)?;
        Ok(Self {
            searcher: Arc::new(searcher),
            reverse_geocoder: Arc::new(reverse_geocoder),
            routing_filter,
            ready: Arc::new(AtomicBool::new(true)),
            permits: Arc::new(Semaphore::new(default_max_concurrency().get())),
            queue_timeout: QUEUE_TIMEOUT,
        })
    }

    fn with_max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.permits = Arc::new(Semaphore::new(max_concurrency.max(1)));
        self
    }

    /// True once the Pack is loaded and the Runtime can answer API traffic
    /// (ADR 0017 Decision 28).
    pub(crate) fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}

#[derive(Debug, Deserialize)]
struct SearchParams {
    q: Option<String>,
    #[serde(default)]
    limit: usize,
    layer: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GeocodeParams {
    address: Option<String>,
    locality: Option<String>,
    region: Option<String>,
    postcode: Option<String>,
    #[serde(default)]
    limit: usize,
    layer: Option<String>,
    #[serde(default)]
    street_fallback: bool,
}

impl GeocodeParams {
    /// The lookup these parameters ask for, or why they cannot be one. The one place that
    /// validates `/geocode`, shared by the GET route and every item of `/geocode/batch` so the
    /// two cannot drift.
    fn into_options(self) -> Result<AddressGeocodeOptions, Problem> {
        let address = bounds::validate_query_length(self.address.as_deref().unwrap_or_default())?;
        if address.is_empty() {
            return Err(Problem::invalid_query("address must not be empty"));
        }
        let context = |value: Option<String>| -> Result<Option<String>, Problem> {
            let value = bounds::validate_query_length(value.as_deref().unwrap_or_default())?;
            Ok(Some(value).filter(|value| !value.is_empty()))
        };
        let locality = context(self.locality)?;
        let region = context(self.region)?;
        let postcode = context(self.postcode)?;
        bounds::validate_search_limit(self.limit)?;
        Ok(AddressGeocodeOptions {
            address,
            locality,
            region,
            postcode,
            limit: self.limit,
            layer: self.layer,
            street_fallback: self.street_fallback,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ReverseParams {
    lon: f64,
    lat: f64,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct SearchResponse {
    pub query: String,
    pub results: Vec<SearchApiResult>,
}

/// The one best match for a structured address, or `null` when nothing in the
/// Pack fits its locality, region and postcode. `query` is the rewrite that matched.
#[derive(Debug, Serialize, PartialEq)]
pub struct GeocodeResponse {
    pub query: String,
    pub result: Option<SearchApiResult>,
}

/// One item of a `/geocode/batch` answer, in the position of the item it answers: what `GET
/// /geocode` would have answered, or the Problem it would have answered instead.
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum BatchItem {
    Answer(Box<GeocodeResponse>),
    Failed { problem: Problem },
}

#[derive(Debug, Serialize)]
struct BatchResponse {
    results: Vec<BatchItem>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct AutocompleteResponse {
    pub query: String,
    pub suggestions: Vec<SearchApiResult>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct SearchApiResult {
    pub record_id: u64,
    pub id: String,
    pub layer: String,
    pub label: String,
    pub score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub point: Option<SearchApiPoint>,
    pub source: SearchApiSource,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SearchApiPoint {
    pub lon: f64,
    pub lat: f64,
    pub precision: SearchApiPointPrecision,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SearchApiPointPrecision {
    Point,
    Centroid,
    RepresentativePoint,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SearchApiSource {
    pub dataset: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_type: Option<OsmObjectType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub derived_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_count: Option<u64>,
}

pub async fn serve(options: ServeOptions) -> Result<()> {
    let state = AppState::open(&options.pack)
        .with_context(|| format!("failed to open Pack {}", options.pack.display()))?
        .with_max_concurrency(options.max_concurrency);
    let ready = Arc::clone(&state.ready);
    let app = build_router(state, &options.demo, &options.basemap);

    let listener = TcpListener::bind(options.bind)
        .await
        .with_context(|| format!("failed to bind {}", options.bind))?;
    tracing::info!(
        pack = %options.pack.display(),
        max_concurrency = options.max_concurrency.max(1),
        "serving at http://{}",
        options.bind
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            // Not ready from here on, so a probe that still reaches us takes us out of rotation
            // while in-flight requests finish.
            ready.store(false, Ordering::Release);
            tracing::info!("shutdown signal received, draining in-flight requests");
        })
        .await
        .context("runtime server failed")?;
    tracing::info!("runtime stopped");
    Ok(())
}

/// Assemble the public-demo router with its boundary policy: method-restricted
/// routes (ADR 0017 Decision 27), Problem Details 404/405 fallbacks
/// (Decisions 27, 37), and request-id propagation (Decision 30). The basemap is
/// served raw with no guard — in the deployed topology PMTiles is served from
/// Cloudflare R2, not the Runtime (Decisions 6/35a, amended 2026-06-03).
///
/// Split out from [`serve`] so it can be exercised without binding a socket.
pub(crate) fn build_router(state: AppState, demo: &Path, basemap: &Path) -> Router {
    // Read-only API and health routes: GET only (Decision 27). axum would
    // otherwise route HEAD to the GET handler automatically, so `get_only`
    // registers an explicit HEAD responder that returns 405. Other methods fall
    // through to `method_not_allowed_fallback`.
    let mut app: Router<AppState> = Router::new()
        .route("/search", get_only(search))
        .route("/autocomplete", get_only(autocomplete))
        .route("/geocode", get_only(geocode))
        .route(
            "/geocode/batch",
            post_only(geocode_batch).layer(DefaultBodyLimit::max(bounds::MAX_BATCH_BODY_BYTES)),
        )
        .route("/reverse", get_only(reverse))
        .route("/routing-filter", get_only(routing_filter))
        .route("/healthz", get_only(health::healthz))
        .route("/readyz", get_only(health::readyz));

    // The basemap PMTiles lives with the other data artifacts (not in the demo
    // dir), so serve it explicitly. ServeFile honors HTTP range requests, which
    // is what the PMTiles client uses. Skip it if the file is absent so a fresh
    // clone without a basemap still serves the API and demo.
    //
    // This is served raw and is a local-dev convenience only. In the public
    // demo the basemap is served from Cloudflare R2, not the Runtime (ADR 0017
    // Decisions 6/7/8, amended 2026-06-03); R2's free egress makes PMTiles
    // abuse protection a non-issue. The bare binary hands back the file with
    // native range support and no policy.
    if basemap.exists() {
        app = app.route_service("/basemap.pmtiles", ServeFile::new(basemap));
        tracing::info!(basemap = %basemap.display(), "serving basemap");
    } else {
        tracing::warn!(
            basemap = %basemap.display(),
            "basemap not found; serving demo without a basemap"
        );
    }

    // ServeDir keeps local-dev static serving (Decision 10) but its built-in 404
    // is replaced with a Problem Details 404 so unknown paths don't fall through
    // to a static-file response (Decision 37). `method_not_allowed_fallback`
    // turns method mismatches into a Problem 405 instead of an empty body. The
    // request-id layer is outermost so it also wraps both fallbacks, and the
    // trace layer inside it can read the id.
    app.method_not_allowed_fallback(method::method_not_allowed)
        .fallback_service(ServeDir::new(demo).not_found_service(method::not_found.into_service()))
        .layer(request_id::trace_layer())
        .layer(middleware::from_fn(request_id::propagate))
        .with_state(state)
}

/// A GET-only route that also rejects HEAD with 405, defeating axum's default
/// HEAD-to-GET routing so the public method surface stays exactly GET.
fn get_only<H, T>(handler: H) -> MethodRouter<AppState>
where
    H: Handler<T, AppState>,
    T: 'static,
{
    on(MethodFilter::GET, handler).on(MethodFilter::HEAD, method::method_not_allowed)
}

/// A POST-only route. Every other method, HEAD included, is a 405 that says `Allow: POST`.
fn post_only<H, T>(handler: H) -> MethodRouter<AppState>
where
    H: Handler<T, AppState>,
    T: 'static,
{
    on(MethodFilter::POST, handler).fallback(method::post_only_method_not_allowed)
}

/// Attach the request id (if present) to a Problem so it appears in the body.
fn with_id(problem: Problem, request_id: &Option<String>) -> Problem {
    match request_id {
        Some(id) => problem.with_request_id(id.clone()),
        None => problem,
    }
}

/// A bare 500 for the client, with the cause logged here under the request id: the body must not
/// carry it (ADR 0017 Decision 31), so the log is the only place it survives.
fn internal(cause: impl std::fmt::Display, request_id: &Option<String>) -> Problem {
    tracing::error!(
        request_id = request_id.as_deref().unwrap_or("-"),
        "internal error: {cause:#}"
    );
    with_id(Problem::internal(), request_id)
}

/// A slot on the blocking pool, or 503 when none frees up within the queue timeout. The permit
/// is moved into the blocking closure, so the slot frees only when that work really ends, even if
/// the client has gone.
async fn acquire_permit(
    state: &AppState,
    request_id: &Option<String>,
) -> Result<OwnedSemaphorePermit, Problem> {
    match tokio::time::timeout(
        state.queue_timeout,
        Arc::clone(&state.permits).acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => Ok(permit),
        Ok(Err(closed)) => Err(internal(closed, request_id)),
        Err(_) => Err(with_id(Problem::overloaded(), request_id)),
    }
}

/// A search error as a Problem, logging the cause when it is ours rather than the query's.
fn search_error(error: anyhow::Error, request_id: &Option<String>) -> Problem {
    let cause = format!("{error:#}");
    let problem = classify_search_error(error);
    if problem.is_internal() {
        return internal(cause, request_id);
    }
    with_id(problem, request_id)
}

async fn search(
    State(state): State<AppState>,
    request_id: Option<Extension<RequestId>>,
    params: Result<Query<SearchParams>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<SearchResponse>, Problem> {
    let request_id = request_id.map(|Extension(RequestId(id))| id);
    let Query(params) = params.map_err(|_| {
        with_id(
            Problem::malformed_parameter("could not parse query parameters"),
            &request_id,
        )
    })?;

    let query = bounds::validate_query(params.q.as_deref().unwrap_or_default())
        .map_err(|problem| with_id(problem, &request_id))?;
    bounds::validate_search_limit(params.limit).map_err(|problem| with_id(problem, &request_id))?;

    let options = TextSearchOptions {
        query: query.clone(),
        limit: params.limit,
        layer: params.layer,
    };
    let permit = acquire_permit(&state, &request_id).await?;
    let searcher = Arc::clone(&state.searcher);
    let hits = task::spawn_blocking(move || {
        let _permit = permit;
        searcher.search(options)
    })
    .await
    .map_err(|error| internal(error, &request_id))?
    .map_err(|error| search_error(error, &request_id))?;

    Ok(Json(SearchResponse {
        query,
        results: hits.into_iter().map(SearchApiResult::from_hit).collect(),
    }))
}

/// `/geocode`: the structured-address lookup `batch-geocode` runs per row. The
/// street address is searched on its own and hits are kept only when they fit the
/// given locality, region and postcode, so a town in the query cannot outrank the
/// street.
async fn geocode(
    State(state): State<AppState>,
    request_id: Option<Extension<RequestId>>,
    params: Result<Query<GeocodeParams>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<GeocodeResponse>, Problem> {
    let request_id = request_id.map(|Extension(RequestId(id))| id);
    let Query(params) = params.map_err(|_| {
        with_id(
            Problem::malformed_parameter("could not parse query parameters"),
            &request_id,
        )
    })?;
    let options = params
        .into_options()
        .map_err(|problem| with_id(problem, &request_id))?;

    let permit = acquire_permit(&state, &request_id).await?;
    let searcher = Arc::clone(&state.searcher);
    let response = task::spawn_blocking(move || {
        let _permit = permit;
        geocode_one(&searcher, options)
    })
    .await
    .map_err(|error| internal(error, &request_id))?
    .map_err(|error| search_error(error, &request_id))?;

    Ok(Json(response))
}

/// One validated `/geocode` lookup, answered the way both `/geocode` and `/geocode/batch` do: the
/// rewrite that matched with its hit, or the address itself with a `null` result.
fn geocode_one(
    searcher: &PackTextSearcher,
    options: AddressGeocodeOptions,
) -> anyhow::Result<GeocodeResponse> {
    let address = options.address.clone();
    Ok(match searcher.geocode_address(options)? {
        Some(hit) => GeocodeResponse {
            query: hit.query,
            result: Some(SearchApiResult::from_hit(hit.hit)),
        },
        None => GeocodeResponse {
            query: address,
            result: None,
        },
    })
}

/// `POST /geocode/batch`: many `/geocode` lookups in one request, answered in the order asked.
///
/// A client geocoding millions of addresses otherwise pays an HTTP request, and its own
/// per-request CPU, for each. An item is the parameters of `GET /geocode` as JSON and is
/// validated exactly like it; an invalid one answers its own Problem and does not fail the rest.
/// Only a body that is not the batch itself (malformed, too many items, too large) fails the
/// request.
///
/// The items run in chunks of [`BATCH_CHUNK`] on the blocking pool, each chunk under one of the
/// slots `/geocode` takes one of per request. So a batch is held to the same `--max-concurrency`
/// as the single routes, however many cores it could use, and shares the slots with them in
/// arrival order. One slot for the whole batch would let it run on every core and still count as
/// one request. A batch that cannot get its first slot in time is shed like any request (503);
/// one that gets some and then none answers `overloaded` for the items it did not run, which the
/// client retries, instead of throwing away the work done.
async fn geocode_batch(
    State(state): State<AppState>,
    request_id: Option<Extension<RequestId>>,
    body: Result<axum::body::Bytes, BytesRejection>,
) -> Result<Json<BatchResponse>, Problem> {
    let request_id = request_id.map(|Extension(RequestId(id))| id);
    let items = bounds::validate_batch(body).map_err(|problem| with_id(problem, &request_id))?;
    let total = items.len();

    // Validated up front: it is cheap, and an item that is not a lookup costs no search.
    let lookups = items.into_iter().map(|item| {
        serde_json::from_value::<GeocodeParams>(item)
            .map_err(|_| {
                Problem::malformed_parameter(
                    "an item must be an object of address, locality, region, postcode, layer, limit and street_fallback",
                )
            })
            .and_then(GeocodeParams::into_options)
            .map_err(|problem| with_id(problem, &request_id))
    });

    let mut lookups = lookups.peekable();
    let mut chunks = Vec::new();
    let mut shed = false;
    while lookups.peek().is_some() {
        let chunk: Vec<_> = lookups.by_ref().take(BATCH_CHUNK).collect();
        let len = chunk.len();
        let permit = if shed {
            None
        } else {
            match acquire_permit(&state, &request_id).await {
                Ok(permit) => Some(permit),
                // Nothing has run yet, so the whole request is shed.
                Err(problem) if chunks.is_empty() => return Err(problem),
                Err(_) => {
                    shed = true;
                    None
                }
            }
        };
        let handle = permit.map(|permit| {
            let searcher = Arc::clone(&state.searcher);
            let request_id = request_id.clone();
            task::spawn_blocking(move || {
                let _permit = permit;
                chunk
                    .into_iter()
                    .map(|lookup| match lookup {
                        Ok(options) => match geocode_one(&searcher, options) {
                            Ok(answer) => BatchItem::Answer(Box::new(answer)),
                            Err(error) => BatchItem::Failed {
                                problem: search_error(error, &request_id),
                            },
                        },
                        Err(problem) => BatchItem::Failed { problem },
                    })
                    .collect::<Vec<_>>()
            })
        });
        chunks.push((len, handle));
    }

    let mut results = Vec::with_capacity(total);
    for (len, handle) in chunks {
        match handle {
            Some(handle) => {
                results.extend(handle.await.map_err(|error| internal(error, &request_id))?)
            }
            None => results.extend((0..len).map(|_| BatchItem::Failed {
                problem: with_id(Problem::overloaded(), &request_id),
            })),
        }
    }

    let found = results
        .iter()
        .filter(|item| matches!(item, BatchItem::Answer(answer) if answer.result.is_some()))
        .count();
    let answered = results
        .iter()
        .filter(|item| matches!(item, BatchItem::Answer(_)))
        .count();
    tracing::info!(
        request_id = request_id.as_deref().unwrap_or("-"),
        items = total,
        found,
        not_found = answered - found,
        problems = total - answered,
        "geocode batch"
    );
    Ok(Json(BatchResponse { results }))
}

async fn autocomplete(
    State(state): State<AppState>,
    request_id: Option<Extension<RequestId>>,
    params: Result<Query<SearchParams>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<AutocompleteResponse>, Problem> {
    let request_id = request_id.map(|Extension(RequestId(id))| id);
    let Query(params) = params.map_err(|_| {
        with_id(
            Problem::malformed_parameter("could not parse query parameters"),
            &request_id,
        )
    })?;

    // Autocomplete tolerates an empty/short query (it returns no suggestions), so
    // only bound the length here; the searcher caps the result count internally.
    let query = bounds::validate_query_length(params.q.as_deref().unwrap_or_default())
        .map_err(|problem| with_id(problem, &request_id))?;
    bounds::validate_autocomplete_limit(params.limit)
        .map_err(|problem| with_id(problem, &request_id))?;

    let options = TextAutocompleteOptions {
        query: query.clone(),
        limit: params.limit,
        layer: params.layer,
    };
    let permit = acquire_permit(&state, &request_id).await?;
    let searcher = Arc::clone(&state.searcher);
    let hits = task::spawn_blocking(move || {
        let _permit = permit;
        searcher.autocomplete(options)
    })
    .await
    .map_err(|error| internal(error, &request_id))?
    .map_err(|error| search_error(error, &request_id))?;

    Ok(Json(AutocompleteResponse {
        query,
        suggestions: hits.into_iter().map(SearchApiResult::from_hit).collect(),
    }))
}

async fn reverse(
    State(state): State<AppState>,
    request_id: Option<Extension<RequestId>>,
    params: Result<Query<ReverseParams>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<ReverseGeocodeResponse>, Problem> {
    let request_id = request_id.map(|Extension(RequestId(id))| id);
    let Query(params) = params.map_err(|_| {
        with_id(
            Problem::malformed_parameter("could not parse query parameters"),
            &request_id,
        )
    })?;

    bounds::validate_coords(params.lon, params.lat)
        .map_err(|problem| with_id(problem, &request_id))?;

    let permit = acquire_permit(&state, &request_id).await?;
    let geocoder = Arc::clone(&state.reverse_geocoder);
    let response = task::spawn_blocking(move || {
        let _permit = permit;
        geocoder.reverse(ReverseGeocodeOptions {
            lon: params.lon,
            lat: params.lat,
        })
    })
    .await
    .map_err(|error| internal(error, &request_id))?
    .map_err(|error| internal(error, &request_id))?;

    Ok(Json(response))
}

/// `/routing-filter`: the Pack's street membership filter, for `open-geocode route` to decide which
/// Runtime holds a street (see [`crate::routing`]). The format, version and hash seed are in the
/// section's own header. The ETag is the section checksum, so a router refreshing every few
/// minutes pays for a 304 until the Pack is swapped.
async fn routing_filter(
    State(state): State<AppState>,
    request_id: Option<Extension<RequestId>>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id.map(|Extension(RequestId(id))| id);
    let Some(section) = state.routing_filter else {
        return with_id(
            Problem::not_found().with_detail("this Pack has no routing filter"),
            &request_id,
        )
        .into_response();
    };
    let etag = HeaderValue::from_str(&section.etag).expect("a hex etag is a header value");
    let unchanged = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|tags| {
            tags.split(',')
                .any(|tag| tag.trim().trim_start_matches("W/") == section.etag)
        });
    let mut response = if unchanged {
        (StatusCode::NOT_MODIFIED, Body::empty()).into_response()
    } else {
        let mut response = Response::new(Body::from(hyper::body::Bytes::from_owner(
            section.bytes.clone(),
        )));
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        response
    };
    response.headers_mut().insert(header::ETAG, etag);
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

impl SearchApiResult {
    fn from_hit(hit: TextSearchHit) -> Self {
        Self {
            record_id: hit.record_id,
            id: hit.record.id,
            layer: hit.record.layer,
            label: hit.record.label,
            score: hit.score,
            point: hit.record.point.map(SearchApiPoint::from),
            source: SearchApiSource::from(hit.record.source),
        }
    }
}

impl From<RecordPoint> for SearchApiPoint {
    fn from(point: RecordPoint) -> Self {
        Self {
            lon: point.lon,
            lat: point.lat,
            precision: match point.precision {
                RecordPointPrecision::Point => SearchApiPointPrecision::Point,
                RecordPointPrecision::Centroid => SearchApiPointPrecision::Centroid,
                RecordPointPrecision::RepresentativePoint => {
                    SearchApiPointPrecision::RepresentativePoint
                }
            },
        }
    }
}

impl From<RecordSource> for SearchApiSource {
    fn from(source: RecordSource) -> Self {
        Self {
            dataset: source.dataset,
            object_type: source.object_type,
            object_id: source.object_id,
            derived_from: source.derived_from,
            record_count: source.record_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pack::RecordSummary;
    use crate::record::OsmObjectType;

    use super::*;

    #[test]
    fn api_result_shapes_address_point() {
        let hit = TextSearchHit {
            record_id: 7,
            score: 4.25,
            record: RecordSummary {
                id: "osm:node:1".to_string(),
                layer: "address".to_string(),
                label: "10 King Street, Toronto".to_string(),
                point: Some(RecordPoint {
                    lon: -79.3832,
                    lat: 43.6532,
                    precision: RecordPointPrecision::Point,
                }),
                source: RecordSource {
                    dataset: "osm".to_string(),
                    object_type: Some(OsmObjectType::Node),
                    object_id: Some(1),
                    derived_from: None,
                    record_count: None,
                },
            },
        };

        let result = SearchApiResult::from_hit(hit);

        assert_eq!(result.record_id, 7);
        assert_eq!(result.id, "osm:node:1");
        assert_eq!(result.layer, "address");
        assert_eq!(result.label, "10 King Street, Toronto");
        assert_eq!(
            result.point,
            Some(SearchApiPoint {
                lon: -79.3832,
                lat: 43.6532,
                precision: SearchApiPointPrecision::Point,
            })
        );
        assert_eq!(result.source.dataset, "osm");
        assert_eq!(result.source.object_type, Some(OsmObjectType::Node));
    }

    #[test]
    fn api_result_uses_representative_point_for_street() {
        let hit = TextSearchHit {
            record_id: 12,
            score: 3.0,
            record: RecordSummary {
                id: "osm:way:9".to_string(),
                layer: "street".to_string(),
                label: "King Street".to_string(),
                point: Some(RecordPoint {
                    lon: -79.41,
                    lat: 43.61,
                    precision: RecordPointPrecision::RepresentativePoint,
                }),
                source: RecordSource {
                    dataset: "osm".to_string(),
                    object_type: Some(OsmObjectType::Way),
                    object_id: Some(9),
                    derived_from: None,
                    record_count: None,
                },
            },
        };

        let result = SearchApiResult::from_hit(hit);

        assert_eq!(
            result.point,
            Some(SearchApiPoint {
                lon: -79.41,
                lat: 43.61,
                precision: SearchApiPointPrecision::RepresentativePoint,
            })
        );
    }
}

#[cfg(test)]
mod router_tests {
    use std::collections::BTreeMap;
    use std::path::Path;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt;

    use super::*;
    use crate::pack::PackWriter;
    use crate::record::{
        AddressComponents, AddressRecord, LocationPrecision, OsmObjectType, SourceProvenance,
        point_geometry,
    };

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("open-geocode-http-{name}-{}", std::process::id()))
    }

    fn write_pack(dir: &Path) {
        let _ = std::fs::remove_dir_all(dir);
        let mut writer = PackWriter::create(dir).expect("writer");
        writer
            .write(
                &AddressRecord {
                    address: AddressComponents {
                        number: "10".to_string(),
                        street: Some("King Street".to_string()),
                        place: None,
                        unit: None,
                        locality: Some("Toronto".to_string()),
                        region: None,
                        postcode: Some("M5V 1A1".to_string()),
                        country: None,
                    },
                    geometry: point_geometry(-79.0, 43.0),
                    location_precision: LocationPrecision::Point,
                    source: SourceProvenance {
                        dataset: "osm".to_string(),
                        object_type: OsmObjectType::Node,
                        object_id: 1,
                        tags: Some(BTreeMap::new()),
                    },
                }
                .into(),
                None,
            )
            .expect("write address");
        writer.finish().expect("finish");
    }

    fn state_for(pack_dir: &Path, ready: bool) -> AppState {
        let state = AppState::open(pack_dir).expect("open shared pack");
        state.ready.store(ready, Ordering::Release);
        state
    }

    fn request(method: &str, uri: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap()
    }

    struct Reply {
        status: StatusCode,
        headers: axum::http::HeaderMap,
        body: Vec<u8>,
    }

    impl Reply {
        fn content_type(&self) -> &str {
            self.headers
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
        }

        fn json(&self) -> serde_json::Value {
            serde_json::from_slice(&self.body).expect("response body is JSON")
        }

        fn request_id(&self) -> Option<String> {
            self.headers
                .get("x-request-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        }
    }

    async fn send(router: Router, request: Request<Body>) -> Reply {
        let response = router.oneshot(request).await.expect("router response");
        let status = response.status();
        let headers = response.headers().clone();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body")
            .to_vec();
        Reply {
            status,
            headers,
            body,
        }
    }

    #[tokio::test]
    async fn health_endpoints_reflect_readiness() {
        let pack = temp_path("health");
        write_pack(&pack);
        let demo = temp_path("health-demo");
        let no_basemap = temp_path("health-no-basemap");

        let router = build_router(state_for(&pack, true), &demo, &no_basemap);
        assert_eq!(
            send(router.clone(), request("GET", "/healthz"))
                .await
                .status,
            StatusCode::OK
        );
        assert_eq!(
            send(router, request("GET", "/readyz")).await.status,
            StatusCode::OK
        );

        let not_ready = build_router(state_for(&pack, false), &demo, &no_basemap);
        assert_eq!(
            send(not_ready, request("GET", "/readyz")).await.status,
            StatusCode::SERVICE_UNAVAILABLE
        );

        let _ = std::fs::remove_dir_all(&pack);
    }

    #[tokio::test]
    async fn search_enforces_query_and_limit_bounds() {
        let pack = temp_path("search-bounds");
        write_pack(&pack);
        let demo = temp_path("search-demo");
        let no_basemap = temp_path("search-no-basemap");
        let router = build_router(state_for(&pack, true), &demo, &no_basemap);

        // Empty query -> 400 invalid_query, Problem Details.
        let reply = send(router.clone(), request("GET", "/search?q=")).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST);
        assert_eq!(reply.content_type(), "application/problem+json");
        assert_eq!(reply.json()["error_code"], "invalid_query");

        // Over-length query -> 400.
        let long = "a".repeat(300);
        let reply = send(router.clone(), request("GET", &format!("/search?q={long}"))).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST);

        // limit above the cap -> 400 with invalid_params naming "limit".
        let reply = send(
            router.clone(),
            request("GET", "/search?q=king&limit=999999"),
        )
        .await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST);
        assert_eq!(reply.json()["invalid_params"][0]["name"], "limit");

        // Malformed limit type -> 400 malformed_parameter.
        let reply = send(router.clone(), request("GET", "/search?q=king&limit=abc")).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST);
        assert_eq!(reply.json()["error_code"], "malformed_parameter");

        // A valid query succeeds.
        let reply = send(router, request("GET", "/search?q=King%20Street&limit=5")).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert_eq!(reply.content_type(), "application/json");

        let _ = std::fs::remove_dir_all(&pack);
    }

    #[tokio::test]
    async fn geocode_keeps_hits_in_the_given_postcode() {
        let pack = temp_path("geocode");
        write_pack(&pack);
        let demo = temp_path("geocode-demo");
        let no_basemap = temp_path("geocode-no-basemap");
        let router = build_router(state_for(&pack, true), &demo, &no_basemap);

        let reply = send(
            router.clone(),
            request(
                "GET",
                "/geocode?address=10%20King%20Street&postcode=M5V%201A1",
            ),
        )
        .await;
        assert_eq!(reply.status, StatusCode::OK);
        let body = reply.json();
        assert_eq!(body["result"]["layer"], "address");
        assert_eq!(body["result"]["point"]["lon"], -79.0);

        let reply = send(
            router.clone(),
            request(
                "GET",
                "/geocode?address=10%20King%20Street&postcode=V6B%201A1",
            ),
        )
        .await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(reply.json()["result"].is_null());

        let reply = send(router, request("GET", "/geocode?locality=Toronto")).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST);

        let _ = std::fs::remove_dir_all(&pack);
    }

    fn post(uri: &str, body: impl Into<Body>) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.into())
            .unwrap()
    }

    fn batch_router(name: &str) -> (Router, PathBuf) {
        let pack = temp_path(name);
        write_pack(&pack);
        let demo = temp_path(&format!("{name}-demo"));
        let no_basemap = temp_path(&format!("{name}-no-basemap"));
        (
            build_router(state_for(&pack, true), &demo, &no_basemap),
            pack,
        )
    }

    #[tokio::test]
    async fn geocode_batch_answers_each_item_in_order_and_isolates_bad_ones() {
        let (router, pack) = batch_router("batch");
        let items = serde_json::json!({ "items": [
            { "address": "10 King Street", "postcode": "M5V 1A1" },
            { "address": "10 King Street", "postcode": "V6B 1A1" },
            { "locality": "Toronto" },
            { "address": "10 King Street", "limit": 999999 },
            { "address": "10 King Street", "limit": "many" },
            "10 King Street",
            { "address": "x".repeat(300) },
            { "address": "  10 King Street  " },
        ]});
        let mut batch = post("/geocode/batch", items.to_string());
        batch
            .headers_mut()
            .insert("cf-ray", header::HeaderValue::from_static("ray-batch-1"));
        let reply = send(router.clone(), batch).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert_eq!(reply.content_type(), "application/json");
        assert_eq!(reply.request_id().as_deref(), Some("ray-batch-1"));
        let body = reply.json();
        let results = body["results"].as_array().expect("results");
        assert_eq!(results.len(), 8);

        assert_eq!(results[0]["result"]["layer"], "address");
        assert_eq!(results[0]["result"]["point"]["lon"], -79.0);
        // Not in that postcode: a null result keeps the address as its query.
        assert!(results[1]["result"].is_null());
        assert_eq!(results[1]["query"], "10 King Street");
        // Each of these is the Problem the same parameters get from GET /geocode.
        let problem = |i: usize, code: &str| {
            let problem = &results[i]["problem"];
            assert_eq!(problem["error_code"], code, "item {i}");
            assert_eq!(problem["status"], 400, "item {i}");
            assert_eq!(problem["request_id"], "ray-batch-1", "item {i}");
            assert!(results[i].get("result").is_none(), "item {i}");
        };
        problem(2, "invalid_query");
        problem(3, "limit_too_large");
        assert_eq!(results[3]["problem"]["invalid_params"][0]["name"], "limit");
        problem(4, "malformed_parameter");
        problem(5, "malformed_parameter");
        problem(6, "invalid_query");
        // The address is trimmed, as on GET.
        assert_eq!(results[7]["result"]["layer"], "address");

        // The batch of one is the GET answer.
        let get = send(
            router.clone(),
            request(
                "GET",
                "/geocode?address=10%20King%20Street&postcode=M5V%201A1",
            ),
        )
        .await;
        assert_eq!(results[0], get.json());

        let _ = std::fs::remove_dir_all(&pack);
    }

    #[tokio::test]
    async fn geocode_batch_keeps_the_order_across_chunks() {
        let (router, pack) = batch_router("batch-chunks");
        // Three kinds of item in turn, over several chunks: found, not found (its query names its
        // position) and invalid.
        let count = BATCH_CHUNK * 3 + 5;
        let items: Vec<_> = (0..count)
            .map(|i| match i % 3 {
                0 => serde_json::json!({ "address": "10 King Street" }),
                1 => serde_json::json!({ "address": format!("99 Nowhere Road {i}") }),
                _ => serde_json::json!({ "address": "10 King Street", "limit": 999999 }),
            })
            .collect();
        let reply = send(
            router,
            post(
                "/geocode/batch",
                serde_json::json!({ "items": items }).to_string(),
            ),
        )
        .await;
        assert_eq!(reply.status, StatusCode::OK);
        let body = reply.json();
        let results = body["results"].as_array().expect("results");
        assert_eq!(results.len(), count);
        for (i, result) in results.iter().enumerate() {
            match i % 3 {
                0 => assert_eq!(result["result"]["layer"], "address", "item {i}"),
                1 => {
                    assert!(result["result"].is_null(), "item {i}");
                    assert_eq!(result["query"], format!("99 Nowhere Road {i}"));
                }
                _ => assert_eq!(
                    result["problem"]["error_code"], "limit_too_large",
                    "item {i}"
                ),
            }
        }

        let _ = std::fs::remove_dir_all(&pack);
    }

    #[tokio::test]
    async fn geocode_batch_answers_overloaded_for_the_chunks_it_could_not_run() {
        let pack = temp_path("batch-partial-shed");
        write_pack(&pack);
        let demo = temp_path("batch-partial-shed-demo");
        let no_basemap = temp_path("batch-partial-shed-no-basemap");
        // One slot and no patience: the first chunk takes it, and a later one may find it taken.
        // Whether it does depends on timing, so only what must hold either way is checked: the
        // answer is complete and in order, the first chunk ran, and once a chunk was shed so is
        // everything after it.
        let mut state = state_for(&pack, true);
        state.permits = Arc::new(Semaphore::new(1));
        state.queue_timeout = Duration::ZERO;
        let router = build_router(state, &demo, &no_basemap);

        let count = BATCH_CHUNK * 12;
        let items = vec![serde_json::json!({ "address": "10 King Street" }); count];
        let reply = send(
            router,
            post(
                "/geocode/batch",
                serde_json::json!({ "items": items }).to_string(),
            ),
        )
        .await;
        assert_eq!(reply.status, StatusCode::OK);
        let body = reply.json();
        let results = body["results"].as_array().expect("results");
        assert_eq!(results.len(), count);
        let shed: Vec<bool> = results
            .iter()
            .map(|result| result["problem"]["error_code"] == "overloaded")
            .collect();
        assert!(
            results[..BATCH_CHUNK]
                .iter()
                .all(|r| r["result"]["layer"] == "address")
        );
        assert!(shed.windows(2).all(|pair| !pair[0] || pair[1]));
        assert!(
            results
                .iter()
                .zip(&shed)
                .all(|(result, shed)| *shed || result["result"]["layer"] == "address")
        );

        let _ = std::fs::remove_dir_all(&pack);
    }

    #[tokio::test]
    async fn geocode_batch_rejects_a_body_that_is_not_a_batch() {
        let (router, pack) = batch_router("batch-bounds");

        for body in [
            "",
            "not json",
            "[]",
            "{}",
            r#"{"items": "x"}"#,
            r#"{"items": [1}"#,
        ] {
            let reply = send(router.clone(), post("/geocode/batch", body)).await;
            assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{body:?}");
            assert_eq!(reply.content_type(), "application/problem+json");
            assert_eq!(reply.json()["error_code"], "malformed_parameter");
            assert!(reply.json()["request_id"].is_string());
        }

        // An empty batch is a valid batch with no results.
        let reply = send(router.clone(), post("/geocode/batch", r#"{"items": []}"#)).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert_eq!(reply.json(), serde_json::json!({ "results": [] }));

        // The cap is on the item count: one over is a 413 Problem, the cap itself is answered.
        let items = |count: usize| {
            serde_json::json!({ "items": vec![serde_json::json!({ "address": "10 King Street" }); count] })
                .to_string()
        };
        let reply = send(
            router.clone(),
            post("/geocode/batch", items(bounds::MAX_BATCH_ITEMS + 1)),
        )
        .await;
        assert_eq!(reply.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(reply.content_type(), "application/problem+json");
        assert_eq!(reply.json()["error_code"], "batch_too_large");
        let reply = send(
            router.clone(),
            post("/geocode/batch", items(bounds::MAX_BATCH_ITEMS)),
        )
        .await;
        assert_eq!(reply.status, StatusCode::OK);
        assert_eq!(
            reply.json()["results"].as_array().map(Vec::len),
            Some(bounds::MAX_BATCH_ITEMS)
        );

        // So is the body size, whatever the item count.
        let padding = " ".repeat(bounds::MAX_BATCH_BODY_BYTES);
        let reply = send(
            router,
            post("/geocode/batch", format!(r#"{{"items": []{padding}}}"#)),
        )
        .await;
        assert_eq!(reply.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(reply.content_type(), "application/problem+json");
        assert_eq!(reply.json()["error_code"], "batch_too_large");

        let _ = std::fs::remove_dir_all(&pack);
    }

    #[tokio::test]
    async fn geocode_batch_is_post_only() {
        let (router, pack) = batch_router("batch-method");
        for method in ["GET", "HEAD", "PUT", "DELETE"] {
            let reply = send(router.clone(), request(method, "/geocode/batch")).await;
            assert_eq!(reply.status, StatusCode::METHOD_NOT_ALLOWED, "{method}");
            assert_eq!(reply.content_type(), "application/problem+json");
            assert_eq!(
                reply
                    .headers
                    .get(header::ALLOW)
                    .and_then(|v| v.to_str().ok()),
                Some("POST"),
                "{method}"
            );
        }
        // And /geocode stays GET only.
        let reply = send(router, post("/geocode", r#"{"items": []}"#)).await;
        assert_eq!(reply.status, StatusCode::METHOD_NOT_ALLOWED);

        let _ = std::fs::remove_dir_all(&pack);
    }

    #[tokio::test]
    async fn reverse_validates_coordinates() {
        let pack = temp_path("reverse-bounds");
        write_pack(&pack);
        let demo = temp_path("reverse-demo");
        let no_basemap = temp_path("reverse-no-basemap");
        let router = build_router(state_for(&pack, true), &demo, &no_basemap);

        let reply = send(router.clone(), request("GET", "/reverse?lon=999&lat=0")).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST);
        assert_eq!(reply.json()["invalid_params"][0]["name"], "lon");

        let reply = send(router.clone(), request("GET", "/reverse?lon=0&lat=999")).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST);
        assert_eq!(reply.json()["invalid_params"][0]["name"], "lat");

        let reply = send(router, request("GET", "/reverse?lon=-79.0&lat=43.0")).await;
        assert_eq!(reply.status, StatusCode::OK);

        let _ = std::fs::remove_dir_all(&pack);
    }

    #[tokio::test]
    async fn methods_unknown_paths_and_request_ids() {
        let pack = temp_path("methods");
        write_pack(&pack);
        let demo = temp_path("methods-demo");
        let no_basemap = temp_path("methods-no-basemap");
        let router = build_router(state_for(&pack, true), &demo, &no_basemap);

        // POST to a GET-only route -> 405 with Allow: GET.
        let reply = send(router.clone(), request("POST", "/search")).await;
        assert_eq!(reply.status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(reply.content_type(), "application/problem+json");
        assert_eq!(
            reply
                .headers
                .get(header::ALLOW)
                .and_then(|v| v.to_str().ok()),
            Some("GET")
        );

        // HEAD is not part of the API method surface (Decision 27) -> 405.
        let reply = send(router.clone(), request("HEAD", "/search")).await;
        assert_eq!(reply.status, StatusCode::METHOD_NOT_ALLOWED);

        // Unknown path -> 404 Problem Details, not a static-file response.
        let reply = send(router.clone(), request("GET", "/does-not-exist")).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
        assert_eq!(reply.content_type(), "application/problem+json");
        assert_eq!(reply.json()["error_code"], "not_found");

        // Every response carries a request id; an inbound cf-ray is honored.
        let reply = send(router.clone(), request("GET", "/healthz")).await;
        assert!(reply.request_id().is_some());

        let mut tagged = request("GET", "/search?q=");
        tagged
            .headers_mut()
            .insert("cf-ray", header::HeaderValue::from_static("ray-abc-123"));
        let reply = send(router, tagged).await;
        assert_eq!(reply.request_id().as_deref(), Some("ray-abc-123"));
        assert_eq!(reply.json()["request_id"], "ray-abc-123");

        let _ = std::fs::remove_dir_all(&pack);
    }

    #[tokio::test]
    async fn routing_filter_is_served_with_an_etag_and_only_by_get() {
        use crate::street_filter::{RoutingFilter, pair_hash, street_hash, town_hash};

        let pack = temp_path("routing-filter");
        write_pack(&pack);
        let demo = temp_path("routing-filter-demo");
        let no_basemap = temp_path("routing-filter-no-basemap");
        let router = build_router(state_for(&pack, true), &demo, &no_basemap);

        let reply = send(router.clone(), request("GET", "/routing-filter")).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert_eq!(reply.content_type(), "application/octet-stream");
        let etag = reply
            .headers
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok())
            .expect("etag")
            .to_string();
        // "king street" and king street|toronto are in this Pack's one address.
        let filter = RoutingFilter::parse(reply.body.as_slice()).expect("the body is the section");
        let street = street_hash("King Street").expect("street");
        assert!(filter.maybe_contains(street));
        assert!(filter.maybe_contains(pair_hash(street, town_hash("Toronto").expect("town"))));
        assert!(!filter.maybe_contains(street_hash("Queen Street").expect("street")));
        assert_eq!(filter.key_count(), 2);

        // The same section served again, revalidated by its ETag, is a 304 with no body.
        let mut conditional = request("GET", "/routing-filter");
        conditional
            .headers_mut()
            .insert(header::IF_NONE_MATCH, etag.parse().expect("header value"));
        let reply = send(router.clone(), conditional).await;
        assert_eq!(reply.status, StatusCode::NOT_MODIFIED);
        assert!(reply.body.is_empty());
        let mut stale = request("GET", "/routing-filter");
        stale.headers_mut().insert(
            header::IF_NONE_MATCH,
            "\"0000000000000000\"".parse().expect("header value"),
        );
        assert_eq!(send(router.clone(), stale).await.status, StatusCode::OK);

        // GET only, like the rest of the API.
        let reply = send(router.clone(), request("HEAD", "/routing-filter")).await;
        assert_eq!(reply.status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            reply
                .headers
                .get(header::ALLOW)
                .and_then(|v| v.to_str().ok()),
            Some("GET")
        );
        let reply = send(router, request("POST", "/routing-filter")).await;
        assert_eq!(reply.status, StatusCode::METHOD_NOT_ALLOWED);

        let _ = std::fs::remove_dir_all(&pack);
    }

    #[tokio::test]
    async fn api_sheds_load_with_retry_after_when_no_slot_frees_up() {
        let pack = temp_path("overloaded");
        write_pack(&pack);
        let demo = temp_path("overloaded-demo");
        let no_basemap = temp_path("overloaded-no-basemap");
        let mut state = state_for(&pack, true);
        state.permits = Arc::new(Semaphore::new(0));
        state.queue_timeout = Duration::from_millis(10);
        let router = build_router(state, &demo, &no_basemap);

        for uri in [
            "/search?q=king",
            "/autocomplete?q=kin",
            "/geocode?address=10%20King%20Street",
            "/reverse?lon=-79.0&lat=43.0",
        ] {
            let reply = send(router.clone(), request("GET", uri)).await;
            assert_eq!(reply.status, StatusCode::SERVICE_UNAVAILABLE, "{uri}");
            assert_eq!(reply.content_type(), "application/problem+json");
            assert_eq!(
                reply
                    .headers
                    .get(header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok()),
                Some("1")
            );
            assert_eq!(reply.json()["error_code"], "overloaded");
            assert!(reply.json()["request_id"].is_string());
        }

        // A batch that cannot get its first slot is shed whole, like a single request.
        let reply = send(
            router.clone(),
            post(
                "/geocode/batch",
                r#"{"items": [{"address": "10 King Street"}]}"#,
            ),
        )
        .await;
        assert_eq!(reply.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(reply.json()["error_code"], "overloaded");
        assert_eq!(
            reply
                .headers
                .get(header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );

        // Health probes never wait for a slot.
        let reply = send(router.clone(), request("GET", "/healthz")).await;
        assert_eq!(reply.status, StatusCode::OK);
        let reply = send(router, request("GET", "/readyz")).await;
        assert_eq!(reply.status, StatusCode::OK);

        let _ = std::fs::remove_dir_all(&pack);
    }

    /// Rewrite the Pack's file with its routing filter section dropped (`None`, a Pack built before
    /// the section existed) or filed under another `version` (one from a newer build).
    fn rewrite_routing_filter(dir: &Path, version: Option<u32>) {
        use crate::{
            container::{Container, ContainerWriter},
            pack::PACK_FILE,
            street_filter::ROUTING_FILTER_SECTION,
        };

        let id = std::fs::read_to_string(dir.join("CURRENT")).expect("pointer");
        let file = dir.join("generations").join(id.trim()).join(PACK_FILE);
        let old = Container::open(&file).expect("open");
        let rewritten = file.with_extension("rewritten");
        let mut writer = ContainerWriter::create(&rewritten).expect("create");
        for (name, info) in old.sections() {
            let bytes = old.raw_section(name).expect("section");
            if name == ROUTING_FILTER_SECTION {
                let Some(version) = version else { continue };
                writer.add(name, version, bytes.as_slice()).expect("add");
            } else {
                writer
                    .add(name, info.version, bytes.as_slice())
                    .expect("add");
            }
        }
        writer.finish().expect("finish");
        drop(old);
        std::fs::rename(&rewritten, &file).expect("replace");
    }

    #[tokio::test]
    async fn a_pack_without_a_usable_routing_filter_answers_a_404_problem_and_still_serves() {
        // An old Pack has no section; a newer build's Pack has one this build cannot read. Both
        // open, serve /search, and leave the router to treat them as "might hold anything".
        for (name, version) in [("old", None), ("newer", Some(2))] {
            let pack = temp_path(&format!("no-routing-filter-{name}"));
            write_pack(&pack);
            rewrite_routing_filter(&pack, version);
            let demo = temp_path(&format!("no-routing-filter-{name}-demo"));
            let no_basemap = temp_path(&format!("no-routing-filter-{name}-no-basemap"));
            let router = build_router(state_for(&pack, true), &demo, &no_basemap);

            let mut tagged = request("GET", "/routing-filter");
            tagged
                .headers_mut()
                .insert("cf-ray", header::HeaderValue::from_static("ray-filter-1"));
            let reply = send(router.clone(), tagged).await;
            assert_eq!(reply.status, StatusCode::NOT_FOUND, "{name}");
            assert_eq!(reply.content_type(), "application/problem+json");
            assert_eq!(reply.json()["error_code"], "not_found");
            assert_eq!(reply.json()["request_id"], "ray-filter-1");
            assert!(
                reply.json()["detail"]
                    .as_str()
                    .is_some_and(|detail| detail.contains("routing filter"))
            );

            let reply = send(router, request("GET", "/search?q=king+street")).await;
            assert_eq!(reply.status, StatusCode::OK, "{name}");
            assert!(
                !reply.json()["results"]
                    .as_array()
                    .expect("results")
                    .is_empty()
            );

            let _ = std::fs::remove_dir_all(&pack);
        }
    }

    #[tokio::test]
    async fn basemap_is_served_raw_with_range_support() {
        // The runtime serves the basemap raw: range requests work natively and
        // there is deliberately no full-file fetch guard here (that is a
        // deployment-layer concern, ADR 0017 Decision 35a). The bare binary hands
        // back exactly what was asked for.
        let pack = temp_path("basemap");
        write_pack(&pack);
        let demo = temp_path("basemap-demo");
        let basemap = temp_path("basemap.pmtiles");
        std::fs::write(&basemap, vec![0u8; 600_000]).expect("write basemap");

        let router = build_router(state_for(&pack, true), &demo, &basemap);

        // A bounded range is honored as a 206 with exactly the requested bytes.
        let mut req = request("GET", "/basemap.pmtiles");
        req.headers_mut().insert(
            header::RANGE,
            header::HeaderValue::from_static("bytes=0-511999"),
        );
        let reply = send(router.clone(), req).await;
        assert_eq!(reply.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(reply.body.len(), 512_000);

        // A Range-less GET returns the whole file (200) — raw, unguarded.
        let reply = send(router.clone(), request("GET", "/basemap.pmtiles")).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert_eq!(reply.body.len(), 600_000);

        // HEAD works too.
        let reply = send(router, request("HEAD", "/basemap.pmtiles")).await;
        assert_eq!(reply.status, StatusCode::OK);

        let _ = std::fs::remove_dir_all(&pack);
        let _ = std::fs::remove_file(&basemap);
    }
}
