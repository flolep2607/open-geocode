# open-geocode

Fast, lightweight, self-hosted geocoding in pure Rust.

`open-geocode` is a minimal Rust-native geocoding engine: address search,
Tantivy-backed autocomplete, and reverse geocoding from coordinates to
address-first location context. It turns OpenStreetMap PBF extracts into a
single compact Pack file holding a memory-mapped record store, a Tantivy text
index, and an H3-backed spatial index, with no database or search cluster to run.

## Why open-geocode?

Address data does not change minute to minute. You build it from an OSM extract
and rebuild it when a newer one comes out, so it behaves like a lookup table
rather than a live document you keep editing. The standard open-source options,
Nominatim and Pelias, serve it from stateful systems that stay running, a
PostgreSQL/PostGIS database or an Elasticsearch cluster.

open-geocode skips that. It compiles the extract once into one Pack file and
serves it from a single small binary. No database, no Java, no cluster to
babysit. To update, you build a new Pack and swap it in. And because a Pack is
just a file, you can copy it, cache it, or ship it to the edge like any other
static asset.

open-geocode is stateless and read-only: it memory-maps the Pack for
zero-overhead lookups, so it can run on scale-to-zero platforms like Cloud Run or
Fly Machines and idle at near-zero cost between bursts. Always-on PostgreSQL or
Elasticsearch can't scale to zero, so for spiky or low-traffic workloads, og can
be far cheaper to run.

## Quickstart

Building the Ontario pack (~940 MB PBF) takes under a minute on a 24-core machine and produces a ~300 MB Pack.

1. Download an OSM extract, for example Ontario from [Geofabrik](https://download.geofabrik.de/north-america/canada/ontario.html), and save it as `data/ontario.pbf`.
2. Build the pack:
   ```
   cargo run --release -- build --input data/ontario.pbf --pack data/pack
   ```
3. Serve the API and demo UI on `http://127.0.0.1:8080`:
   ```
   cargo run --release -- serve --pack data/pack
   ```

`serve` and `route` log to stderr; set `RUST_LOG` to change the level (default `info`, for example `RUST_LOG=open_geocode=debug,info`).
On Ctrl-C or SIGTERM both stop accepting connections and exit once the requests in flight have finished; `serve` answers `/readyz` with 503 during that drain.
`serve --max-concurrency <N>` (default: the number of CPUs) caps the API requests processed at once; a request that waits more than 2 s for a slot gets 503 with `Retry-After: 1`.

## Building large extracts and the planet

The builder streams: OSM objects, node references, coordinates, and finished records flow through external sorts, so its large buffers stay within `--memory-budget-mb` whatever the input size.
A region fits in the budget and never touches disk; a country or the planet spills sorted runs to scratch files and merges them, through the same code.

```
cargo run --release -- build --input planet.osm.pbf --pack data/planet --memory-budget-mb 16384 --scratch-dir /mnt/fast-ssd/og-scratch
```

- `--memory-budget-mb` (default 1024, minimum 64) is one shared pool for the build's large buffers: sort buffers (counted by capacity), the string table of the current segment, merge read buffers, and the text indexing buffers.
  Each sorter gets a quarter of the pool and the text index a quarter, reserved only while records are written.
  A sort buffer that would overflow the pool spills to disk.
  The build report records the tracked peak (`scratch.peak_tracked_bytes`).
- The string table cannot spill, so it starts a new segment once it uses an eighth of the pool (or after 1,048,576 records); with a small budget, common strings such as country names are then stored a few more times.
- A few reservations are needed to make progress and are taken even when the pool is full, so the tracked peak can pass a very small budget:
  merge read buffers (64 KiB to 1 MiB per run file being merged, sized from the sorter's share) and the text index's minimum of 16 MiB.
- The process also uses memory outside the pool: the PBF blocks being decoded (a few hundred MB on a many-core machine), the admin boundary polygons, the postcode centroids, and the operating system's cache of mapped files.
- `--scratch-dir` puts temporary files on a different disk; it defaults to the Pack directory.
  Each sorted run is deleted as soon as it has been merged, and the whole scratch directory is removed when the build ends.
  Ontario writes about 1 GB of scratch in total for a 0.94 GB input.
- The routing filter (see [Routing packs by country](#routing-packs-by-country)) gathers one 64-bit hash per street key in a sorter that takes an eighth of a sorter's share, so it spills like the others, and is deduplicated and built one shard of at most about 4 million keys at a time.
  The shard's `Vec<u64>` (32 MB) is reserved from the pool; xorf's construction scratch for it (a few times that, about 150 MB transient, the same whatever the input size) and the table of place names it needs to add towns (16 bytes per place or boundary record that a record's context can point at, plus 16 for each district whose name loses administrative words, held until the Pack is sealed) are outside the pool, like the boundary polygons.
  A spilled key takes about 11 bytes of scratch (a varint of 9 to 10 bytes for a random 64-bit hash, plus a length prefix), and the sorter drops only repeats that its small cache sees, so scratch grows with the keys pushed, not the unique ones.
  Only a New Zealand extract has been built so far (333,356 keys pushed for 318,828 unique, a 369 KB section, nothing spilled), so the planet figures are estimates: about 120 to 180 million unique keys, a 140 to 210 MB section and about 1.8 GB of scratch for 170 million keys pushed.
  The build report records the measured keys pushed (`scratch.sorters.routing_keys`), the unique keys (`output.routing_filter_keys`) and the section size, which is how to check the estimate on a large extract.
- The input must be sorted by type and id, which Geofabrik and planet.openstreetmap.org files are.
  Run `osmium sort` on anything else; the builder stops with an error on unsorted nodes.
- Admin boundary polygons and postcode centroids grow with the number of boundaries and postcodes, not with the number of addresses.
- `verify-pack --pack <file>` checks a copied or downloaded Pack against its per-section checksums; every build runs the same check before it publishes.

The build report (`audit/build-report.json`) records the time of each phase, how much each sorter spilled, and the size of every Pack section.

## Adding address points from CSV

`--addresses [dataset=]path.csv` (repeatable) adds OpenAddresses-format rows, such as G-NAF or LINZ extracts, to the build.
The header is `LON,LAT,NUMBER,STREET,UNIT,CITY,DISTRICT,REGION,POSTCODE`, matched case-insensitively in any order; `LON`, `LAT`, `NUMBER` and `STREET` are required, and `DISTRICT`, `HASH` and `ID` are ignored.

```
cargo run --release -- build --input new-zealand.osm.pbf --addresses linz=oa_nz.csv --pack data/nz
```

- Rows are streamed in batches and go through the same sorters, boundary context, postcode centroids, text index and reverse geocoding as OSM addresses, so the memory budget holds whatever the file size.
- Imported rows matching an OSM address with the same street and number within 150 m are skipped; imported rows are not deduplicated against each other.
- The build report records input rows in `scanned.address_rows` and OSM matches skipped in `scanned.duplicate_address_rows`.
- A record's id is `dataset:row` (`linz:42`): the dataset name, or the file stem without one, and the row's 1-based position among the data rows. Its source reports object type `row`.
- Rows without a number or street, or with coordinates that are missing, not numeric or out of range, are skipped and counted in the build report (`scanned.address_rows`, `rejected.by_reason`). A record with a line break inside a field, as a stray quote makes it, is rejected as `malformed_row`, and the rows it swallows are lost with it; files should be well-formed CSV.
- Packs without imported rows are unchanged and older Packs still open. A Pack with imported rows stores its record sections as version 5, so an older binary refuses it when it opens it.

Official address sources have their own attribution terms: Luxembourg BD-Adresses is CC0. For swisstopo data, use the attribution `Source: Federal Office of Topography swisstopo` and see <https://www.swisstopo.admin.ch/ogd-conditions>.

## Rebuilding packs safely

Rebuilding the same `--pack` path writes a separate version under `generations/` and updates `CURRENT` only after the new version finishes and passes validation.
A failed build removes its partial output and leaves the previous version available.
Existing servers keep using the version they opened; restart the server to use the replacement.
Published versions are retained, so repeated builds use additional disk space.

A Pack is one file, `generations/<id>/pack.ogp`.
Every command that takes `--pack` accepts either the Pack directory or that file, so you can copy, cache, or upload the file on its own.
The build audit (`audit/rejections.jsonl` and `audit/build-report.json`) sits next to the file but is not part of the Pack you serve.

## Routing packs by country

`open-geocode route` puts one URL in front of one Runtime per country Pack (`--worker NZ=http://127.0.0.1:8081`, repeated).
Requests with `country=XX` go to that country's Runtime; `/reverse` always needs `country`.
Requests without one are routed by street, not by comparing the raw scores of different Packs.

**The `routing/filter` section.**
The builder writes it into every Pack: a binary fuse filter over hashes of `street` and `street|town` for every address, interpolation and street record.
Towns are the record's own locality and the district, locality, neighbourhood and place boundaries around it.
A district is also a town without its generic administrative words (District, City, Council, Shire, Region, Regional, Rural, Municipality, and a leading "City of", "Shire of", "Town of", "Regional Municipality of" and the like): an invoice writes "Hastings" for New Zealand's "Hastings District" and "Sydney" for Australia's "City of Sydney".
This adds about 20% keys to a New Zealand build.
Every street in a district then confirms that town, so a very common street with a district's name ("Main Road, Marlborough") confirms New Zealand if the Pack of the intended country does not hold that street and town; how often that costs a right answer has not been measured on a gold set against Packs of several countries.
Keys use the same normalization as the text index, so a query's street hashes like a stored one.
It costs about 1.2 bytes per key and has about 0.4% false positives and no false negatives.
A Pack with no street at all gets an empty filter, which says it holds nothing; a Pack without the section was built before it existed, and the router treats it as holding anything.
The header carries a key scheme number, bumped whenever the normalization, the abbreviation rules or the hashes change, and a reader refuses a filter of another scheme, so a router built with other rules treats that Runtime as holding anything instead of misrouting.
Scheme 2 is the one with accent folding and the French and German abbreviations: a router of this build ignores the filter an older Runtime serves from a scheme 1 Pack (that Runtime counts as holding anything) until the Runtime is upgraded and its Pack rebuilt.
Scheme 3 adds the districts' names without their administrative words: a router of this build ignores a scheme 2 Pack's filter until that Pack is rebuilt, and an older router ignores a scheme 3 Pack's, so no mix of builds lets a new Pack be the only one to confirm a town.
It is a section of its own version: Packs without it still open, and older binaries ignore it (the container lists unknown sections and checksums them like the others).

**`GET /routing-filter`.**
The Runtime serves the section's bytes as `application/octet-stream`, straight from the mapped file, with the section checksum as `ETag` (`If-None-Match` gets a 304).
The format, version and hash seed are in the section's header.
A Pack without a filter answers a 404 Problem.
GET only, like the rest of the API.
The endpoint is for the router, on the internal network: the body is megabytes (an estimated 115 to 170 MB for a planet Pack) and is not rate limited, so a public deployment should deny `/routing-filter` at its proxy.

**Router behaviour.**
For `/geocode` the street is `address` without its house number and unit and the town is `locality`; for `/search` and `/autocomplete` the street comes from `q` and `locality` is an optional hint that the Runtimes ignore.
Text before the number is read two ways (a unit like "Level 3", or part of a name like "Highway 6 North") and both are tried.
Only a leading house number is read: an address written with the number after the street ("Rue de la Paix 12") or with a bis/ter suffix ("12 ter Rue de la Paix") finds no holder and goes to the fan-out.

1. If exactly one country's filter holds the street with the town, the request goes there.
2. Else if exactly one holds the street, it goes there. This step is for `/geocode`, and for `/search` with a house number or a `locality`: a name with no number may be a place, and `/autocomplete` text may be half a street, so `/autocomplete` routes only with a `locality`.
3. Else every Runtime is asked, and `/search` and `/autocomplete` merge: labels that cover more of the query first (for autocomplete the last word counts as a prefix), then each Runtime's own ranking, interleaved, since BM25 scores of different Packs do not compare.
   `/geocode` answers the one non-null result; when several Runtimes answer, the score of one Pack against another's is not evidence (a street held by several countries makes each of them answer, and an address alone gives them no locality, region or postcode to check), so unless exactly one answering Runtime has a filter that holds the street, the router answers 400 `country_required`.

**`prefer=XX`.**
`/search`, `/autocomplete` and `/geocode` take an optional `prefer` for when `country` is absent (`country` wins over it).
It is a soft hint, for example the country of an invoice's currency, and a currency can be wrong: an NZ supplier may bill in AUD, a Canadian one in USD.
So it ranks below what the address itself says, and above everything that is only a guess.
A `prefer` that is not a configured `--worker` (`EU`, a typo, a currency code such as `AUD`) is ignored, case aside, and routing is exactly as without it.
With one that is:

1. If exactly one country's filter holds the street with the town, the request goes there, even when it is not `prefer` (`x-route: filter-street-town`).
   This needs a `locality`.
2. Else if `prefer`'s filter holds the street, or `prefer` has no filter (it might hold anything), the request goes to `prefer` (`x-route: prefer`).
3. Else if exactly one country's filter holds the street, it goes there (`x-route: filter-street`), under the same rule as above: not for a `/search` with neither a house number nor a `locality`, and not for `/autocomplete` without a `locality`.
4. Else it goes to `prefer` anyway (`x-route: prefer-fallback`), including a request with no street at all, which would otherwise be asked of everyone.

`x-route-country` names the country in every case.
A routed Runtime that fails or finds nothing is followed by the fan-out, `prefer` included.
A 4xx or a load-shedding 503 from the `prefer` Runtime goes back as it is, with no fan-out, as for any routed Runtime.
Two costs of a wrong `prefer` are accepted: rungs 2 and 4 do not look at `street_alone`, so a name or a street typed so far with no `locality` goes to `prefer` and its answer, if not empty, replaces the fan-out merge; and a filter false positive (about 1 in 256 per street reading) on `prefer` takes rung 2 over the one Runtime that really holds the street.
Neither has been measured against wrong-currency swaps on the gold set.
`prefer` is also forwarded to the Runtimes in the query string, which they ignore.

A Runtime without a readable filter counts as holding anything, so it never lets another win alone, but it never wins alone either: a request is routed only when a filter confirms the country.
A routed Runtime that fails, or answers with nothing, is followed by the fan-out, because a filter can be wrong (a false positive, a town spelled another way).
A Runtime that sheds load (a 503 with `Retry-After`) is the exception: its answer goes back as it is, with the header, and nobody is asked again; when every Runtime sheds in a fan-out, the first one's 503 goes back.
The route taken and the country are logged at debug level as `routed`.
The street comes from the whole text, so a one-string address that includes the town or postcode ("15 Eruera Street, Rotorua 3010") finds no holder and is asked of everyone; send the town as `locality` to route it.
Responses carry `x-route` (`filter-street-town`, `filter-street`, `prefer`, `prefer-fallback`, `fanout`, or `country` when `country=XX` was given) and, when one Runtime answered, `x-route-country`; routed and merged results carry a `country`.
The router's request id goes to every Runtime it calls as `x-request-id`, so one request has one id in both logs.

The router fetches every filter at startup, then every `--filter-refresh-secs` (default 300, `0` turns the timer off), and again as soon as `/readyz` sees a Runtime that was not ready turn ready, so a swapped Pack is picked up without restarting the router.
The first fetch runs in the background, so the router listens at once, and a Runtime that has no filter yet (still starting, or an old Pack) is asked again every 15 seconds.
An unchanged filter costs a 304.

## Batch geocoding

`POST /geocode/batch` answers many `/geocode` lookups in one request, so a client geocoding millions of addresses pays for one HTTP request per page of addresses instead of one per address.
The body is JSON, and the `Runtime` and the router both serve it:

```
POST /geocode/batch
{"items": [
  {"address": "15 Eruera Street", "locality": "Rotorua", "postcode": "3010", "street_fallback": true},
  {"address": "1 King Street", "locality": "Sydney", "country": "AU"}
]}
```

An item takes the parameters of `GET /geocode` (`address`, `locality`, `region`, `postcode`, `limit`, `layer`, `street_fallback`), validates the same way and has the same defaults.
Through the router it also takes `country` and `prefer`, with the meaning they have in the query string.
The answer is `{"results": [...]}` with one entry per item, **in the order of the items**:

- `{"query": ..., "result": {...} | null}` is what `GET /geocode` would have answered.
- `{"problem": {...}}` is the Problem Details object (`status`, `error_code`, `detail`, `request_id`, `invalid_params`) `GET /geocode` would have answered, for an item that is not valid (an empty `address`, a `limit` above 50, a text over 256 characters, a field of the wrong type) or that fails.
  One bad item does not fail the others: the request itself fails only when its body is not a batch (`400 malformed_parameter`) or is too large (`413 batch_too_large`).
- Through the router an entry also has `route` (what `x-route` says for a GET: `filter-street-town`, `filter-street`, `prefer`, `prefer-fallback`, `fanout` or `country`), and a result has its `country`, also when the item named the country.

Limits: at most 1000 items and 4 MiB per request.
Both are 413 Problems above the limit; a client with more addresses sends several batches.
POST only: any other method is a 405 with `Allow: POST`, and `/geocode` stays GET only.
Every response has the `x-request-id` header, and an item's Problem carries the request id too.

**On the Runtime.**
The items run in parallel on the blocking pool, in chunks of 16, each chunk holding one of the `--max-concurrency` slots that a `/geocode` request holds.
A batch is therefore held to the same limit as the single routes, however large it is, and shares the slots with them in arrival order; one slot for the whole batch would let a request use every core and count as one.
A batch that cannot get its first slot within 2 s is shed whole (503 with `Retry-After: 1`).
One that got some slots and then none answers an `overloaded` Problem (`status` 503) for the items it did not run, and keeps the answers it has; the client retries those items.
Each batch is logged at info level as `geocode batch` with its `items`, `found`, `not_found` and `problems`.

**On the router.**
Each item is decided as `GET /geocode` decides it: its `country`, else the filters and `prefer`, else every Runtime.
The items for the same Runtime are sent as one sub-batch, all Runtimes at once, and an item that goes to every Runtime is in each one's.
The answers are put back in the order of the items and picked as for a GET: the one non-null answer, a Problem for an item that is the client's (a bad field, an unknown `country`, an address several countries answer: `country_required`), and a routed Runtime that finds nothing or fails is followed by the others (not asked a second time).
An item that named its `country` is asked of that Runtime only.
A Runtime that does not answer its sub-batch fails only its items, with an `upstream_unavailable` Problem (`overloaded` if it shed load); the items for the other Runtimes are answered.
The router's request id goes to every Runtime as `x-request-id`.

## Hosted demo

The public live demo and its Cloudflare Worker + Tunnel + VM deployment live in a
separate repo, `open-geocode-live`. None of it is needed to run open-geocode
locally.

## Benchmarks

Three geocoders on the same inputs and resources: **open-geocode**, **Nominatim**,
and **Pelias**, each serving the same Ontario OSM extract (~2.03M addresses) in
Docker, one at a time, capped at an identical 4 CPU / 10 GB, over loopback HTTP.
Host: 24-core / 32 GB, Windows 11 + Docker Desktop:

- **Service time**: closed-loop, single client: time for one request with **no
  queue**, so it can't be inflated by overload. This is the honest latency.
- **Throughput**: open-loop: max sustained arrival rate holding **p99 < 100 ms**.

| | open-geocode | Pelias | Nominatim |
| --- | ---: | ---: | ---: |
| Deployable footprint ³ | **1.28 GiB** | 5.88 GiB | 5.92 GiB |
| RAM while serving | **~1.2 GiB** ¹ | 4.60 GiB | 2.75 GiB |
| Forward service-time p50 / p99 | **1.0 / 2.0 ms** | 11.5 / 67.6 ms | 40.0 / 1384.7 ms |
| Reverse service-time p50 / p99 | **1.0 / 1.5 ms** | 5.0 / 9.5 ms | 7.5 / 14.2 ms |
| Forward throughput (req/s @ p99<100ms) | **1000** ² | 100 | 25 |
| Reverse throughput (req/s @ p99<100ms) | **1000** ² | 500 | 100 |
| Addresses indexed | 2,032,851 | 2,036,529 | 2,037,682 |

Single binary, no database or cluster: **~4.6× smaller on disk, lower memory use,
~10–40× higher forward throughput, sub-millisecond latency.**

### Notes

- ¹ open-geocode memory-maps its pack, so process RSS is only ~0.03 GiB but ~1.2 GiB figure is the working set: resident in the OS page cache
- ² open-geocode's true throughput ceiling is higher; the load grid stopped at 1000.
- ³ Measured with the previous multi-file Pack format, where the Ontario Pack was about 1.2 GB.
  The single-file Pack for the same extract is about 0.3 GB; the serving benchmarks have not been re-run with it yet.
- Engines do unequal work per query (Pelias libpostal parsing, Nominatim hierarchy); reported, not corrected.
- Nominatim's forward p99 is a genuine heavy tail under the cap (p50 is 40 ms), not overload.
- Single host: CPU capped, cache/disk shared; ratios travel better than absolute numbers.
- Only the ~2.03M addresses are directly comparable; each engine indexes different extra layers.

## Use Cases

| Capability | Example use case |
|---|---|
| Forward geocoding | Turn customer, store, vendor, or service addresses into coordinates |
| Reverse geocoding | Convert fleet, delivery, device, or field-work GPS pings into readable locations using H3 candidate lookup and address-first gates |
| Autocomplete | Power address forms, checkout flows, internal tools, and store locators with Tantivy-native prefix queries |
| Batch geocoding | Enrich CSVs, database tables, and large address lists without per-row API pricing |
| Search optimization | Handle messy addresses, abbreviations, partial queries, field-aware matches, interpolation ranges, and ranked candidates |
| Private data | Geocode internal addresses, custom places, service zones, or proprietary datasets |

## License

`open-geocode` is licensed under the [MIT License](LICENSE).

Third-party code dependencies remain under their own OSS licenses. Generated
Packs preserve source metadata needed for attribution and auditability; users are
responsible for following the license terms of the geospatial data they build
from.
