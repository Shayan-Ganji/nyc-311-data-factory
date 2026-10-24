# NYC 311 Data Factory: Architectural Decision & Resilience Report
**Author:** Shayan Ganji  
**Project:** Stage 1 — Resilient Data Factory Pipeline  
**Repository:** [https://github.com/Shayan-Ganji/nyc-311-data-factory](https://github.com/Shayan-Ganji/nyc-311-data-factory)  
**Language Toggle:** [English Version](report.md) | [نسخه فارسی](report.fa.md)  

---

## 1. Problem Understanding and Boundary Assumptions
The objective of this challenge is not merely to fetch an arbitrary slice of urban data and render visualizations, but to design, implement, and validate a resilient, modular, and bitwise deterministic data ingestion pipeline (**Data Factory**) capable of handling over 300,000 real-world records from the New York City 311 service request ecosystem (SODA 2.0 API).

Real-world municipal telemetry is notorious for dirty records, unindexed schema transitions, network intermittency, and temporal anomalies. To build a robust foundation, several implicit boundaries were formalized:
1. **Temporal Scope and Domain Integrity:** Rather than artificially subsampling across fragmented municipal departments, we scoped the ingestion to the **NYC Department of Transportation (DOT)** across an 18-month continuous window from **January 1, 2023, to June 30, 2024**. This domain organically contains **302,457 records**, surpassing the 300,000 threshold without artificial subsampling.
2. **The Non-Negotiable Contract of Determinism:** Re-running the pipeline across the same input domain must produce bitwise identical output artifacts verified via cryptographic SHA-256 digests (`run_manifest.json`).
3. **Decoupled Architecture for Future Extensibility:** As mandated by the Stage 2 syllabus, the ingestion framework must serve as an extensible engine capable of loading downstream database targets (PostgreSQL) without refactoring extraction or validation logic.

---

## 2. Five Architectural Decisions and Empirical Trade-Offs

### Decision 1: Scope Selection — DOT 18-Month Continuous Horizon
- **Options Considered:** 
  - (a) Multi-agency cross-sectional sampling (e.g., combining NYPD noise and DOHMH rodent complaints).
  - (b) High-volume single agency (e.g., NYPD or HPD exceeding 2M records) filtered arbitrarily by geographic sub-boroughs.
  - (c) Continuous 18-month window of the Department of Transportation (DOT).
- **Option Selected:** Option (c) — DOT from `2023-01-01T00:00:00.000` to `2024-06-30T23:59:59.999`.
- **Empirical Evidence:** Probing the SODA API revealed that DOT generated exactly **302,457 records** during this period. This naturally crossed the $\ge 300,000$ requirement while preserving genuine operational characteristics (street conditions, traffic signals, sidewalks, and street lights).
- **Trade-Off / What Was Sacrificed:** We sacrificed cross-agency comparison across distinct city departments in exchange for complete, unfragmented structural coverage of NYC's physical transportation grid.

### Decision 2: Pagination Mechanics — Keyset (Cursor) vs. Deep Offset Scanning
- **Options Considered:**
  - (a) Standard HTTP offset pagination using `$limit=50000` and incremental `$offset=50000, 100000...`
  - (b) Keyset (cursor) pagination using composite tie-breaking: `$where=... AND (created_date > '{last_created}' OR (created_date = '{last_created}' AND unique_key > '{last_key}'))` ordered by `created_date ASC, unique_key ASC`.
- **Option Selected:** Option (b) — Keyset Cursor Pagination.
- **Empirical Evidence:** As documented in our failed trials, Socrata's underlying database experiences quadratic scanning degradation ($\mathcal{O}(\text{offset} + \text{limit})$) when offsets exceed 100,000, resulting in fatal `504 Gateway Timeout` errors. With keyset pagination, all **7 batches** (totaling 302,457 records) ingested seamlessly in **81.54 seconds** (averaging ~11 seconds per 50k batch) with zero timeouts.
- **Trade-Off / What Was Sacrificed:** Random access capability was lost; data must be consumed in strict, deterministic forward sequence.

### Decision 3: Storage Decoupling via Adapter Pattern (`BaseSink`)
- **Options Considered:**
  - (a) Monolithic file serialization directly within the pipeline ingestion loop (`df.to_parquet()`).
  - (b) Decoupled Adapter Pattern defining an abstract `BaseSink` with pluggable destination drivers (`ParquetSink`, `CsvSink`, `JsonlSink`).
- **Option Selected:** Option (b) — Adapter Pattern.
- **Empirical Evidence:** The Stage 2 specification explicitly requires: *"Data loading into PostgreSQL must be executed through the same package from Stage 1 by adding a new Database sink; disposable loading scripts are unacceptable."* Implementing `BaseSink` guarantees zero-refactor database connectivity in Stage 2.
- **Trade-Off / What Was Sacrificed:** Increased initial architectural boilerplate and interface abstraction overhead in exchange for long-term open/closed extensibility.

### Decision 4: Quarantine Governance — Dead-Letter Queue (DLQ) vs. Silent Dropping or Imputation
- **Options Considered:**
  - (a) Silent record discarding via `.dropna()` or boolean filtering.
  - (b) Synthetic imputation (e.g., swapping inverted dates or setting negative resolution times to zero).
  - (c) Segregation into a Dead-Letter Queue (`quarantine.jsonl`) accompanied by machine-readable error codes.
- **Option Selected:** Option (c) — Strict Quarantine Router.
- **Empirical Evidence:** Ingestion revealed **4,558 defective records (1.507% quarantine rate)**, safely below our 10% budget. Exactly **4,455 records** contained temporal paradoxes (`closed_date < created_date` tagged as `ERR_CLOSED_BEFORE_CREATED`), and **103 records** were marked 'Closed' with null timestamps (`ERR_CLOSED_WITHOUT_CLOSED_DATE`). Imputing them would fabricate municipal records; silently dropping them would obscure system bugs.
- **Trade-Off / What Was Sacrificed:** 1.507% of records were excluded from analytical duration metrics, and 6.46 MB of disk storage was dedicated to maintaining the quarantine log.

### Decision 5: Spatial Denominator Normalization — MODZCTA Integration vs. Static Constants
- **Options Considered:**
  - (a) Static borough populations hardcoded from Wikipedia/Census tables.
  - (b) Census Tract spatial-joins requiring external GIS polygon shapefiles.
  - (c) Dynamic integration of NYC Department of Health Modified ZIP Code Tabulation Areas (**MODZCTA**).
- **Option Selected:** Option (c) — MODZCTA Reference Ingestion (`pri4-ifjk.json`, 178 zones).
- **Empirical Evidence:** Postal ZIP codes alone create division-by-zero artifacts (e.g., commercial zones like Empire State Building 10118 have zero residents). MODZCTA consolidates these into 178 residential zones with 8,443,636 residents. This allowed us to recover hundreds of `Unspecified` borough records using valid incident ZIPs and compute exact rates per 10k residents.
- **Trade-Off / What Was Sacrificed:** Added a secondary external file dependency that must be maintained and verified within the repository.

---

## 3. Failed Paths and Engineering Post-Mortems
A credible engineering report must document failures and dead ends:
1. **The SODA Unindexed Gateway Crash:** In our initial exploratory phase, we executed an SoQL query targeting inverted dates across the entire 23-million record database: `GET ?$where=closed_date < created_date`. The unindexed full-table comparison triggered a `504 Gateway Timeout` after 30 seconds. We abandoned global scans and restructured the architecture to scope queries within indexed agency and date boundaries, performing row-level validation locally inside `QuarantineRouter`.
2. **PowerShell CLI Variable Expansion Trap:** Initial CLI invocation attempts inside Windows PowerShell using string parameter blocks (`params={'$limit': 5}`) failed with `400 Bad Request: Unrecognized arguments []`. PowerShell interpreted `$limit` as an unset shell variable and evaluated it to empty string. We rectified this by decoupling request parameter serialization entirely into Python's `requests` dictionary mapping.
3. **Pandas Timestamp Timezone Stripping:** During monthly aggregation, calling `.dt.to_period("M")` emitted repeated user warnings regarding timezone dropping from aware UTC timestamps. We replaced this with ISO-compliant `.dt.strftime("%Y-%m")`, preserving determinism without mutating underlying datetimes.

---

## 4. Limitations and Production Risks
1. **Unauthenticated SODA Throttling:** The pipeline utilizes SODA 2.0 public endpoints. Without an official `X-App-Token`, Socrata imposes stricter rate limits. While our exponential backoff with full jitter successfully handled transient states, high-frequency pipeline polling in production risks IP-level rate-limiting.
2. **Schema Drift Vulnerability:** If NYC DoITT modifies upstream column names (e.g., renaming `unique_key` to `incident_id`), our schema validator halts execution immediately with `SchemaDriftError` (Exit Code 3).
3. **Survivorship Bias in Resolution Times:** Approximately 3,760 records remain `Pending` or `Open` with null `closed_date`. Calculating resolution metrics exclusively on closed cases introduces survivorship bias, underestimating duration for complex municipal works.

---

## 5. What Would We Do with Two More Days?
If granted two additional development days, we would:
1. Implement asynchronous non-blocking page extraction using `aiohttp` and `asyncio.Queue` to fetch batches concurrently while maintaining keyset determinism, cutting ingestion time from 81s to under 25s.
2. Integrate native GeoPandas / Shapely vector polygons to perform spatial point-in-polygon assignment for coordinates, enabling direct mapping to NYC Community District boards and City Council districts.

---

## 6. AI Usage Transparency
In compliance with Section 7 of `00_Delivery_Guide.pdf`, artificial intelligence tooling (Claude / Antigravity IDE) was utilized collaboratively as an interactive pair-programmer. AI assisted in scaffolding boilerplate typing, drafting unit test stubs, and debugging PowerShell escaping quirks. All architectural decisions (Keyset pagination, MODZCTA selection, Quarantine DLQ rules, and Adapter sink design) were conceptualized, evaluated, mathematically validated, and implemented directly by the author. Every line of code and statistical result has been independently verified and defended.
