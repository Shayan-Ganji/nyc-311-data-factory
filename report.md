# Technical Architecture and Engineering Decision Report: NYC 311 Data Factory
**Student:** Shayan Ganji  
**Project:** Stage 1 — Resilient Data Factory Pipeline  
**Repository:** [https://github.com/Shayan-Ganji/nyc-311-data-factory](https://github.com/Shayan-Ganji/nyc-311-data-factory)  
**Language Toggle:** [English Version](report.md) | [نسخه فارسی](report.fa.md)  

---

## 1. Problem Formulation, Dimensions, and Boundary Assumptions
The objective of this project is to architect, implement, and validate an automated data factory pipeline capable of extracting, validating, and curating over 300,000 real-world records from the New York City 311 service request ecosystem (SODA 2.0 API). Decentralized field reporting by citizens and dispatch agencies introduces structural data defects: inverted timestamps, malformed postal codes, unexpected upstream schema shifts, and network latency spikes. The primary engineering goal is to guarantee bitwise deterministic execution, enforce robust data isolation without silent data loss, and decouple processing stages so the core components remain directly reusable in subsequent coursework phases.

The operational boundary assumptions established for this system include:
- Automated, batch-oriented ingestion with checkpointing capabilities to resume seamlessly after interruptions.
- Strict isolation of corrupted payloads into an auditable quarantine sink without dropping records via naive discarding.
- Complete abstraction of ingestion and validation logic from downstream storage media.

---

## 2. Architectural Decisions and Human Engineering Rationale

### 2.1. Domain Scoping: 18-Month Continuous Horizon for NYC DOT
To satisfy the 300,000-record threshold, two initial alternatives were evaluated: cross-agency sampling combining disparate entities (such as NYPD noise complaints and DOHMH rodent reports), or random cross-sectional slicing across the broader database. Both approaches fragmented structural continuity; the former lacked a coherent physical domain, while the latter broke temporal sequences. By auditing API distributions, the Department of Transportation (DOT) was selected across a continuous 18-month window (January 1, 2023, to June 30, 2024), yielding exactly **302,457 records** organically without artificial filtering. This preserved the complete physical lifecycle of urban transportation infrastructure (street potholes, roadway resurfacing, traffic signal outages, and sidewalk maintenance). The deliberate trade-off was abandoning multi-agency comparative benchmarking in favor of structural infrastructure depth.

### 2.2. Pagination Migration: Keyset Cursor Ingestion vs. Deep Offset Timeout
Initial ingestion attempts utilizing standard `$limit` and `$offset` parameters failed upon reaching deeper offsets. When offset values exceeded 100,000 records, database latency exceeded 30 seconds, terminating in an HTTP `504 Gateway Timeout`. In relational storage backends, deep offset scanning requires sequentially reading and discarding all preceding rows in memory with `O(offset + limit)` time complexity.  
To eliminate this bottleneck, **Keyset (Cursor) Pagination** was implemented. Queries were deterministically sorted by a compound primary key:
```sql
$where = agency = 'DOT' AND created_date >= '2023-01-01' 
         AND (created_date > '{last_date}' OR (created_date = '{last_date}' AND unique_key > '{last_key}'))
$order = created_date ASC, unique_key ASC
```
By indexing on `(created_date, unique_key)`, each request seeks directly to the index position following the last seen cursor, reducing complexity to `O(limit)`. This enabled streaming all 302,457 records across 7 consecutive batches in **81.54 seconds** without timeouts. The accepted trade-off was losing arbitrary random access to intermediate pages, restricting the pipeline to forward-progressing linear ingestion.

### 2.3. Network Resilience: Exponential Backoff with Full Jitter
In the absence of a registered municipal application token, submitting rapid sequential HTTP requests risks triggering HTTP 429 (Too Many Requests) or transient connection drops. Immediate, naive retries compound server congestion and risk persistent IP bans. To ensure robust network resilience, an exponential backoff strategy with Full Jitter was implemented: interval delays follow $T = \text{random}(0, \min(T_{\max}, T_{\text{base}} \times 2^{\text{attempt}}))$. This spreads retry attempts across the time domain, allowing transient network disruptions to recover without halting pipeline execution.

### 2.4. Storage Layer Decoupling via the Adapter Pattern
Directly hardcoding storage commands (such as serializing DataFrames inside extraction loops) introduces technical debt; downstream project requirements mandate loading data into a relational database (PostgreSQL). To eliminate future refactoring, an abstract `BaseSink` contract was established:
```python
class BaseSink(ABC):
    def write_batch(self, records: list[dict]) -> None: ...
    def write_dataframe(self, df: pd.DataFrame) -> None: ...
    def finalize(self) -> dict: ...
```
Concrete adapters (`ParquetSink`, `CsvSink`, and `JsonlSink`) implement these uniform hooks. This architectural decoupling allows introducing database destinations in subsequent phases without modifying extraction, validation, or enrichment routines, trading off a slight increase in initial class scaffolding for modularity.

### 2.5. Temporal Defect Governance: Dead-Letter Queue Quarantine Router
Data auditing revealed **4,455 records** where ticket resolution preceded creation (`closed_date < created_date`). Inverting timestamps or imputing zero durations would distort historical telemetry; conversely, silently dropping these rows via `.dropna()` would destroy data auditability.  
To resolve this, a `QuarantineRouter` based on the Dead-Letter Queue (DLQ) pattern was implemented. Inverted records were tagged with `ERR_CLOSED_BEFORE_CREATED`, while 103 closed records missing closure dates were tagged with `ERR_CLOSED_WITHOUT_CLOSED_DATE`, routing both to `quarantine.jsonl`. This isolated defective payloads to an auditable sink, maintained the quarantine rate at **1.507%** (well below the 10% threshold), and produced a pristine analytical table of **297,899 records** free from negative duration artifacts.

### 2.6. Timezone UTC Normalization & Handling Daylight Saving Transitions
Timestamps provided by the public endpoint lacked timezone metadata (naive timestamps). Because New York City observes Daylight Saving Time—shifting between Eastern Standard Time (UTC-5) and Eastern Daylight Time (UTC-4) in March and November—computing durations directly from naive strings produced 1-hour errors for requests spanning clock changeovers. All timestamps were explicitly localized to `America/New_York` and transformed to UTC ISO strings, guaranteeing robust temporal math across seasonal shifts at the cost of additional date-parsing overhead.

### 2.7. Per-Capita Demographic Normalization via NYC MODZCTA Census Data
Analyzing raw request counts provides a skewed perspective of municipal demand. Standard USPS ZIP codes proved mathematically unsuitable: administrative zones (such as 10118 for the Empire State Building) have zero residential populations, triggering division-by-zero errors.  
To address this, the NYC Department of Health's Modified ZIP Code Tabulation Area (**MODZCTA**) dataset (178 zones, 8,443,636 residents) was integrated. This standard merges commercial postal codes into adjacent residential tracts. Joining this table allowed calculating reliable complaint rates per 10,000 residents and enabled recovering the geographic borough for 1,807 records logged as `Unspecified` using incident postal codes.

### 2.8. Regular Expression Sanitation of Postal Codes
Raw postal codes exhibited structural defects: floating-point representations (`10001.0`), 9-digit ZIP+4 formats (`11201-1234`), and missing leading zeros. A dedicated regex cleaning routine was developed to extract the valid 5-digit prefix and restore leading zeros, ensuring deterministic joins against MODZCTA boundaries. The trade-off was omitting secondary 4-digit routing identifiers to maintain census alignment.

### 2.9. Cryptographic Bitwise Determinism & Run Manifest Serialization
In production data engineering, executing identical ingestion logic on identical inputs must yield bitwise identical output artifacts. If records are stored without deterministic ordering, row shuffling alters cryptographic file hashes across runs. Sinks were implemented to deterministically sort records by `unique_key` prior to serialization, and a cryptographic SHA-256 digest of all curated artifacts was logged to `run_manifest.json` to verify bitwise reproducibility.

### 2.10. Final Storage Format Optimization: Parquet vs. CSV Balance
Serializing the curated dataset to uncompressed CSV resulted in a 113 MB file, exceeding GitHub's 100 MB hard limit. To optimize storage and improve query performance, case-level data was persisted in binary columnar Parquet format using Snappy compression (14 MB). In parallel, the 3,306-row aggregated summary matrix was published in both Parquet and CSV formats to facilitate immediate inspection in plain text editors without requiring specialized tools.

### 2.11. Strict Fail-Fast Schema Drift Detection
If municipal authorities modify or drop mandatory fields (such as `unique_key` or `created_date`), silent ingestion of null columns compromises downstream integrity. The validation layer enforces strict contract validation, immediately raising a `SchemaDriftError` with exit code 3 if upstream schemas diverge from specifications.

### 2.12. Anchor-Based Project Root Independence
To eliminate brittle working-directory dependencies when executing CLI commands or unit test suites from arbitrary parent folders, an anchor-file discovery mechanism (`requirements.txt` or `.git`) was implemented to dynamically resolve absolute paths, ensuring consistent execution across diverse terminal environments.

---

## 3. Failed Approaches and Discarded Experiments
During development, three technical approaches proved unviable and were discarded:
1. **Unindexed Global Comparison Queries in SODA:** Attempting to filter inverted records remotely via `$where=closed_date < created_date` triggered an immediate `504 Timeout` due to the lack of composite indices on the remote server, demonstrating that temporal validation must occur locally within the pipeline.
2. **PowerShell Argument Expansion Escaping:** Supplying `$limit` directly in Windows PowerShell evaluated the dollar sign as an empty shell variable, resulting in an HTTP 400 Bad Request. Parameter encoding was subsequently delegated to structured Python dictionaries via `requests`.
3. **UTC Offset Stripping during Pandas Period Conversions:** Converting timestamps to monthly periods via `.dt.to_period("M")` generated warnings regarding timezone stripping. This was replaced with standard string formatting `.dt.strftime("%Y-%m")` to preserve timestamp integrity.

---

## 4. Operational Risks and Production Limitations
In an automated, continuous deployment environment, three operational vulnerabilities require ongoing monitoring:
1. **Unauthenticated Public Endpoint Throttling:** Operating without a municipal application token risks IP throttling during frequent pipeline runs. While exponential backoff absorbs transient 429 errors, continuous production deployments mandate a registered token.
2. **Upstream Schema Drift:** Rigorous contract validation halts the pipeline upon schema alterations; any unannounced municipal field renaming requires explicit schema updates before ingestion can proceed.
3. **Survivorship Bias in Open Incidents:** Approximately 3,760 tickets remain open or pending without closure dates. Calculating resolution metrics solely on closed tickets creates an optimistic survivorship bias, underrepresenting long-term, complex infrastructure projects.

---

## 5. Proposed Extensions Given Two Additional Days
1. Upgrade the extraction client to asynchronous I/O via `aiohttp` to stream batches concurrently while preserving keyset ordering, reducing extraction duration from 81 seconds to under 25 seconds.  
2. Integrate municipal polygon shapefiles to perform point-in-polygon spatial joins, mapping coordinates directly to NYC's 59 Community District boards for localized administrative analysis.

---

## 6. Transparent AI Usage Disclosure
Implementation and code writing across most parts of this project (including the API ingestion client, storage sink adapters, schema contract validation, quarantine routing, and automated tests) were carried out using a vibe-coding workflow with an AI assistant (Antigravity). The assistant was also utilized to edit and refine the technical prose of this report.

However, resolving the practical pipeline hurdles was driven directly by my own hands-on investigation: diagnosing NYC OpenData API timeouts on deep offsets and switching the ingestion logic to keyset pagination on record IDs, deciding to isolate inverted date records into a quarantine sink rather than silently dropping them, and validating dataset integrity through automated tests and cryptographic manifests. Full responsibility for the pipeline design and delivered codebase rests with me.
