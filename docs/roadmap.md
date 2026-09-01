# Project roadmap and retention

## Direction

Zugzwang is intended to become a monthly railway analytics pipeline maintaining
a rolling year of analysis-ready data. June 2026 is the first validated release
and the basis of the first analytical case study.

The roadmap separates demonstrated capabilities from intended ones. A target is
not considered implemented until it has processed real source data successfully.

## Current state

- June 2026 raw sources have been transformed into Silver and Gold materialized
  views on Databricks Serverless.
- Automated source preparation exists for the fixed June snapshot.
- Source manifests record landed artifact identity and integrity.
- The analytical case study and recurring monthly operation are not complete.

## Delivery sequence

### 1. Publish the June case study

Establish the first useful analytical result before broadening the platform:

- delay and cancellation baselines;
- comparisons by station, train category, geography, and time;
- source-coverage and weather-match reporting;
- weather-stratified comparisons with quality and sensor-distance caveats;
- one reproducible notebook or concise SQL dashboard.

### 2. Harden the June contracts

- Validate complete consumer schemas and target-period coverage.
- Verify copied landing artifacts before committing the manifest.
- Protect foreign or corrupted snapshots from automated cleanup.
- Enforce the documented station, mapping, weather, and train-stop grains.
- Limit weather facts to the intended analytical period.
- Run the complete unit and Spark transformation suite in the standard check.

### 3. Process a second real month

- Introduce one validated monthly snapshot specification.
- Move landing data to period-scoped snapshot directories.
- Parameterize ingestion paths and transformation date bounds together.
- Add temporal snapshot keys to facts and reference mappings.
- Compare schema, coverage, join rates, runtime, and storage with June.

This second month is the test for the monthly abstraction. The project should
not build generic source plugins or enable a schedule before this succeeds.

### 4. Maintain a rolling year

- Make monthly replacement idempotent.
- Retain twelve months of Silver and Gold analytical data.
- Publish monthly coverage and quality metrics.
- Add scheduling based on observed upstream release availability.
- Apply retention only after the corresponding monthly pipeline output passes
  validation.

## Target monthly workflow

1. Select an available processing month.
2. Download the railway, StaDa, and DWD sources into isolated temporary storage.
3. Validate source schemas, period coverage, quality fields, and expected grain.
4. Extract period-bounded canonical artifacts.
5. Publish the month-scoped snapshot and manifest.
6. Refresh or replace the corresponding analytical month.
7. Validate row counts, uniqueness, source coverage, and join rates.
8. Apply retention to data older than the configured cutoff.

The intended workflow is monthly batch processing. Real-time ingestion and
streaming infrastructure are outside the current scope.

## Retention model

Different data classes have different operational value:

| Data class | Initial target | Reason |
| --- | --- | --- |
| Downloaded upstream archives | 30–60 days | Re-extraction and debugging during validation |
| Canonical monthly source artifacts | 13 months | Safe reconstruction of the rolling analytical window |
| Silver and Gold Delta data | 12 months | Current comparative and seasonal analysis |
| Manifests and run metadata | Indefinite | Low-cost provenance and audit history |
| Published aggregates and case-study outputs | Indefinite | Preserve findings without retaining all detailed data |

These periods are initial policy targets and must be revisited after measuring
real storage cost and analytical value.

### DWD provisional data

DWD `recent/` archives are mutable, provisionally quality controlled, and can
contain observations outside the selected month. Monthly ingestion should:

1. download and hash the upstream archive;
2. validate the required observation structure;
3. extract only the selected period into a canonical artifact;
4. retain the archive temporarily;
5. retain the canonical artifact for the rolling input window.

After an upstream archive expires, its manifest retains the URL, retrieval time,
and digest but cannot reproduce deleted bytes. Exact analytical reconstruction
then depends on the retained canonical artifact. If provisional observations
are later replaced with DWD historical data, that replacement must be recorded
as a new source version rather than performed silently.

## Scope boundaries

The following remain deferred until evidence justifies them:

- streaming railway ingestion;
- generic source plugins;
- Kafka or another message bus;
- an unbounded historical warehouse;
- concurrent or resumable source downloads;
- multiple dashboards;
- automated replacement of provisional DWD observations;
- permanent retention of every downloaded archive.

## Required decision update

[ADR 0002](decisions/0002-ingestion-snapshot-contract.md) governs the current
fixed June snapshot and requires retained source archives. It must be superseded
or amended before month-scoped snapshots and archive expiry are implemented.
