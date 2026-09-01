# Initial dataset setup

This guide deploys and runs Zugzwang's validated June 2026 vertical slice. The
supported entry point is the two-task Lakeflow Job defined by the Databricks
bundle:

1. `prepare_sources` downloads, validates, and publishes the immutable source
   snapshot with a manifest.
2. `refresh_pipeline` materializes the Silver and Gold datasets after source
   preparation succeeds.

Do not upload files directly into the landing Volume or use `--overwrite`. That
bypasses the snapshot integrity and provenance checks defined by
[ADR 0002](decisions/0002-ingestion-snapshot-contract.md).

## Prerequisites

- Python 3.12 or newer
- [uv](https://docs.astral.sh/uv/)
- Java 17 or newer for local PySpark tests
- [Task](https://taskfile.dev/)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- Access to a Databricks workspace with Unity Catalog and serverless pipeline
  support

Verify the local tools:

```bash
python --version
uv --version
java -version
task --version
databricks version
```

Authenticate the Databricks CLI using an appropriate profile. Do not store
credentials in this repository.

```bash
databricks auth profiles
```

If more than one profile is configured, pass `--profile <profile-name>` to the
Databricks commands or configure the intended profile in your shell session.

## Verify locally

Install the locked dependencies and run the complete local check:

```bash
uv sync
task check
```

The check runs Ruff linting and formatting validation, ty type checking, table
metadata validation, and the unit and local PySpark transformation tests.

## Validate and deploy the bundle

Validate the development target:

```bash
task validate:dev
```

Then deploy its catalog schemas, landing Volume, Lakeflow pipeline, and
orchestration job:

```bash
task deploy
```

The development target uses the catalog configured by `var.catalog`, currently
`zugzwang_dev`. The bundle creates these resources:

- `raw.landing`: managed Volume for the manifest-validated source snapshot;
- `silver`: normalized materialized views;
- `gold`: the analytical materialized view;
- `zugzwang_june2026`: the declarative transformation pipeline; and
- `zugzwang_pipeline_job`: the end-to-end orchestration job.

## Run the end-to-end job

Run the canonical entry point:

```bash
task run-job
```

The first run downloads the June railway Parquet release, extracts the
contemporaneous StaDa snapshot, downloads and extracts the DWD archives,
validates all artifacts, and writes `manifest.json` last as the completion
marker. A safe rerun verifies the existing manifest and artifact digests instead
of replacing a valid snapshot.

The source preparation task aborts without cleanup if it finds a foreign
manifest or artifacts that do not match a valid manifest. This preserves the
unexpected state for investigation.

## Verify the result

In the Databricks workspace, open **Jobs & Pipelines** and inspect both tasks of
the orchestration run. Confirm that `prepare_sources` succeeded before
`refresh_pipeline` started.

Check the following published datasets in Catalog Explorer or with SQL:

```sql
SELECT COUNT(*) FROM zugzwang_dev.silver.train_stops;
SELECT COUNT(*) FROM zugzwang_dev.gold.train_stop_weather;
```

For the accepted June 2026 source snapshot, both counts should be `14,752,336`
when the enrichment joins preserve the train-stop grain. Also verify:

- the `id` grain of `silver.train_stops` and `gold.train_stop_weather`;
- the EVA grain of `silver.stations` and `silver.station_weather_mapping`;
- the station-hour grain of both weather observation datasets;
- station and weather join coverage;
- pipeline expectation results; and
- the six documented missing railway collection hours are not presented as
  complete national coverage.

The table and column contracts are defined in `metadata/silver_tables.yml` and
`metadata/gold_tables.yml`. Source provenance and limitations are documented in
[Data sources](data-sources.md).

## Direct pipeline runs

For normal operation, use `task run-job`. Running only the transformation
pipeline is supported when the landing snapshot already has a valid manifest
and matching artifact digests:

```bash
task run-pipeline
```

Use a validation-only pipeline update when changing the pipeline graph:

```bash
databricks bundle run -t dev --validate-only zugzwang_june2026
```

Neither command prepares or repairs source data.

## Local ingestion diagnostics

To exercise source preparation outside Databricks, target a disposable local
directory:

```bash
task prepare-local LANDING_PATH=/tmp/zugzwang-landing-test
```

This downloads the complete public snapshot and can be expensive in time,
bandwidth, and disk space. The target must not contain unrelated files. Local
diagnostic output and downloaded datasets must not be committed.

## Current scope

The implementation is fixed to June 2026. It does not yet support an arbitrary
processing month, recurring scheduling, or rolling retention. Those changes
follow the sequence in the [project roadmap](roadmap.md).
