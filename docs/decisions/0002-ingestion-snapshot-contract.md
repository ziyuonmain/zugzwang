# ADR 0002: Ingestion snapshot and execution contract

## Status

Accepted

## Context

The June 2026 vertical slice currently depends on source files prepared
manually. Milestone 2 will automate acquisition of piebro railway data, the
contemporaneous StaDa snapshot, and DWD observations before refreshing the
existing Lakeflow Declarative Pipeline.

Some upstream objects are mutable. In particular, piebro can reprocess monthly
releases and DWD continuously updates files under `recent/`. Publishing hundreds
of files directly into stable landing paths can also expose a partial mixture if
a run fails.

The initial project needs reproducible fixed-period snapshots without adding a
database-backed ingestion ledger or a general ingestion framework.

## Decision

1. **Immutable period snapshot:** A completed snapshot is immutable. An
   ingestion rerun verifies its manifest and content digests, then exits without
   replacing valid artifacts.
2. **Local preparation:** Each run uses a unique ephemeral directory for
   downloading, extracting, hashing, and validating files.
3. **Validate before publication:** No artifact is published until the complete
   local candidate passes source-specific validation.
4. **Completion marker:** `manifest.json` is written only after the snapshot
   artifacts have been published successfully. A missing or invalid manifest
   identifies an incomplete landing snapshot.
5. **Incomplete-run recovery:** A later run may clean only a known incomplete
   target for the same snapshot. It must never modify a target whose manifest
   and digests validate.
6. **Source retention:** Preserve downloaded piebro Parquet files and DWD ZIP
   archives alongside the extracted files consumed by the pipeline. The
   manifest links every derived artifact to its source URL and digest.
7. **No overlapping runs:** Concurrent preparation of the same snapshot is not
   supported initially. The Lakeflow Job configuration must prevent overlapping
   runs.
8. **Canonical execution path:** The two-task Lakeflow Job is the supported
   end-to-end path: `prepare_sources` succeeds before `refresh_pipeline` starts.
   A direct pipeline refresh is valid only when the configured landing snapshot
   has already passed manifest validation.
9. **Responsibility boundary:** Procedural acquisition and publication remain
   outside the declarative pipeline. The pipeline continues to own Raw-to-Silver
   and Silver-to-Gold transformations.

## Consequences

### Positive

- Repeated runs do not silently replace an accepted analytical snapshot.
- Mutable upstream URLs remain traceable to the exact bytes used.
- Source extraction can be reproduced from retained archives.
- The transformation pipeline remains declarative and independently testable.

### Negative

- Retaining source archives uses more volume storage.
- Recovery logic must distinguish incomplete targets from valid immutable ones.
- Direct pipeline execution requires an already validated landing snapshot.

## Deferred

- concurrent downloads;
- byte-range resume;
- multiple active snapshot generations and pointer switching;
- Delta-based ingestion ledgers;
- generic source plugins;
- live or streaming ingestion.
