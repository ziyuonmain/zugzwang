---
name: rail-data-research
description: Research and validate Deutsche Bahn and German public railway data sources, schemas, identifiers, field semantics, licenses, and known data-quality limitations. Use when working with DB timetable, station, journey, delay, cancellation, or rail infrastructure data.
---

# Rail data research

Before implementing transformations against an unfamiliar railway dataset:

1. Identify the authoritative upstream source.
2. Record its license and provenance.
3. Inspect actual records.
4. Identify keys and identifiers used by the source.
5. Distinguish observed semantics from assumptions.
6. Identify known missing-data and schema-evolution behavior.
7. Document uncertain semantics rather than guessing.

Pay particular attention to:
- EVA station identifiers
- train/service identifiers
- planned vs actual events
- arrival vs departure events
- cancellation semantics
- timetable revisions
- repeated observations of the same operational event
- timestamps and time zones
- station-name changes
- schema evolution

When new source semantics are learned, update:
docs/data-sources.md

When a consequential modelling decision follows from them, add or update an
ADR under:
docs/decisions/