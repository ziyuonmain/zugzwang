"""Ingestion and raw source snapshot management for Zugzwang."""

from zugzwang.ingestion.manifest import (
    ArtifactEntry,
    SnapshotCorruptionError,
    SnapshotManifest,
    compute_sha256,
    read_manifest,
    write_manifest,
)
from zugzwang.ingestion.snapshot import prepare_june_2026_snapshot

__all__ = [
    'ArtifactEntry',
    'SnapshotCorruptionError',
    'SnapshotManifest',
    'compute_sha256',
    'prepare_june_2026_snapshot',
    'read_manifest',
    'write_manifest',
]
