from pathlib import Path

import pytest

from zugzwang.ingestion.manifest import (
    ArtifactEntry,
    SnapshotManifest,
    compute_sha256,
    read_manifest,
    verify_snapshot_artifacts,
    write_manifest,
)


def test_compute_sha256(tmp_path: Path) -> None:
    sample_file = tmp_path / 'sample.txt'
    content = b'Hello Zugzwang railway platform!'
    sample_file.write_bytes(content)

    digest, size = compute_sha256(sample_file)
    assert size == len(content)
    assert len(digest) == 64
    assert digest == '850bce375d9702739c05ec98b2fb83df702090cd5f5f7c75723c0444de1ff616'


def test_artifact_entry_validation() -> None:
    # Valid entry
    entry = ArtifactEntry(
        relative_path='railway/data-2026-06.parquet',
        byte_size=100,
        sha256='a' * 64,
        role='railway_parquet',
    )
    assert entry.relative_path == 'railway/data-2026-06.parquet'

    # Reject absolute path
    with pytest.raises(ValueError, match='must be relative'):
        ArtifactEntry(
            relative_path='/Volumes/dev/landing/file.txt',
            byte_size=10,
            sha256='a' * 64,
            role='railway_parquet',
        )

    # Reject directory traversal
    with pytest.raises(ValueError, match='directory traversal'):
        ArtifactEntry(
            relative_path='../secrets.txt',
            byte_size=10,
            sha256='a' * 64,
            role='railway_parquet',
        )

    # Reject invalid role
    with pytest.raises(ValueError, match='Invalid role'):
        ArtifactEntry(
            relative_path='railway/data.parquet',
            byte_size=10,
            sha256='a' * 64,
            role='invalid_role_name',
        )


def test_manifest_integrity_and_provenance() -> None:
    # 1. Unsupported schema_version
    with pytest.raises(ValueError, match='Unsupported schema_version'):
        SnapshotManifest(
            schema_version='99.0',
            snapshot_id='2026-06',
            created_at_utc='2026-09-01T12:00:00+00:00',
        )

    # 2. Invalid created_at_utc timestamp
    with pytest.raises(ValueError, match='Invalid ISO 8601'):
        SnapshotManifest(
            schema_version='1.0',
            snapshot_id='2026-06',
            created_at_utc='invalid-date-string',
        )

    # 3. Duplicate paths
    entry1 = ArtifactEntry('dwd/tu.txt', 100, 'a' * 64, 'dwd_observation_txt')
    entry2 = ArtifactEntry('dwd/tu.txt', 200, 'b' * 64, 'dwd_observation_txt')
    with pytest.raises(ValueError, match='Duplicate artifact path'):
        SnapshotManifest(
            schema_version='1.0',
            snapshot_id='2026-06',
            created_at_utc='2026-09-01T12:00:00+00:00',
            artifacts=(entry1, entry2),
        )

    # 4. Provenance failure: parent hash does not exist in manifest
    derived_entry = ArtifactEntry(
        relative_path='stada/stada_stations.json',
        byte_size=500,
        sha256='c' * 64,
        role='stada_json',
        parent_artifact_sha256='f' * 64,  # parent not present!
    )
    with pytest.raises(ValueError, match='references parent hash'):
        SnapshotManifest(
            schema_version='1.0',
            snapshot_id='2026-06',
            created_at_utc='2026-09-01T12:00:00+00:00',
            artifacts=(derived_entry,),
        )


def test_write_and_read_manifest_roundtrip(tmp_path: Path) -> None:
    manifest_path = tmp_path / 'manifest.json'
    raw_entry = ArtifactEntry(
        relative_path='stada/hour.parquet',
        byte_size=1000,
        sha256='d' * 64,
        role='stada_raw_parquet',
    )
    derived_entry = ArtifactEntry(
        relative_path='stada/stada_stations.json',
        byte_size=500,
        sha256='c' * 64,
        role='stada_json',
        source_url='https://example.com/stada.parquet',
        parent_artifact_sha256='d' * 64,
    )
    manifest = SnapshotManifest(
        schema_version='1.0',
        snapshot_id='2026-06',
        created_at_utc='2026-09-01T12:00:00+00:00',
        artifacts=(raw_entry, derived_entry),
    )

    write_manifest(manifest, manifest_path, exclusive=True)
    assert manifest_path.is_file()

    # Re-writing with exclusive=True raises FileExistsError
    with pytest.raises(FileExistsError):
        write_manifest(manifest, manifest_path, exclusive=True)

    loaded = read_manifest(manifest_path)
    assert loaded.schema_version == '1.0'
    assert loaded.snapshot_id == '2026-06'
    assert len(loaded.artifacts) == 2
    assert isinstance(loaded.artifacts, tuple)
    assert loaded.artifacts[1].relative_path == 'stada/stada_stations.json'
    assert loaded.artifacts[1].parent_artifact_sha256 == 'd' * 64


def test_read_manifest_malformed_json(tmp_path: Path) -> None:
    bad_manifest = tmp_path / 'manifest.json'
    bad_manifest.write_text('{ invalid json content')

    with pytest.raises(ValueError, match='Malformed JSON'):
        read_manifest(bad_manifest)


def test_verify_snapshot_artifacts(tmp_path: Path) -> None:
    f1 = tmp_path / 'railway' / 'data-2026-06.parquet'
    f1.parent.mkdir(parents=True)
    f1.write_bytes(b'parquet_bytes')
    h1, s1 = compute_sha256(f1)

    f2 = tmp_path / 'stada' / 'stada_stations.json'
    f2.parent.mkdir(parents=True)
    f2.write_bytes(b'json_bytes')
    h2, s2 = compute_sha256(f2)

    manifest = SnapshotManifest(
        schema_version='1.0',
        snapshot_id='2026-06',
        created_at_utc='2026-09-01T12:00:00+00:00',
        artifacts=(
            ArtifactEntry('railway/data-2026-06.parquet', s1, h1, 'railway_parquet'),
            ArtifactEntry('stada/stada_stations.json', s2, h2, 'stada_json'),
        ),
    )

    # 1. Matching artifacts -> no errors
    errors = verify_snapshot_artifacts(manifest, tmp_path)
    assert errors == []

    # 2. Corrupt file content -> digest mismatch error
    f2.write_bytes(b'corrupted_content')
    errors = verify_snapshot_artifacts(manifest, tmp_path)
    assert len(errors) == 1
    assert 'Size mismatch' in errors[0] or 'Digest mismatch' in errors[0]

    # 3. Missing file -> missing error
    f1.unlink()
    errors = verify_snapshot_artifacts(manifest, tmp_path)
    assert any('Missing expected artifact file' in e for e in errors)
