from pathlib import Path
from unittest.mock import patch

import pytest

from zugzwang.ingestion.manifest import (
    ArtifactEntry,
    SnapshotCorruptionError,
    SnapshotManifest,
    compute_sha256,
    write_manifest,
)
from zugzwang.ingestion.snapshot import (
    STATE_A_VALID,
    STATE_B_EMPTY,
    STATE_C_INCOMPLETE,
    STATE_D_CORRUPTED,
    clean_incomplete_target,
    evaluate_target_state,
    prepare_june_2026_snapshot,
)


def _create_mock_snapshot(base_dir: Path) -> SnapshotManifest:
    """Helper to create valid files and matching manifest in base_dir."""
    p_file = base_dir / 'railway' / 'data-2026-06.parquet'
    p_file.parent.mkdir(parents=True, exist_ok=True)
    p_file.write_bytes(b'valid_parquet_bytes')
    p_hash, p_size = compute_sha256(p_file)

    s_file = base_dir / 'stada' / 'stada_stations.json'
    s_file.parent.mkdir(parents=True, exist_ok=True)
    s_file.write_bytes(b'valid_stada_bytes')
    s_hash, s_size = compute_sha256(s_file)

    manifest = SnapshotManifest(
        schema_version='1.0',
        snapshot_id='2026-06',
        created_at_utc='2026-09-01T12:00:00+00:00',
        artifacts=(
            ArtifactEntry(
                'railway/data-2026-06.parquet', p_size, p_hash, 'railway_parquet'
            ),
            ArtifactEntry('stada/stada_stations.json', s_size, s_hash, 'stada_json'),
        ),
    )
    write_manifest(manifest, base_dir / 'manifest.json', exclusive=True)
    return manifest


def test_evaluate_target_state_a_valid(tmp_path: Path) -> None:
    _create_mock_snapshot(tmp_path)
    state, manifest, errors = evaluate_target_state(tmp_path)
    assert state == STATE_A_VALID
    assert manifest is not None
    assert errors == []


def test_evaluate_target_state_b_empty(tmp_path: Path) -> None:
    state, manifest, errors = evaluate_target_state(tmp_path / 'non_existent')
    assert state == STATE_B_EMPTY
    assert manifest is None


def test_evaluate_target_state_c_incomplete_and_malformed(tmp_path: Path) -> None:
    # 1. Directory with partial file but no manifest -> State C
    p_file = tmp_path / 'railway' / 'data-2026-06.parquet'
    p_file.parent.mkdir(parents=True)
    p_file.write_bytes(b'partial')

    state, manifest, errors = evaluate_target_state(tmp_path)
    assert state == STATE_C_INCOMPLETE
    assert manifest is None

    # 2. Directory with malformed manifest JSON -> State C
    (tmp_path / 'manifest.json').write_text('{ malformed json')
    state, manifest, errors = evaluate_target_state(tmp_path)
    assert state == STATE_C_INCOMPLETE


def test_evaluate_target_state_d_foreign_snapshot_id(tmp_path: Path) -> None:
    # Structurally valid manifest for another snapshot -> State D (do NOT delete!)
    bad_id_manifest = SnapshotManifest(
        schema_version='1.0',
        snapshot_id='2026-07',
        created_at_utc='2026-09-01T12:00:00+00:00',
        artifacts=(),
    )
    write_manifest(bad_id_manifest, tmp_path / 'manifest.json', exclusive=True)
    state, manifest, errors = evaluate_target_state(tmp_path)
    assert state == STATE_D_CORRUPTED
    assert manifest is not None
    assert any('Foreign snapshot_id' in e for e in errors)


def test_evaluate_target_state_d_corrupted(tmp_path: Path) -> None:
    _create_mock_snapshot(tmp_path)

    # Corrupt the parquet file
    p_file = tmp_path / 'railway' / 'data-2026-06.parquet'
    p_file.write_bytes(b'corrupted_bytes_tampered')

    state, manifest, errors = evaluate_target_state(tmp_path)
    assert state == STATE_D_CORRUPTED
    assert manifest is not None
    assert len(errors) == 1
    assert 'Digest mismatch' in errors[0] or 'Size mismatch' in errors[0]


def test_clean_incomplete_target_managed_allowlist(tmp_path: Path) -> None:
    # Create managed directories/files and an unmanaged custom file
    (tmp_path / 'railway').mkdir()
    (tmp_path / 'railway' / 'temp.pq').write_text('tmp')
    (tmp_path / 'stada').mkdir()
    (tmp_path / 'stada' / 'temp.json').write_text('tmp')
    (tmp_path / 'dwd').mkdir()
    (tmp_path / 'manifest.json').write_text('tmp')
    (tmp_path / 'unmanaged_user_notes.txt').write_text('do not delete')

    clean_incomplete_target(tmp_path)

    assert not (tmp_path / 'railway').exists()
    assert not (tmp_path / 'stada').exists()
    assert not (tmp_path / 'dwd').exists()
    assert not (tmp_path / 'manifest.json').exists()
    assert (tmp_path / 'unmanaged_user_notes.txt').is_file()


def test_prepare_snapshot_state_a_noop(tmp_path: Path) -> None:
    _create_mock_snapshot(tmp_path)

    with patch(
        'zugzwang.ingestion.snapshot.fetch_piebro_railway_parquet'
    ) as mock_fetch:
        outcome = prepare_june_2026_snapshot(tmp_path)
        assert outcome == 'NO_OP_SNAPSHOT_VALID'
        mock_fetch.assert_not_called()


def test_prepare_snapshot_state_d_raises_without_cleanup(tmp_path: Path) -> None:
    _create_mock_snapshot(tmp_path)
    p_file = tmp_path / 'railway' / 'data-2026-06.parquet'
    p_file.write_bytes(b'tampered')

    with pytest.raises(SnapshotCorruptionError, match='Snapshot corruption'):
        prepare_june_2026_snapshot(tmp_path)

    # Corrupted evidence file must NOT be deleted
    assert p_file.exists()
    assert (tmp_path / 'manifest.json').exists()


def test_prepare_snapshot_state_b_and_c(tmp_path: Path) -> None:
    target = tmp_path / 'landing'
    staging_base = tmp_path / 'scratch'

    # Mock all fetchers
    def fake_railway(staging):
        f = staging / 'railway/data-2026-06.parquet'
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b'rail_data')
        h, s = compute_sha256(f)
        return ArtifactEntry('railway/data-2026-06.parquet', s, h, 'railway_parquet')

    def fake_stada(staging):
        r = staging / 'stada/hour_00_19_20_21_22_23.parquet'
        r.parent.mkdir(parents=True, exist_ok=True)
        r.write_bytes(b'stada_raw')
        rh, rs = compute_sha256(r)
        d = staging / 'stada/stada_stations.json'
        d.write_bytes(b'stada_json')
        dh, ds = compute_sha256(d)
        return (
            ArtifactEntry(
                'stada/hour_00_19_20_21_22_23.parquet', rs, rh, 'stada_raw_parquet'
            ),
            ArtifactEntry(
                'stada/stada_stations.json',
                ds,
                dh,
                'stada_json',
                parent_artifact_sha256=rh,
            ),
        )

    def fake_dwd(staging):
        m = staging / 'dwd/metadata/TU_Stundenwerte_Beschreibung_Stationen.txt'
        m.parent.mkdir(parents=True, exist_ok=True)
        m.write_bytes(b'tu_meta')
        mh, ms = compute_sha256(m)
        return [
            ArtifactEntry(
                'dwd/metadata/TU_Stundenwerte_Beschreibung_Stationen.txt',
                ms,
                mh,
                'dwd_metadata',
            )
        ]

    with (
        patch(
            'zugzwang.ingestion.snapshot.fetch_piebro_railway_parquet',
            side_effect=fake_railway,
        ),
        patch(
            'zugzwang.ingestion.snapshot.fetch_and_extract_stada_snapshot',
            side_effect=fake_stada,
        ),
        patch(
            'zugzwang.ingestion.snapshot.fetch_and_extract_dwd_observations',
            side_effect=fake_dwd,
        ),
    ):
        # 1. State B: Empty publication
        outcome = prepare_june_2026_snapshot(target, staging_base_dir=staging_base)
        assert outcome == 'PUBLICATION_COMPLETED'
        assert (target / 'manifest.json').is_file()
        assert (target / 'railway/data-2026-06.parquet').is_file()
        assert (target / 'stada/hour_00_19_20_21_22_23.parquet').is_file()
        assert (target / 'stada/stada_stations.json').is_file()

        # 2. Corrupt manifest to simulate State C incomplete candidate
        (target / 'manifest.json').write_text('{ bad json')
        outcome_c = prepare_june_2026_snapshot(target, staging_base_dir=staging_base)
        assert outcome_c == 'RECOVERY_AND_PUBLICATION_COMPLETED'
        assert (target / 'manifest.json').is_file()
