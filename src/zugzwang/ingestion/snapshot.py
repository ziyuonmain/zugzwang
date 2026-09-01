"""Snapshot lifecycle management, 4-state recovery, and publication."""

import logging
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

from zugzwang.ingestion.fetchers import (
    SourceExtractionError,
    fetch_and_extract_dwd_observations,
    fetch_and_extract_stada_snapshot,
    fetch_piebro_railway_parquet,
)
from zugzwang.ingestion.manifest import (
    ArtifactEntry,
    SnapshotCorruptionError,
    SnapshotManifest,
    read_manifest,
    verify_snapshot_artifacts,
    write_manifest,
)

logger = logging.getLogger(__name__)

SNAPSHOT_ID = '2026-06'
MANIFEST_FILENAME = 'manifest.json'
SCHEMA_VERSION = '1.0'

# Explicit allowlist of prefixes managed by Zugzwang landing snapshots
MANAGED_PREFIXES = ('railway', 'stada', 'dwd', 'manifest.json')

STATE_A_VALID = 'STATE_A_VALID'
STATE_B_EMPTY = 'STATE_B_EMPTY'
STATE_C_INCOMPLETE = 'STATE_C_INCOMPLETE'
STATE_D_CORRUPTED = 'STATE_D_CORRUPTED'


def evaluate_target_state(
    target_dir: Path,
) -> tuple[str, SnapshotManifest | None, list[str]]:
    """Evaluates the landing target directory into one of four lifecycle states.

    Args:
        target_dir: Target landing volume directory path.

    Returns:
        A tuple of (state_name, parsed_manifest_or_none, error_messages_list).
    """
    manifest_path = target_dir / MANIFEST_FILENAME

    if manifest_path.is_file():
        try:
            manifest = read_manifest(manifest_path)
        except Exception as exc:
            # Unparseable/corrupted JSON file without valid manifest structure
            return STATE_C_INCOMPLETE, None, [f'Malformed manifest: {exc}']

        # A structurally valid manifest with foreign snapshot_id or unsupported version
        # is foreign/corrupted state and must NOT be cleaned up as an incomplete run.
        if manifest.snapshot_id != SNAPSHOT_ID:
            return (
                STATE_D_CORRUPTED,
                manifest,
                [
                    f"Foreign snapshot_id '{manifest.snapshot_id}', expected '{SNAPSHOT_ID}'. "
                    f'Aborting without cleanup.'
                ],
            )

        errors = verify_snapshot_artifacts(manifest, target_dir)
        if errors:
            return STATE_D_CORRUPTED, manifest, errors
        return STATE_A_VALID, manifest, []

    # Manifest does not exist
    if not target_dir.exists():
        return STATE_B_EMPTY, None, []

    existing_files = [p for p in target_dir.rglob('*') if p.is_file()]
    if not existing_files:
        return STATE_B_EMPTY, None, []

    return (
        STATE_C_INCOMPLETE,
        None,
        [
            f'Landing directory contains {len(existing_files)} files without a manifest.json'
        ],
    )


def clean_incomplete_target(target_dir: Path) -> None:
    """Removes managed candidate files from an incomplete landing target.

    Only removes files and directories matching MANAGED_PREFIXES allowlist.

    Args:
        target_dir: Target landing directory to clean.
    """
    if not target_dir.exists():
        return

    logger.info('[RECOVERY] Cleaning incomplete candidate files in %s', target_dir)
    for child in target_dir.iterdir():
        if child.name in MANAGED_PREFIXES:
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()


def publish_staged_snapshot(
    staging_dir: Path,
    target_dir: Path,
    manifest: SnapshotManifest,
) -> None:
    """Verifies staged candidate, copies to target volume, verifies destination, and commits manifest.

    Args:
        staging_dir: Ephemeral directory containing validated staged files.
        target_dir: Destination landing directory (e.g. in /Volumes/...).
        manifest: The snapshot manifest to commit.

    Raises:
        SourceExtractionError: If candidate verification fails before or after publication.
    """
    # 1. Verify staged candidate integrity before publication
    staged_errors = verify_snapshot_artifacts(manifest, staging_dir)
    if staged_errors:
        raise SourceExtractionError(
            f'Pre-publication verification failed in staging directory: {staged_errors}'
        )

    target_dir.mkdir(parents=True, exist_ok=True)

    # 2. Copy all staged artifacts to destination
    for artifact in manifest.artifacts:
        src_file = staging_dir / artifact.relative_path
        dst_file = target_dir / artifact.relative_path
        dst_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_file, dst_file)

    # 3. Verify destination artifacts match manifest digests before committing completion marker
    dest_errors = verify_snapshot_artifacts(manifest, target_dir)
    if dest_errors:
        raise SourceExtractionError(
            f'Post-copy destination verification failed in {target_dir}: {dest_errors}'
        )

    # 4. Write manifest.json as the final completion marker
    manifest_dest = target_dir / MANIFEST_FILENAME
    write_manifest(manifest, manifest_dest, exclusive=True)
    logger.info(
        '[PUBLISH] %d artifacts copied to %s and verified; %s committed',
        len(manifest.artifacts),
        target_dir,
        MANIFEST_FILENAME,
    )


def prepare_june_2026_snapshot(
    target_dir: Path | str,
    staging_base_dir: Path | str | None = None,
) -> str:
    """Orchestrates acquisition, validation, and publication for June 2026.

    Args:
        target_dir: Destination path for the landed snapshot (e.g. /Volumes/.../landing).
        staging_base_dir: Base directory for ephemeral scratch staging (defaults to /tmp).

    Returns:
        Completion status string:
        'NO_OP_SNAPSHOT_VALID', 'PUBLICATION_COMPLETED', or 'RECOVERY_AND_PUBLICATION_COMPLETED'.

    Raises:
        SnapshotCorruptionError: If target has a valid manifest but corrupted files or foreign snapshot.
        Exception: If network, extraction, or publication fails.
    """
    target_path = Path(target_dir)
    state, manifest, errors = evaluate_target_state(target_path)

    logger.info('[LIFECYCLE] Target evaluated to %s at %s', state, target_path)

    if state == STATE_A_VALID:
        logger.info(
            '[COMPLETED] Valid snapshot %s already exists with %d matching artifacts. No-op.',
            SNAPSHOT_ID,
            len(manifest.artifacts) if manifest else 0,
        )
        return 'NO_OP_SNAPSHOT_VALID'

    if state == STATE_D_CORRUPTED:
        err_msg = (
            f'Snapshot corruption/foreign state detected in {target_path} for {SNAPSHOT_ID}! '
            f'Errors: {errors}. Aborting without cleanup to preserve evidence.'
        )
        logger.error('[LIFECYCLE] %s', err_msg)
        raise SnapshotCorruptionError(err_msg)

    is_recovery = state == STATE_C_INCOMPLETE
    if is_recovery:
        logger.warning(
            '[LIFECYCLE] Incomplete candidate state detected (%s). Initiating cleanup.',
            errors,
        )
        clean_incomplete_target(target_path)

    # Prepare candidate in isolated ephemeral staging directory
    stage_root = Path(staging_base_dir or '/tmp')
    staging_dir = stage_root / f'zugzwang-stage-{uuid.uuid4().hex}'
    staging_dir.mkdir(parents=True, exist_ok=True)

    try:
        artifacts: list[ArtifactEntry] = []

        # 1. Fetch railway Parquet
        railway_entry = fetch_piebro_railway_parquet(staging_dir)
        artifacts.append(railway_entry)

        # 2. Fetch and extract StaDa stations
        raw_stada, derived_stada = fetch_and_extract_stada_snapshot(staging_dir)
        artifacts.extend([raw_stada, derived_stada])

        # 3. Fetch and extract DWD observations
        dwd_artifacts = fetch_and_extract_dwd_observations(staging_dir)
        artifacts.extend(dwd_artifacts)

        # 4. Construct manifest
        now_utc = datetime.now(UTC).isoformat()
        manifest = SnapshotManifest(
            schema_version=SCHEMA_VERSION,
            snapshot_id=SNAPSHOT_ID,
            created_at_utc=now_utc,
            artifacts=tuple(artifacts),
        )

        logger.info(
            '[VALIDATE] Candidate snapshot assembled with %d total artifacts. Publishing...',
            len(artifacts),
        )

        # 5. Publish to target directory, verify, and commit manifest
        publish_staged_snapshot(staging_dir, target_path, manifest)

        outcome = (
            'RECOVERY_AND_PUBLICATION_COMPLETED'
            if is_recovery
            else 'PUBLICATION_COMPLETED'
        )
        logger.info('[COMPLETED] Lifecycle finished with result: %s', outcome)
        return outcome

    finally:
        # Safely remove ephemeral staging directory
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)
