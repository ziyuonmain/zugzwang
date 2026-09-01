"""Manifest schema, hashing, and integrity validation for snapshot ingestion."""

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

SUPPORTED_SCHEMA_VERSIONS = {'1.0'}
ALLOWED_ROLES = {
    'railway_parquet',
    'stada_raw_parquet',
    'stada_json',
    'dwd_metadata',
    'dwd_observation_zip',
    'dwd_observation_txt',
}


class SnapshotCorruptionError(Exception):
    """Raised when a valid snapshot manifest exists but artifact integrity fails."""


@dataclass(frozen=True)
class ArtifactEntry:
    """Represents a single ingested or derived file in a snapshot."""

    relative_path: str
    byte_size: int
    sha256: str
    role: str
    source_url: str | None = None
    parent_artifact_sha256: str | None = None

    def __post_init__(self) -> None:
        """Validates entry fields upon instantiation."""
        _validate_relative_path(self.relative_path)
        if self.byte_size < 0:
            raise ValueError(f'Byte size cannot be negative: {self.byte_size}')
        if len(self.sha256) != 64 or not all(
            c in '0123456789abcdefABCDEF' for c in self.sha256
        ):
            raise ValueError(f'Invalid SHA-256 hex digest: {self.sha256}')
        if not self.role or self.role not in ALLOWED_ROLES:
            raise ValueError(
                f"Invalid role '{self.role}', expected one of {sorted(ALLOWED_ROLES)}"
            )
        if self.source_url is not None and not isinstance(self.source_url, str):
            raise ValueError(
                f'source_url must be a string or None, got: {type(self.source_url)}'
            )
        if self.parent_artifact_sha256 is not None:
            if (
                not isinstance(self.parent_artifact_sha256, str)
                or len(self.parent_artifact_sha256) != 64
            ):
                raise ValueError(
                    f'Invalid parent SHA-256 digest: {self.parent_artifact_sha256}'
                )


@dataclass(frozen=True)
class SnapshotManifest:
    """Immutable manifest pinning a complete period snapshot."""

    schema_version: str
    snapshot_id: str
    created_at_utc: str
    artifacts: tuple[ArtifactEntry, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        """Validates manifest integrity, provenance links, and path uniqueness."""
        if self.schema_version not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(
                f"Unsupported schema_version '{self.schema_version}'. "
                f'Supported: {sorted(SUPPORTED_SCHEMA_VERSIONS)}'
            )
        if not self.snapshot_id or not self.snapshot_id.strip():
            raise ValueError('snapshot_id cannot be empty')

        # Validate created_at_utc timestamp format
        try:
            datetime.fromisoformat(self.created_at_utc)
        except Exception as exc:
            raise ValueError(
                f"Invalid ISO 8601 created_at_utc timestamp '{self.created_at_utc}': {exc}"
            ) from exc

        paths: set[str] = set()
        artifact_hashes: set[str] = set()
        for artifact in self.artifacts:
            if artifact.relative_path in paths:
                raise ValueError(
                    f'Duplicate artifact path in manifest: {artifact.relative_path}'
                )
            paths.add(artifact.relative_path)
            artifact_hashes.add(artifact.sha256.lower())

        # Validate provenance: parent artifact must be present in manifest
        for artifact in self.artifacts:
            if artifact.parent_artifact_sha256 is not None:
                parent_hash = artifact.parent_artifact_sha256.lower()
                if parent_hash not in artifact_hashes:
                    raise ValueError(
                        f"Artifact '{artifact.relative_path}' references parent hash "
                        f"'{parent_hash}' which is not present in the manifest."
                    )


def _validate_relative_path(rel_path: str) -> None:
    """Validates that a relative path does not traverse or escape root."""
    if not rel_path or not rel_path.strip():
        raise ValueError('Relative path cannot be empty')
    path_obj = Path(rel_path)
    if path_obj.is_absolute():
        raise ValueError(f'Artifact path must be relative, got absolute: {rel_path}')
    for part in path_obj.parts:
        if part == '..':
            raise ValueError(
                f"Artifact path cannot contain directory traversal '..': {rel_path}"
            )


def compute_sha256(file_path: Path | str, chunk_size: int = 65536) -> tuple[str, int]:
    """Computes the SHA-256 digest and total byte size of a file.

    Args:
        file_path: Path to the target file.
        chunk_size: Byte size of chunks to stream from disk.

    Returns:
        A tuple of (sha256_hex_lowercase, total_bytes).

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    path = Path(file_path)
    hasher = hashlib.sha256()
    total_bytes = 0

    with path.open('rb') as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
            total_bytes += len(chunk)

    return hasher.hexdigest().lower(), total_bytes


def write_manifest(
    manifest: SnapshotManifest,
    target_path: Path | str,
    exclusive: bool = True,
) -> None:
    """Writes a SnapshotManifest to disk as formatted JSON.

    Args:
        manifest: The SnapshotManifest instance to write.
        target_path: File path where manifest.json should be saved.
        exclusive: If True, uses 'x' mode to prevent overwriting existing files.

    Raises:
        FileExistsError: If exclusive is True and the target file already exists.
    """
    path = Path(target_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(manifest)
    json_bytes = json.dumps(payload, indent=2, ensure_ascii=False).encode('utf-8')

    mode = 'xb' if exclusive else 'wb'
    with path.open(mode) as f:
        f.write(json_bytes)


def read_manifest(manifest_path: Path | str) -> SnapshotManifest:
    """Reads and validates a SnapshotManifest from a JSON file.

    Args:
        manifest_path: Path to the manifest.json file.

    Returns:
        The validated SnapshotManifest instance.

    Raises:
        FileNotFoundError: If manifest file does not exist.
        ValueError: If JSON is malformed or violates schema/path constraints.
    """
    path = Path(manifest_path)
    with path.open('r', encoding='utf-8') as f:
        try:
            data: dict[str, Any] = json.load(f)
        except json.JSONDecodeError as exc:
            raise ValueError(f'Malformed JSON in manifest: {exc}') from exc

    if not isinstance(data, dict):
        raise ValueError('Manifest root must be a JSON object')

    schema_version = data.get('schema_version')
    snapshot_id = data.get('snapshot_id')
    created_at_utc = data.get('created_at_utc')
    raw_artifacts = data.get('artifacts')

    if not isinstance(schema_version, str) or not isinstance(snapshot_id, str):
        raise ValueError('Manifest missing required schema_version or snapshot_id')
    if not isinstance(created_at_utc, str) or not isinstance(raw_artifacts, list):
        raise ValueError('Manifest missing required created_at_utc or artifacts array')

    artifacts: list[ArtifactEntry] = []
    for item in raw_artifacts:
        if not isinstance(item, dict):
            raise ValueError(f'Manifest artifact item must be object: {item}')
        artifacts.append(
            ArtifactEntry(
                relative_path=str(item.get('relative_path', '')),
                byte_size=int(item.get('byte_size', -1)),
                sha256=str(item.get('sha256', '')).lower(),
                role=str(item.get('role', '')),
                source_url=item.get('source_url'),
                parent_artifact_sha256=item.get('parent_artifact_sha256'),
            )
        )

    return SnapshotManifest(
        schema_version=schema_version,
        snapshot_id=snapshot_id,
        created_at_utc=created_at_utc,
        artifacts=tuple(artifacts),
    )


def verify_snapshot_artifacts(
    manifest: SnapshotManifest,
    base_dir: Path | str,
) -> list[str]:
    """Verifies that all artifacts listed in a manifest exist and match digests.

    Args:
        manifest: The manifest describing expected artifacts.
        base_dir: Root directory containing the landing snapshot files.

    Returns:
        List of error descriptions for any missing or mismatched artifacts.
    """
    root = Path(base_dir)
    errors: list[str] = []

    for entry in manifest.artifacts:
        file_path = root / entry.relative_path
        if not file_path.is_file():
            errors.append(f'Missing expected artifact file: {entry.relative_path}')
            continue

        actual_hash, actual_size = compute_sha256(file_path)
        if actual_size != entry.byte_size:
            errors.append(
                f'Size mismatch for {entry.relative_path}: expected {entry.byte_size} bytes, got {actual_size}'
            )
        elif actual_hash != entry.sha256.lower():
            errors.append(
                f'Digest mismatch for {entry.relative_path}: expected {entry.sha256}, got {actual_hash}'
            )

    return errors
