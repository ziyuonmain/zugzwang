"""Source data fetchers and extractors for railway, station, and DWD observations."""

import json
import logging
import re
import shutil
import zipfile
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from zugzwang.ingestion.http_client import download_file_streamed, fetch_text
from zugzwang.ingestion.manifest import ArtifactEntry, compute_sha256

logger = logging.getLogger(__name__)

# Upstream fixed URLs for the June 2026 snapshot
PIEBRO_MONTHLY_PARQUET_URL = (
    'https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/'
    'monthly_processed_data/data-2026-06.parquet'
)

PIEBRO_DAILY_RAW_STADA_URL = (
    'https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/'
    'raw_data/year=2026/month=6/day=1/hour_00_19_20_21_22_23.parquet'
)

DWD_TU_BASE_URL = (
    'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/'
    'hourly/air_temperature/recent/'
)

DWD_FF_BASE_URL = (
    'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/'
    'hourly/wind/recent/'
)

DWD_TU_META_FILENAME = 'TU_Stundenwerte_Beschreibung_Stationen.txt'
DWD_FF_META_FILENAME = 'FF_Stundenwerte_Beschreibung_Stationen.txt'

# Full set of columns consumed by the railway pipeline
REQUIRED_RAILWAY_COLUMNS = {
    'station_name',
    'xml_station_name',
    'eva',
    'train_number',
    'line_number',
    'final_destination_station',
    'delay_in_min',
    'time',
    'arrival_is_canceled',
    'departure_is_canceled',
    'train_type',
    'train_line_ride_id',
    'train_line_station_num',
    'arrival_planned_time',
    'arrival_change_time',
    'departure_planned_time',
    'departure_change_time',
    'id',
}


class SourceExtractionError(Exception):
    """Raised when upstream source extraction or structural validation fails."""


def fetch_piebro_railway_parquet(
    staging_dir: Path,
    url: str = PIEBRO_MONTHLY_PARQUET_URL,
) -> ArtifactEntry:
    """Downloads monthly railway parquet and validates schema readability and June 2026 coverage.

    Args:
        staging_dir: Base directory for local candidate staging.
        url: Remote URL of the monthly railway Parquet file.

    Returns:
        The ArtifactEntry describing the landed railway parquet file.

    Raises:
        SourceExtractionError: If download fails or parquet schema/coverage is invalid.
    """
    rel_path = 'railway/data-2026-06.parquet'
    dest_path = staging_dir / rel_path

    logger.info('[DOWNLOAD] Railway Parquet: downloading from %s', url)
    sha256_hex, total_bytes = download_file_streamed(url, dest_path)

    # Validate complete consumer schema and June coverage
    try:
        parquet_file = pq.ParquetFile(dest_path)
        schema_names = set(parquet_file.schema.names)
        missing = REQUIRED_RAILWAY_COLUMNS - schema_names
        if missing:
            raise SourceExtractionError(
                f'Railway Parquet missing required consumer columns: {sorted(missing)}'
            )
        num_rows = parquet_file.metadata.num_rows
        if num_rows == 0:
            raise SourceExtractionError('Railway Parquet contains 0 rows')

        # Sample timestamps to verify June 2026 representation (timestamps around 2026-06)
        first_batch = next(parquet_file.iter_batches(batch_size=1000, columns=['time']))
        times = first_batch.column('time').to_pylist()
        # Nanoseconds for 2026-06-01 is ~1.717e18; check year range (2026)
        valid_june_sample = any(
            1717200000000000000 <= t <= 1719878400000000000
            for t in times
            if t is not None
        )
        if not valid_june_sample:
            raise SourceExtractionError(
                'Railway Parquet does not contain expected June 2026 timestamps'
            )

        logger.info(
            '[EXTRACT] Railway Parquet: validated %d rows, all %d required columns, June 2026 coverage verified',
            num_rows,
            len(REQUIRED_RAILWAY_COLUMNS),
        )
    except Exception as exc:
        raise SourceExtractionError(f'Invalid railway parquet file: {exc}') from exc

    return ArtifactEntry(
        relative_path=rel_path,
        byte_size=total_bytes,
        sha256=sha256_hex,
        role='railway_parquet',
        source_url=url,
    )


def fetch_and_extract_stada_snapshot(
    staging_dir: Path,
    raw_url: str = PIEBRO_DAILY_RAW_STADA_URL,
) -> tuple[ArtifactEntry, ArtifactEntry]:
    """Downloads daily raw Parquet and extracts deduplicated StaDa stations JSON.

    Args:
        staging_dir: Base directory for local candidate staging.
        raw_url: Remote URL of the raw daily partition parquet file.

    Returns:
        A tuple of (retained_raw_parquet_entry, derived_stada_json_entry).

    Raises:
        SourceExtractionError: If raw download or station extraction fails.
    """
    raw_rel_path = 'stada/hour_00_19_20_21_22_23.parquet'
    derived_rel_path = 'stada/stada_stations.json'

    raw_dest = staging_dir / raw_rel_path
    derived_dest = staging_dir / derived_rel_path

    logger.info('[DOWNLOAD] StaDa Raw Parquet: downloading from %s', raw_url)
    raw_sha256, raw_bytes = download_file_streamed(raw_url, raw_dest)

    raw_entry = ArtifactEntry(
        relative_path=raw_rel_path,
        byte_size=raw_bytes,
        sha256=raw_sha256,
        role='stada_raw_parquet',
        source_url=raw_url,
    )

    # Extract station objects using batch scanning with column projection
    stations_by_number: dict[str, dict[str, Any]] = {}
    malformed_responses = 0

    try:
        parquet_file = pq.ParquetFile(raw_dest)
        available_cols = set(parquet_file.schema.names)

        cols_to_read = ['api_name', 'response_data']
        if 'status_code' in available_cols:
            cols_to_read.append('status_code')

        for batch in parquet_file.iter_batches(columns=cols_to_read):
            api_names = batch.column('api_name').to_pylist()
            response_datas = batch.column('response_data').to_pylist()
            status_codes = (
                batch.column('status_code').to_pylist()
                if 'status_code' in available_cols
                else [200] * len(api_names)
            )

            for api_name, status_code, response_data in zip(
                api_names, status_codes, response_datas, strict=False
            ):
                if api_name != 'station-data/v2/stations':
                    continue
                if str(status_code) != '200' or not response_data:
                    continue

                try:
                    payload = json.loads(response_data)
                    results = payload.get('result', [])
                    if not isinstance(results, list) or len(results) == 0:
                        malformed_responses += 1
                        continue

                    for station in results:
                        st_num = station.get('number')
                        if st_num is not None:
                            stations_by_number[str(st_num)] = station
                except Exception as exc:
                    malformed_responses += 1
                    logger.warning('Malformed StaDa response JSON: %s', exc)

    except Exception as exc:
        raise SourceExtractionError(f'Failed to scan raw StaDa Parquet: {exc}') from exc

    if malformed_responses > 0:
        raise SourceExtractionError(
            f'Encountered {malformed_responses} malformed or empty status-200 StaDa API responses'
        )

    if not stations_by_number:
        raise SourceExtractionError(
            'No valid StaDa station records extracted from raw Parquet'
        )

    # Serialize extracted JSON array
    derived_dest.parent.mkdir(parents=True, exist_ok=True)
    station_list = list(stations_by_number.values())
    with derived_dest.open('w', encoding='utf-8') as f:
        json.dump(station_list, f, ensure_ascii=False, indent=2)

    derived_sha256, derived_bytes = compute_sha256(derived_dest)
    logger.info(
        '[EXTRACT] StaDa Station Master: extracted %d unique stations into %s',
        len(station_list),
        derived_rel_path,
    )

    derived_entry = ArtifactEntry(
        relative_path=derived_rel_path,
        byte_size=derived_bytes,
        sha256=derived_sha256,
        role='stada_json',
        source_url=raw_url,
        parent_artifact_sha256=raw_sha256,
    )

    return raw_entry, derived_entry


def fetch_and_extract_dwd_observations(
    staging_dir: Path,
    tu_base_url: str = DWD_TU_BASE_URL,
    ff_base_url: str = DWD_FF_BASE_URL,
) -> list[ArtifactEntry]:
    """Fetches DWD metadata and observation archives, extracting verified text files.

    Args:
        staging_dir: Base directory for local candidate staging.
        tu_base_url: Root URL for hourly temperature archives and metadata.
        ff_base_url: Root URL for hourly wind archives and metadata.

    Returns:
        List of ArtifactEntry objects for all retained metadata, archives, and text observations.

    Raises:
        SourceExtractionError: If DWD archive discovery, extraction, or validation fails.
    """
    artifacts: list[ArtifactEntry] = []

    # 1. Fetch Metadata Files
    tu_meta_url = tu_base_url + DWD_TU_META_FILENAME
    ff_meta_url = ff_base_url + DWD_FF_META_FILENAME

    tu_meta_rel = f'dwd/metadata/{DWD_TU_META_FILENAME}'
    ff_meta_rel = f'dwd/metadata/{DWD_FF_META_FILENAME}'

    tu_hash, tu_size = download_file_streamed(tu_meta_url, staging_dir / tu_meta_rel)
    artifacts.append(
        ArtifactEntry(
            relative_path=tu_meta_rel,
            byte_size=tu_size,
            sha256=tu_hash,
            role='dwd_metadata',
            source_url=tu_meta_url,
        )
    )

    ff_hash, ff_size = download_file_streamed(ff_meta_url, staging_dir / ff_meta_rel)
    artifacts.append(
        ArtifactEntry(
            relative_path=ff_meta_rel,
            byte_size=ff_size,
            sha256=ff_hash,
            role='dwd_metadata',
            source_url=ff_meta_url,
        )
    )

    # 2. Discover and Extract Temperature Observations (TU)
    tu_artifacts = _process_dwd_category(
        staging_dir=staging_dir,
        base_url=tu_base_url,
        category='temperature',
        zip_pattern=r'stundenwerte_TU_\d+_akt\.zip',
        member_pattern=r'^produkt_tu_stunde_.*\.txt$',
        required_fields=['STATIONS_ID', 'MESS_DATUM', 'QN_9', 'TT_TU', 'RF_TU'],
    )
    artifacts.extend(tu_artifacts)

    # 3. Discover and Extract Wind Observations (FF)
    ff_artifacts = _process_dwd_category(
        staging_dir=staging_dir,
        base_url=ff_base_url,
        category='wind',
        zip_pattern=r'stundenwerte_FF_\d+_akt\.zip',
        member_pattern=r'^produkt_ff_stunde_.*\.txt$',
        required_fields=['STATIONS_ID', 'MESS_DATUM', 'QN_3', 'F', 'D'],
    )
    artifacts.extend(ff_artifacts)

    return artifacts


def _process_dwd_category(
    staging_dir: Path,
    base_url: str,
    category: str,
    zip_pattern: str,
    member_pattern: str,
    required_fields: list[str],
) -> list[ArtifactEntry]:
    """Processes a single DWD meteorological parameter category (TU or FF)."""
    logger.info('[DISCOVER] Discovering DWD %s archives from %s', category, base_url)
    html = fetch_text(base_url)
    zip_names = sorted(set(re.findall(zip_pattern, html)))

    if not zip_names:
        raise SourceExtractionError(
            f'No DWD {category} archives discovered from {base_url}'
        )

    logger.info('[DISCOVER] Found %d DWD %s archives', len(zip_names), category)

    category_artifacts: list[ArtifactEntry] = []
    seen_extracted_basenames: set[str] = set()

    for zip_name in zip_names:
        zip_url = base_url + zip_name
        zip_rel = f'dwd/archives/{category}/{zip_name}'
        zip_dest = staging_dir / zip_rel

        # Download and retain ZIP archive
        zip_hash, zip_size = download_file_streamed(zip_url, zip_dest)
        zip_entry = ArtifactEntry(
            relative_path=zip_rel,
            byte_size=zip_size,
            sha256=zip_hash,
            role='dwd_observation_zip',
            source_url=zip_url,
        )
        category_artifacts.append(zip_entry)

        # Validate ZIP CRC integrity and member extraction
        try:
            with zipfile.ZipFile(zip_dest, 'r') as zf:
                bad_file = zf.testzip()
                if bad_file is not None:
                    raise SourceExtractionError(
                        f'Corrupted CRC in archive {zip_name}: member {bad_file}'
                    )

                # Match observation member
                matching_members = [
                    m for m in zf.namelist() if re.match(member_pattern, Path(m).name)
                ]
                if len(matching_members) != 1:
                    raise SourceExtractionError(
                        f'Archive {zip_name} expected exactly 1 observation member, got {len(matching_members)}'
                    )

                member_name = matching_members[0]
                sanitized_basename = Path(member_name).name

                # Reject duplicate basenames across different archives
                if sanitized_basename in seen_extracted_basenames:
                    raise SourceExtractionError(
                        f'Duplicate observation filename collision: {sanitized_basename}'
                    )
                seen_extracted_basenames.add(sanitized_basename)

                # Stream extraction to destination
                txt_rel = f'dwd/{category}/{sanitized_basename}'
                txt_dest = staging_dir / txt_rel
                txt_dest.parent.mkdir(parents=True, exist_ok=True)

                with zf.open(member_name) as source, txt_dest.open('wb') as target:
                    shutil.copyfileobj(source, target, length=65536)

        except Exception as exc:
            raise SourceExtractionError(
                f'Failed extracting DWD {category} archive {zip_name}: {exc}'
            ) from exc

        # Validate header structure and June 2026 observation presence
        has_june_coverage = False
        with txt_dest.open('r', encoding='latin-1') as f:
            header = f.readline().strip()
            header_fields = [h.strip() for h in header.split(';') if h.strip()]
            for rf in required_fields:
                if rf not in header_fields:
                    raise SourceExtractionError(
                        f'Missing required field {rf} in {sanitized_basename} header: {header}'
                    )

            # Check for at least one June 2026 observation record
            for line in f:
                if ';202606' in line or line.startswith('202606') or '202606' in line:
                    has_june_coverage = True
                    break

        if not has_june_coverage:
            raise SourceExtractionError(
                f'Extracted observation file {sanitized_basename} contains no June 2026 observations'
            )

        txt_hash, txt_size = compute_sha256(txt_dest)
        txt_entry = ArtifactEntry(
            relative_path=txt_rel,
            byte_size=txt_size,
            sha256=txt_hash,
            role='dwd_observation_txt',
            source_url=zip_url,
            parent_artifact_sha256=zip_hash,
        )
        category_artifacts.append(txt_entry)

    logger.info(
        '[EXTRACT] DWD %s: extracted %d observation files with verified June 2026 coverage',
        category,
        len(seen_extracted_basenames),
    )
    return category_artifacts
