import email.message
import io
import json
import urllib.error
import zipfile
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from zugzwang.ingestion.fetchers import (
    SourceExtractionError,
    _process_dwd_category,
    fetch_and_extract_stada_snapshot,
    fetch_piebro_railway_parquet,
)
from zugzwang.ingestion.http_client import DownloadError, download_file_streamed


def test_download_retry_exhaustion(tmp_path: Path) -> None:
    dest = tmp_path / 'failed.txt'
    hdrs = email.message.Message()
    with patch(
        'urllib.request.urlopen',
        side_effect=urllib.error.HTTPError('url', 500, 'Server Error', hdrs, None),
    ):
        with pytest.raises(DownloadError, match='Download failed after 3 attempts'):
            download_file_streamed(
                'https://example.com/test', dest, max_retries=3, backoff_factor=0.01
            )


def test_download_fatal_http_error(tmp_path: Path) -> None:
    dest = tmp_path / 'fatal.txt'
    hdrs = email.message.Message()
    with patch(
        'urllib.request.urlopen',
        side_effect=urllib.error.HTTPError('url', 404, 'Not Found', hdrs, None),
    ):
        with pytest.raises(DownloadError, match='Fatal HTTP 404'):
            download_file_streamed(
                'https://example.com/test', dest, max_retries=3, backoff_factor=0.01
            )


def test_fetch_piebro_railway_parquet(tmp_path: Path) -> None:
    # Create valid synthetic parquet with complete consumer schema and June 2026 timestamps
    staging = tmp_path / 'staging'
    staging.mkdir()
    rel_path = 'railway/data-2026-06.parquet'
    fixture_path = staging / rel_path
    fixture_path.parent.mkdir(parents=True)

    table = pa.Table.from_arrays(
        [
            pa.array(['ICE 101', 'ICE 102']),
            pa.array(['Frankfurt Hbf', 'Berlin Hbf']),
            pa.array(['8000105', '8000284']),
            pa.array(['101', '102']),
            pa.array(['1', '2']),
            pa.array(['Berlin Hbf', 'Munchen Hbf']),
            pa.array([2, 0]),
            pa.array(
                [1717200000000000000, 1717203600000000000]
            ),  # June 2026 timestamps
            pa.array([False, False]),
            pa.array([False, False]),
            pa.array(['ICE', 'ICE']),
            pa.array(['ride-1', 'ride-2']),
            pa.array([1, 2]),
            pa.array([1717200000000000000, 1717203600000000000]),
            pa.array([1717200000000000000, 1717203600000000000]),
            pa.array([1717200000000000000, 1717203600000000000]),
            pa.array([1717200000000000000, 1717203600000000000]),
            pa.array(['id-1', 'id-2']),
        ],
        names=[
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
        ],
    )
    pq.write_table(table, fixture_path)

    # Mock download to return existing fixture
    with patch('zugzwang.ingestion.fetchers.download_file_streamed') as mock_dl:
        mock_dl.return_value = ('e' * 64, fixture_path.stat().st_size)
        entry = fetch_piebro_railway_parquet(staging)

    assert entry.role == 'railway_parquet'
    assert entry.relative_path == rel_path
    assert entry.byte_size > 0


def test_fetch_and_extract_stada_snapshot(tmp_path: Path) -> None:
    staging = tmp_path / 'staging'
    staging.mkdir()
    raw_rel = 'stada/hour_00_19_20_21_22_23.parquet'
    raw_fixture = staging / raw_rel
    raw_fixture.parent.mkdir(parents=True)

    # Create synthetic StaDa raw parquet with 2 API calls, 1 duplicated station number
    st1 = {
        'number': 105,
        'name': 'Frankfurt(Main)Hbf',
        'evaNumbers': [{'number': 8000105}],
    }
    st2 = {'number': 284, 'name': 'Berlin Hbf', 'evaNumbers': [{'number': 8011160}]}
    # Duplicate station 105 in another response
    st1_dup = {
        'number': 105,
        'name': 'Frankfurt(Main)Hbf',
        'evaNumbers': [{'number': 8000105}],
    }

    payload1 = json.dumps({'result': [st1, st2]})
    payload2 = json.dumps({'result': [st1_dup]})
    unrelated_payload = json.dumps({'other': 'data'})

    table = pa.Table.from_arrays(
        [
            pa.array(
                [
                    'station-data/v2/stations',
                    'station-data/v2/stations',
                    'timetables/v1/plan',
                ]
            ),
            pa.array([200, 200, 200]),
            pa.array([payload1, payload2, unrelated_payload]),
        ],
        names=['api_name', 'status_code', 'response_data'],
    )
    pq.write_table(table, raw_fixture)

    with patch('zugzwang.ingestion.fetchers.download_file_streamed') as mock_dl:
        mock_dl.return_value = ('0' * 64, raw_fixture.stat().st_size)
        raw_entry, derived_entry = fetch_and_extract_stada_snapshot(staging)

    assert raw_entry.role == 'stada_raw_parquet'
    assert derived_entry.role == 'stada_json'
    assert derived_entry.parent_artifact_sha256 == raw_entry.sha256

    derived_file = staging / 'stada/stada_stations.json'
    assert derived_file.is_file()

    with derived_file.open('r') as f:
        extracted = json.load(f)
    assert len(extracted) == 2  # Deduplicated from 3 station items


def test_dwd_category_processing_and_zip_validation(tmp_path: Path) -> None:
    staging = tmp_path / 'staging'
    staging.mkdir()

    # Create a valid DWD zip archive with June 2026 observations
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, 'w') as zf:
        zf.writestr(
            'produkt_tu_stunde_20240101_20260630_00044.txt',
            'STATIONS_ID;MESS_DATUM;QN_9;TT_TU;RF_TU;\n00044;2026060100;3;15.2;75.0;\n',
        )
    valid_zip_data = zip_bytes.getvalue()

    html_index = (
        '<a href="stundenwerte_TU_00044_akt.zip">stundenwerte_TU_00044_akt.zip</a>'
    )

    with patch('zugzwang.ingestion.fetchers.fetch_text', return_value=html_index):
        with patch('zugzwang.ingestion.fetchers.download_file_streamed') as mock_dl:

            def fake_download(url, dest):
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(valid_zip_data)
                return 'a' * 64, len(valid_zip_data)

            mock_dl.side_effect = fake_download
            artifacts = _process_dwd_category(
                staging_dir=staging,
                base_url='https://opendata.dwd.de/tu/',
                category='temperature',
                zip_pattern=r'stundenwerte_TU_\d+_akt\.zip',
                member_pattern=r'^produkt_tu_stunde_.*\.txt$',
                required_fields=['STATIONS_ID', 'MESS_DATUM', 'QN_9', 'TT_TU', 'RF_TU'],
            )

    # 1 zip artifact + 1 extracted txt artifact
    assert len(artifacts) == 2
    assert artifacts[0].role == 'dwd_observation_zip'
    assert artifacts[1].role == 'dwd_observation_txt'
    assert (
        staging / 'dwd/temperature/produkt_tu_stunde_20240101_20260630_00044.txt'
    ).is_file()


def test_dwd_zip_duplicate_member_collision(tmp_path: Path) -> None:
    staging = tmp_path / 'staging'
    staging.mkdir()

    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, 'w') as zf:
        zf.writestr(
            'produkt_tu_stunde_collision.txt',
            'STATIONS_ID;MESS_DATUM;QN_9;TT_TU;RF_TU;\n00044;2026060100;3;15.2;75.0;\n',
        )
    dup_zip_data = zip_bytes.getvalue()

    html_index = (
        '<a href="stundenwerte_TU_00001_akt.zip">1</a>'
        '<a href="stundenwerte_TU_00002_akt.zip">2</a>'
    )

    with patch('zugzwang.ingestion.fetchers.fetch_text', return_value=html_index):
        with patch('zugzwang.ingestion.fetchers.download_file_streamed') as mock_dl:

            def fake_download(url, dest):
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(dup_zip_data)
                return 'a' * 64, len(dup_zip_data)

            mock_dl.side_effect = fake_download
            with pytest.raises(
                SourceExtractionError, match='Duplicate observation filename collision'
            ):
                _process_dwd_category(
                    staging_dir=staging,
                    base_url='https://opendata.dwd.de/tu/',
                    category='temperature',
                    zip_pattern=r'stundenwerte_TU_\d+_akt\.zip',
                    member_pattern=r'^produkt_tu_stunde_.*\.txt$',
                    required_fields=[
                        'STATIONS_ID',
                        'MESS_DATUM',
                        'QN_9',
                        'TT_TU',
                        'RF_TU',
                    ],
                )
