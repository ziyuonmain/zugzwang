"""Configuration and path management for Zugzwang data pipelines."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class VolumePaths:
    """Encapsulates landing volume file paths for the June 2026 vertical slice."""

    stada_json_path: str
    dwd_tu_meta_path: str
    dwd_ff_meta_path: str
    dwd_tu_data_path: str
    dwd_ff_data_path: str
    railway_parquet_path: str


def get_volume_paths(base_path: str | None = None) -> VolumePaths:
    """Builds default volume landing paths.

    Args:
        base_path: Root URI of the raw landing volume. Defaults to environment
            variable ZUGZWANG_VOLUME_BASE_PATH or '/Volumes/zugzwang/raw/landing'.

    Returns:
        VolumePaths instance containing absolute URIs for each source dataset.
    """
    if base_path is None:
        base_path = os.getenv(
            'ZUGZWANG_VOLUME_BASE_PATH', '/Volumes/zugzwang/raw/landing'
        )

    base = base_path.rstrip('/')
    return VolumePaths(
        stada_json_path=f'{base}/stada/stada_stations.json',
        dwd_tu_meta_path=f'{base}/dwd/TU_Stundenwerte_Beschreibung_Stationen.txt',
        dwd_ff_meta_path=f'{base}/dwd/FF_Stundenwerte_Beschreibung_Stationen.txt',
        dwd_tu_data_path=f'{base}/dwd/extracted/tu/*.txt',
        dwd_ff_data_path=f'{base}/dwd/extracted/ff/*.txt',
        railway_parquet_path=f'{base}/railway/data-2026-06.parquet',
    )
