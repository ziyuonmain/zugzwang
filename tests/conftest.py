"""Pytest configuration and shared fixtures for Zugzwang test suite."""

import os
import shutil
import time

import pytest
from pyspark.sql import SparkSession

# Force UTC timezone for Python driver runtime to match Spark session timezone
os.environ['TZ'] = 'UTC'
time.tzset()


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    """Provides a local SparkSession configured for deterministic unit testing.

    Skips tests gracefully if Java runtime is not available on the host.
    """
    if shutil.which('java') is None and 'JAVA_HOME' not in os.environ:
        pytest.skip(
            'Java 17+ is not installed/configured on host; skipping PySpark tests.'
        )

    try:
        return (
            SparkSession.builder.master('local[2]')
            .appName('zugzwang-unit-tests')
            .config('spark.sql.session.timeZone', 'UTC')
            .config('spark.ui.enabled', 'false')
            .config('spark.sql.shuffle.partitions', '2')
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f'Unable to initialize local PySpark session: {exc}')
