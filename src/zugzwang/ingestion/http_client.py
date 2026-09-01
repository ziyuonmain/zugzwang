"""Resilient HTTP client for streamed downloads with retries and backoff."""

import hashlib
import logging
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)


class DownloadError(Exception):
    """Raised when an HTTP download fails permanently."""


def download_file_streamed(
    url: str,
    destination_path: Path | str,
    connect_timeout: int = 10,
    read_timeout: int = 30,
    max_retries: int = 3,
    backoff_factor: float = 1.0,
    chunk_size: int = 65536,
) -> tuple[str, int]:
    """Streams a remote URL directly to disk with retries and on-the-fly hashing.

    Args:
        url: Remote URL to fetch.
        destination_path: Local target file path.
        connect_timeout: Socket connect timeout in seconds.
        read_timeout: Socket read timeout in seconds.
        max_retries: Maximum retry attempts on transient network or 5xx failures.
        backoff_factor: Multiplier for exponential backoff between retries.
        chunk_size: Size of data chunks to stream into memory and disk.

    Returns:
        A tuple of (sha256_hex_lowercase, total_bytes_downloaded).

    Raises:
        DownloadError: If download fails after max_retries or encounters fatal HTTP status.
    """
    dest = Path(destination_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    temp_file = dest.parent / f'.tmp.{dest.name}.{uuid.uuid4().hex}'

    headers = {'User-Agent': 'zugzwang-ingestion/0.1.0'}
    req = urllib.request.Request(url, headers=headers)

    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        hasher = hashlib.sha256()
        total_bytes = 0

        try:
            # Connect with combined timeout
            timeout = connect_timeout + read_timeout
            with urllib.request.urlopen(req, timeout=timeout) as response:
                status = getattr(response, 'status', 200)
                if status != 200:
                    raise DownloadError(f'HTTP {status} received from {url}')

                with temp_file.open('wb') as f:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        hasher.update(chunk)
                        f.write(chunk)
                        total_bytes += len(chunk)

            # Atomic replace into final destination
            temp_file.replace(dest)
            sha256_hex = hasher.hexdigest().lower()
            return sha256_hex, total_bytes

        except urllib.error.HTTPError as exc:
            last_error = exc
            if 400 <= exc.code < 500 and exc.code not in (408, 429):
                # Fatal client error (e.g. 404 Not Found, 403 Forbidden)
                _cleanup_temp_file(temp_file)
                raise DownloadError(
                    f'Fatal HTTP {exc.code} for {url}: {exc.reason}'
                ) from exc

            logger.warning(
                'Transient HTTP error %d on attempt %d/%d for %s: %s',
                exc.code,
                attempt,
                max_retries,
                url,
                exc.reason,
            )
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            last_error = exc
            logger.warning(
                'Network error on attempt %d/%d for %s: %s',
                attempt,
                max_retries,
                url,
                exc,
            )
        finally:
            _cleanup_temp_file(temp_file)

        if attempt < max_retries:
            sleep_time = backoff_factor * (2 ** (attempt - 1))
            time.sleep(sleep_time)

    raise DownloadError(
        f'Download failed after {max_retries} attempts for {url}: {last_error}'
    ) from last_error


def fetch_text(
    url: str,
    timeout: int = 15,
    max_retries: int = 3,
    backoff_factor: float = 1.0,
) -> str:
    """Fetches text content from a remote URL.

    Args:
        url: Remote URL to fetch.
        timeout: Socket timeout in seconds.
        max_retries: Maximum retry attempts on transient errors.
        backoff_factor: Multiplier for exponential backoff.

    Returns:
        The decoded text content string.

    Raises:
        DownloadError: If request fails after max_retries.
    """
    headers = {'User-Agent': 'zugzwang-ingestion/0.1.0'}
    req = urllib.request.Request(url, headers=headers)

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                status = getattr(response, 'status', 200)
                if status != 200:
                    raise DownloadError(f'HTTP {status} received from {url}')
                raw_bytes = response.read()
                # DWD files are often latin-1 or utf-8
                try:
                    return raw_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    return raw_bytes.decode('latin-1')
        except urllib.error.HTTPError as exc:
            last_error = exc
            if 400 <= exc.code < 500 and exc.code not in (408, 429):
                raise DownloadError(
                    f'Fatal HTTP {exc.code} for {url}: {exc.reason}'
                ) from exc
            logger.warning(
                'HTTP error on text fetch attempt %d/%d: %s', attempt, max_retries, exc
            )
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            last_error = exc
            logger.warning(
                'Network error on text fetch attempt %d/%d: %s',
                attempt,
                max_retries,
                exc,
            )

        if attempt < max_retries:
            time.sleep(backoff_factor * (2 ** (attempt - 1)))

    raise DownloadError(
        f'Failed to fetch text from {url}: {last_error}'
    ) from last_error


def _cleanup_temp_file(temp_file: Path) -> None:
    """Safely removes temporary file if it exists."""
    try:
        if temp_file.exists():
            temp_file.unlink()
    except OSError:
        pass
