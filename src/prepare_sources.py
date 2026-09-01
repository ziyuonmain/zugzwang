"""Task script entrypoint for automated source ingestion (Lakeflow Job Task 1)."""

import argparse
import logging
import os

from zugzwang.ingestion.snapshot import prepare_june_2026_snapshot


def main() -> None:
    """CLI entrypoint for source preparation task.

    Raises:
        Exception: If snapshot preparation or validation fails.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    logger = logging.getLogger('prepare_sources')

    parser = argparse.ArgumentParser(
        description='Prepare and validate raw source snapshot for Zugzwang.'
    )
    parser.add_argument(
        '--volume-base-path',
        type=str,
        default=os.getenv(
            'ZUGZWANG_VOLUME_BASE_PATH', '/Volumes/zugzwang_dev/raw/landing'
        ),
        help='Target base directory in Unity Catalog Volume where sources will land.',
    )

    args = parser.parse_args()
    target_path = args.volume_base_path.rstrip('/')

    logger.info('Starting source preparation task for snapshot 2026-06')
    logger.info('Target landing path: %s', target_path)

    outcome = prepare_june_2026_snapshot(target_dir=target_path)
    logger.info(
        'Source preparation task completed successfully with outcome: %s', outcome
    )


if __name__ == '__main__':
    main()
