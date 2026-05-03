"""Bulk-register local parquet files into Iceberg, grouped by type+year.

Cloudflare R2's Iceberg catalog caps snapshots at ~38-50.  Each append
creates a snapshot, so we must keep total appends per table under that
limit.  This script groups source files by (taxi_type, year) and does
one append per group — e.g. 18 years of yellow = 18 appends.

Usage:
    python -m scripts.iceberg_bulk_register Taxi/
    python -m scripts.iceberg_bulk_register Taxi/ --types green
    python -m scripts.iceberg_bulk_register Taxi/ --dry-run
"""

import argparse
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .config import load_config
from . import iceberg_register
from . import schemas as s

logger = logging.getLogger(__name__)

VALID_TYPES = {"yellow", "green", "fhvhv"}


def _parse_filename(filename: str):
    if not filename.endswith(".parquet") or "_tripdata_" not in filename:
        return None
    stem = filename.removesuffix(".parquet")
    parts = stem.split("_tripdata_")
    if len(parts) != 2:
        return None
    taxi_type, month = parts
    if taxi_type not in VALID_TYPES:
        return None
    year = month.split("-")[0]
    return taxi_type, year, month


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Bulk register into Iceberg by year")
    parser.add_argument("local_dir", type=Path)
    parser.add_argument("--types", nargs="+", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    if not args.local_dir.is_dir():
        logger.error("Not a directory: %s", args.local_dir)
        sys.exit(1)

    # Group files by (taxi_type, year)
    groups = defaultdict(list)
    for p in sorted(args.local_dir.glob("*_tripdata_*.parquet")):
        parsed = _parse_filename(p.name)
        if parsed is None:
            continue
        taxi_type, year, month = parsed
        if args.types and taxi_type not in args.types:
            continue
        groups[(taxi_type, year)].append(p)

    # Sort groups by type then year
    sorted_groups = sorted(groups.items())

    # Count appends per table
    appends_per_type = defaultdict(int)
    for (taxi_type, year), files in sorted_groups:
        appends_per_type[taxi_type] += 1

    logger.info("Plan: %d groups across %d tables", len(sorted_groups), len(appends_per_type))
    for t, count in sorted(appends_per_type.items()):
        logger.info("  %s: %d appends (one per year)", t, count)

    if args.dry_run:
        for (taxi_type, year), files in sorted_groups:
            total_size = sum(f.stat().st_size for f in files)
            logger.info("  %s %s: %d files, %.1f MB",
                        taxi_type, year, len(files), total_size / (1024 ** 2))
        return

    # Connect to Iceberg
    config = load_config(args.config)
    if not iceberg_register.is_configured(config):
        logger.error("Iceberg not configured")
        sys.exit(1)

    reg = iceberg_register.IcebergRegistrar(config)

    succeeded = 0
    failed = 0
    total_rows = 0
    start_time = time.time()

    for i, ((taxi_type, year), files) in enumerate(sorted_groups):
        logger.info("[%d/%d] %s %s (%d files)",
                    i + 1, len(sorted_groups), taxi_type, year, len(files))

        try:
            # Read and concatenate all files for this group
            tables = []
            group_rows = 0
            target_schema = s.SCHEMAS[taxi_type]

            for f in files:
                arrow = pq.read_table(str(f))
                arrow = s.align_table_to_schema(arrow, target_schema)
                group_rows += len(arrow)
                tables.append(arrow)
                logger.info("    Read %s (%d rows)", f.name, len(arrow))

            combined = pa.concat_tables(tables)
            del tables  # free memory

            logger.info("  Appending %d rows for %s %s...", group_rows, taxi_type, year)

            table = reg._ensure_table(taxi_type)
            reg._append_with_retry(table, combined)
            del combined  # free memory

            total_rows += group_rows
            succeeded += 1
            logger.info("  Committed. Total rows so far: %d", total_rows)

        except Exception as e:
            logger.error("  FAILED %s %s: %s", taxi_type, year, e)
            failed += 1

    elapsed = time.time() - start_time
    logger.info("Done in %.0fs. Groups: %d ok, %d fail. Total rows: %d",
                elapsed, succeeded, failed, total_rows)


if __name__ == "__main__":
    main()
