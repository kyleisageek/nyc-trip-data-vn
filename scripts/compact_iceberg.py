"""Compact Iceberg tables by rewriting small data files into larger ones.

PyIceberg doesn't have native rewrite_data_files, so this reads the full
table (or a year slice) and overwrites it with fewer, larger files.

Usage:
    python -m scripts.compact_iceberg                    # Compact all tables
    python -m scripts.compact_iceberg --tables green     # Just green (recommended first)
    python -m scripts.compact_iceberg --tables yellow --by-year  # Yellow chunked by year
    python -m scripts.compact_iceberg --dry-run          # Show file counts only
"""

import argparse
import logging
import sys
import time

import pyarrow as pa
from pyiceberg.expressions import GreaterThanOrEqual, LessThan, And

from .config import load_config
from . import iceberg_register
from . import schemas as s

logger = logging.getLogger(__name__)

TABLE_TIMESTAMP_COLS = {
    "yellow": "tpep_pickup_datetime",
    "green": "lpep_pickup_datetime",
    "fhvhv": "pickup_datetime",
}


def _table_stats(table) -> dict:
    """Get data file count and total records from current snapshot."""
    snapshot = table.current_snapshot()
    if snapshot is None:
        return {"files": 0, "records": 0}
    manifest_list = snapshot.manifests(table.io)
    file_count = 0
    record_count = 0
    for manifest in manifest_list:
        for entry in manifest.fetch_manifest_entry(table.io):
            file_count += 1
            record_count += entry.data_file.record_count
    return {"files": file_count, "records": record_count}


def _compact_whole(table, taxi_type: str) -> None:
    """Read entire table and overwrite with fewer files."""
    logger.info("Reading entire %s table...", taxi_type)
    arrow_table = table.scan().to_arrow()
    row_count = len(arrow_table)
    logger.info("Read %d rows, overwriting table...", row_count)

    target_schema = s.SCHEMAS[taxi_type]
    arrow_table = s.align_table_to_schema(arrow_table, target_schema)
    table.overwrite(arrow_table)
    logger.info("Compaction complete for %s (%d rows rewritten)", taxi_type, row_count)


def _compact_by_year(table, taxi_type: str, start_year: int, end_year: int) -> None:
    """Compact table one year at a time to limit memory usage."""
    ts_col = TABLE_TIMESTAMP_COLS[taxi_type]
    target_schema = s.SCHEMAS[taxi_type]

    for year in range(start_year, end_year + 1):
        start_ts = f"{year}-01-01T00:00:00"
        end_ts = f"{year + 1}-01-01T00:00:00"

        logger.info("Compacting %s year %d...", taxi_type, year)
        row_filter = And(
            GreaterThanOrEqual(ts_col, start_ts),
            LessThan(ts_col, end_ts),
        )

        try:
            arrow_table = table.scan(row_filter=row_filter).to_arrow()
            if len(arrow_table) == 0:
                logger.info("  No data for %d, skipping", year)
                continue

            arrow_table = s.align_table_to_schema(arrow_table, target_schema)
            table.overwrite(arrow_table, overwrite_filter=row_filter)
            logger.info("  Rewrote %d rows for %d", len(arrow_table), year)
        except Exception as e:
            logger.error("  Failed to compact %s year %d: %s", taxi_type, year, e)


YEAR_RANGES = {
    "yellow": (2009, 2026),
    "green": (2014, 2026),
    "fhvhv": (2019, 2026),
}


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Compact Iceberg tables")
    parser.add_argument("--tables", nargs="+", default=["green", "yellow", "fhvhv"],
                        help="Tables to compact (default: all)")
    parser.add_argument("--by-year", action="store_true",
                        help="Compact year-by-year (lower memory, needed for large tables)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show file counts without compacting")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    if not iceberg_register.is_configured(config):
        logger.error("Iceberg catalog not configured")
        sys.exit(1)

    reg = iceberg_register.IcebergRegistrar(config)

    for taxi_type in args.tables:
        table_name = f"{reg._namespace}.{taxi_type}_tripdata"
        try:
            table = reg._catalog.load_table(table_name)
        except Exception as e:
            logger.warning("Could not load table %s: %s", table_name, e)
            continue

        stats = _table_stats(table)
        logger.info("%s: %d data files, %d records",
                    table_name, stats["files"], stats["records"])

        if args.dry_run:
            continue

        start = time.time()
        if args.by_year:
            sy, ey = YEAR_RANGES.get(taxi_type, (2009, 2026))
            _compact_by_year(table, taxi_type, sy, ey)
        else:
            _compact_whole(table, taxi_type)

        new_stats = _table_stats(table)
        elapsed = time.time() - start
        logger.info("%s compacted: %d -> %d files in %.0fs",
                    table_name, stats["files"], new_stats["files"], elapsed)


if __name__ == "__main__":
    main()
