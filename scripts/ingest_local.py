"""Upload local parquet files to R2 and register in Iceberg.

Reads from a local directory (e.g., Taxi/) instead of downloading from
CloudFront.  Skips files already in R2.

Usage:
    python -m scripts.ingest_local Taxi/
    python -m scripts.ingest_local Taxi/ --types yellow green
    python -m scripts.ingest_local Taxi/ --dry-run
    python -m scripts.ingest_local Taxi/ --skip-iceberg
    python -m scripts.ingest_local Taxi/ --oldest-first
    python -m scripts.ingest_local Taxi/ --iceberg-only  # Skip R2, register in Iceberg only
"""

import argparse
import logging
import sys
import time
from pathlib import Path

from .config import load_config
from .r2_client import R2Client
from . import iceberg_register

logger = logging.getLogger(__name__)

VALID_TYPES = {"yellow", "green", "fhvhv"}


def _parse_filename(filename: str) -> tuple[str, str] | None:
    """Extract (taxi_type, YYYY-MM) from a filename like yellow_tripdata_2024-01.parquet."""
    if not filename.endswith(".parquet") or "_tripdata_" not in filename:
        return None
    stem = filename.removesuffix(".parquet")
    parts = stem.split("_tripdata_")
    if len(parts) != 2:
        return None
    taxi_type, month = parts
    if taxi_type not in VALID_TYPES:
        return None
    return taxi_type, month


def _scan_local(local_dir: Path, types: list[str] | None) -> list[dict]:
    """Scan a local directory for parquet files, return sorted list."""
    files = []
    for p in sorted(local_dir.glob("*_tripdata_*.parquet")):
        parsed = _parse_filename(p.name)
        if parsed is None:
            continue
        taxi_type, month = parsed
        if types and taxi_type not in types:
            continue
        files.append({
            "taxi_type": taxi_type,
            "month": month,
            "filename": p.name,
            "local_path": p,
            "size_bytes": p.stat().st_size,
        })
    return files


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Upload local parquet files to R2 + Iceberg")
    parser.add_argument("local_dir", type=Path, help="Directory containing parquet files")
    parser.add_argument("--types", nargs="+", default=None,
                        help="Taxi types to process (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be uploaded without doing it")
    parser.add_argument("--skip-iceberg", action="store_true",
                        help="Upload to R2 only, skip Iceberg registration")
    parser.add_argument("--oldest-first", action="store_true",
                        help="Process oldest files first (default: newest first)")
    parser.add_argument("--iceberg-only", action="store_true",
                        help="Skip R2 upload, only register in Iceberg")
    parser.add_argument("--config", default=None,
                        help="Path to config.yaml")
    args = parser.parse_args()

    if not args.local_dir.is_dir():
        logger.error("Not a directory: %s", args.local_dir)
        sys.exit(1)

    # Scan local files
    local_files = _scan_local(args.local_dir, args.types)
    if not local_files:
        logger.info("No matching parquet files found in %s", args.local_dir)
        return

    # Sort order
    local_files.sort(key=lambda x: (x["month"], x["taxi_type"]),
                     reverse=not args.oldest_first)

    logger.info("Found %d local parquet files in %s", len(local_files), args.local_dir)

    # Load config
    config = load_config(args.config)

    if args.iceberg_only:
        # Iceberg-only mode: process all local files
        to_process = local_files
        skipped = 0
        r2 = None
    else:
        # Check R2 for existing files
        r2 = R2Client(config)
        existing = r2.list_existing()
        to_process = [f for f in local_files if f["filename"] not in existing]
        skipped = len(local_files) - len(to_process)

    total_bytes = sum(f["size_bytes"] for f in to_process)
    logger.info("To process: %d files (%.2f GB) — skipping %d already done",
                len(to_process), total_bytes / (1024 ** 3), skipped)

    if args.dry_run:
        for f in to_process:
            logger.info("  WOULD PROCESS: %s (%.1f MB)",
                        f["filename"], f["size_bytes"] / (1024 ** 2))
        return

    # Set up Iceberg registrar
    ice_registrar = None
    if not args.skip_iceberg and iceberg_register.is_configured(config):
        try:
            ice_registrar = iceberg_register.IcebergRegistrar(config)
            logger.info("Iceberg registration enabled")
        except Exception as e:
            logger.warning("Iceberg catalog unavailable, skipping registration: %s", e)

    if args.iceberg_only and not ice_registrar:
        logger.error("--iceberg-only specified but Iceberg catalog is not available")
        sys.exit(1)

    # Process files
    succeeded = 0
    failed = 0
    bytes_uploaded = 0
    start_time = time.time()

    for i, f in enumerate(to_process):
        filename = f["filename"]
        local_path = f["local_path"]
        taxi_type = f["taxi_type"]

        logger.info("[%d/%d] %s (%.1f MB)",
                    i + 1, len(to_process), filename,
                    f["size_bytes"] / (1024 ** 2))

        try:
            # Upload to R2 (unless iceberg-only mode)
            if r2 and not args.iceberg_only:
                r2.upload(local_path, filename)
                bytes_uploaded += f["size_bytes"]

            # Register in Iceberg
            if ice_registrar:
                try:
                    ice_registrar.register(local_path, taxi_type)
                except Exception as e:
                    logger.error("Iceberg registration failed for %s: %s", filename, e)

            succeeded += 1

        except Exception as e:
            logger.error("Failed to process %s: %s", filename, e)
            failed += 1

        # Progress
        elapsed = time.time() - start_time
        done = succeeded + failed
        remaining = len(to_process) - done
        logger.info("  PROGRESS: %d/%d done (%d ok, %d fail) | %.2f GB uploaded | %d remaining",
                    done, len(to_process), succeeded, failed,
                    bytes_uploaded / (1024 ** 3), remaining)

    elapsed = time.time() - start_time
    logger.info("Complete in %.0fs. Uploaded: %d (%.2f GB), Failed: %d, Skipped: %d",
                elapsed, succeeded, bytes_uploaded / (1024 ** 3), failed, skipped)


if __name__ == "__main__":
    main()
