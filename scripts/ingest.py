"""Main orchestrator for the NYC TLC trip data pipeline.

Usage:
    python -m scripts.ingest                # Full pipeline
    python -m scripts.ingest --dry-run      # Discover only, no downloads
    python -m scripts.ingest --types green  # Only process green taxi data
    python -m scripts.ingest --reprocess    # Re-download and re-register all files
"""

import argparse
import logging
import sys
import tempfile
import time
from pathlib import Path

import requests

from .config import load_config
from .discover import discover_available
from .r2_client import R2Client
from . import iceberg_register

logger = logging.getLogger(__name__)

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB download chunks
MAX_RETRIES = 4
RETRY_BACKOFF = 15  # seconds, doubles each retry
DELAY_BETWEEN_DOWNLOADS = 5  # seconds between files


def _download(url: str, dest: Path) -> None:
    """Stream-download a file to disk with retry and backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            logger.info("Downloading %s (attempt %d/%d)", url, attempt + 1, MAX_RETRIES)
            with requests.get(url, stream=True, timeout=300) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        f.write(chunk)
            logger.info("Downloaded to %s (%d bytes)", dest, dest.stat().st_size)
            return
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 403 and attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF * (2 ** attempt)
                logger.warning("Got 403, retrying in %ds...", wait)
                if dest.exists():
                    dest.unlink()
                time.sleep(wait)
            else:
                raise


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NYC TLC trip data pipeline")
    parser.add_argument("--dry-run", action="store_true",
                        help="Discover available files but don't download or upload")
    parser.add_argument("--types", nargs="+", default=None,
                        help="Taxi types to process (default: all from config)")
    parser.add_argument("--reprocess", action="store_true",
                        help="Re-download and re-register all files, even if already in R2")
    parser.add_argument("--config", default=None,
                        help="Path to config.yaml (default: project root)")
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    args = _parse_args()
    config = load_config(args.config)

    # Override taxi types if specified
    if args.types:
        config["tlc"]["taxi_types"] = args.types

    # Step 1: Discover available files
    available = discover_available(config)
    if not available:
        logger.info("No available files found")
        return

    logger.info("Available files:")
    for item in available:
        size_mb = item["size_bytes"] / (1024 * 1024) if item.get("size_bytes") else 0
        logger.info("  %s (%.1f MB)", item["filename"], size_mb)

    if args.dry_run:
        logger.info("Dry run — exiting without download/upload")
        return

    # Step 2: Check what's already in R2
    r2 = R2Client(config)
    existing = r2.list_existing()

    if args.reprocess:
        new_files = available
        logger.info("Reprocess mode — processing all %d files", len(new_files))
    else:
        new_files = [f for f in available if f["filename"] not in existing]
        if not new_files:
            logger.info("All files already in R2 — nothing to do")
            return
        logger.info("%d new files to process", len(new_files))

    # Step 3: Set up Iceberg registrar if configured
    ice_registrar = None
    if iceberg_register.is_configured(config):
        try:
            ice_registrar = iceberg_register.IcebergRegistrar(config)
            logger.info("Iceberg registration enabled")
        except Exception as e:
            logger.warning("Iceberg catalog unavailable, skipping registration: %s", e)

    # Step 4: Process each new file
    tmp_dir = Path(tempfile.mkdtemp(prefix="tlc_"))
    processed = 0
    try:
        for item in new_files:
            filename = item["filename"]
            taxi_type = item["taxi_type"]
            local_path = tmp_dir / filename

            try:
                # Delay between downloads to avoid rate limiting
                if processed > 0:
                    logger.info("Waiting %ds before next download...", DELAY_BETWEEN_DOWNLOADS)
                    time.sleep(DELAY_BETWEEN_DOWNLOADS)

                # Download
                _download(item["url"], local_path)

                # Upload to R2
                r2.upload(local_path, filename)

                # Register in Iceberg
                if ice_registrar:
                    try:
                        ice_registrar.register(local_path, taxi_type)
                    except Exception as e:
                        logger.error("Iceberg registration failed for %s: %s", filename, e)

                processed += 1

            except Exception as e:
                logger.error("Failed to process %s: %s", filename, e)
            finally:
                # Cleanup temp file
                if local_path.exists():
                    local_path.unlink()
                    logger.debug("Cleaned up %s", local_path)
    finally:
        # Remove temp directory
        try:
            tmp_dir.rmdir()
        except OSError:
            pass

    logger.info("Pipeline complete")


if __name__ == "__main__":
    main()
