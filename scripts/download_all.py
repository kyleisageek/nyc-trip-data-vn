"""Download all available NYC TLC parquet files locally with rate-limit handling.

Downloads newest-first so you get the most recent data quickly.
Resumes where it left off — skips files already downloaded.

Usage:
    python -m scripts.download_all                     # All types
    python -m scripts.download_all --types yellow      # Just yellow
    python -m scripts.download_all --types green fhvhv # Green + FHVHV
    python -m scripts.download_all --delay 30          # 30s between files
    python -m scripts.download_all --start 2020-01     # Only 2020-01 onward
"""

import argparse
import logging
import time
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
OUTPUT_DIR = Path("data/parquet")

# Type -> (start_year, start_month)
TYPE_RANGES = {
    "yellow": (2009, 1),
    "green": (2014, 1),
    "fhvhv": (2019, 2),
}

MAX_RETRIES = 5
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


def generate_months(start_year: int, start_month: int, end: str = "2026-03") -> list[str]:
    """Generate YYYY-MM from start to end (default: 2026-03, latest known available)."""
    end_year, end_month = int(end.split("-")[0]), int(end.split("-")[1])
    months = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append(f"{year}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


def download_file(url: str, dest: Path, delay_on_403: int = 60) -> bool:
    """Download with exponential backoff on 403/429.

    Returns True on success, False on failure/not-found.
    """
    for attempt in range(MAX_RETRIES):
        try:
            # Support resume via Range header
            headers = {}
            existing_size = dest.stat().st_size if dest.exists() else 0
            if existing_size > 0:
                headers["Range"] = f"bytes={existing_size}-"

            resp = requests.get(url, stream=True, timeout=300, headers=headers)

            if resp.status_code == 403:
                # Check if response body indicates "not found" vs rate limit
                # CloudFront 403 with XML body = file doesn't exist
                # CloudFront 403 without = rate limited
                content_type = resp.headers.get("Content-Type", "")
                if "xml" in content_type and attempt == 0:
                    # Try once more after a delay to distinguish rate limit from not-found
                    wait = delay_on_403
                    logger.info("  403 — waiting %ds to confirm (attempt %d/%d)...",
                                wait, attempt + 1, MAX_RETRIES)
                    time.sleep(wait)
                    continue
                elif "xml" in content_type and attempt >= 1:
                    # Likely genuinely not available
                    logger.info("  Not available (confirmed 403)")
                    return False
                else:
                    wait = delay_on_403 * (2 ** attempt)
                    logger.warning("  403 rate-limited, waiting %ds (attempt %d/%d)...",
                                  wait, attempt + 1, MAX_RETRIES)
                    time.sleep(wait)
                    continue

            if resp.status_code == 404:
                logger.info("  Not found (404)")
                return False

            if resp.status_code == 416:
                # Range not satisfiable — file already complete
                logger.info("  Already complete (%d bytes)", existing_size)
                return True

            resp.raise_for_status()

            mode = "ab" if resp.status_code == 206 else "wb"
            with open(dest, mode) as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)

            logger.info("  Downloaded: %s (%.1f MB)", dest.name, dest.stat().st_size / 1024 / 1024)
            return True

        except (requests.RequestException, IOError) as e:
            wait = 30 * (2 ** attempt)
            logger.warning("  Error: %s, retrying in %ds...", e, wait)
            time.sleep(wait)

    logger.error("  Failed after %d attempts: %s", MAX_RETRIES, url)
    return False


def main():
    parser = argparse.ArgumentParser(description="Download all NYC TLC parquet files")
    parser.add_argument("--types", nargs="+", default=["yellow", "green", "fhvhv"],
                        help="Taxi types to download")
    parser.add_argument("--delay", type=int, default=10,
                        help="Seconds to wait between downloads (default: 10)")
    parser.add_argument("--start", type=str, default=None,
                        help="Start month YYYY-MM (download only this month onward)")
    parser.add_argument("--output", type=str, default=str(OUTPUT_DIR),
                        help="Output directory (default: data/parquet)")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build file list (newest first)
    files_to_download = []
    for taxi_type in args.types:
        if taxi_type not in TYPE_RANGES:
            logger.error("Unknown type: %s", taxi_type)
            continue
        start_year, start_month = TYPE_RANGES[taxi_type]
        if args.start:
            sy, sm = args.start.split("-")
            start_year, start_month = max(start_year, int(sy)), int(sm)

        months = generate_months(start_year, start_month)
        for ym in months:
            filename = f"{taxi_type}_tripdata_{ym}.parquet"
            files_to_download.append({
                "taxi_type": taxi_type,
                "month": ym,
                "filename": filename,
                "url": f"{BASE_URL}/{filename}",
            })

    # Sort newest first
    files_to_download.sort(key=lambda x: x["month"], reverse=True)

    # Skip already-downloaded files
    to_download = []
    skipped = 0
    for f in files_to_download:
        dest = output_dir / f["filename"]
        if dest.exists() and dest.stat().st_size > 1000:
            skipped += 1
        else:
            to_download.append(f)

    logger.info("Files to download: %d (skipping %d already downloaded)", len(to_download), skipped)
    logger.info("Output directory: %s", output_dir.resolve())
    logger.info("Delay between files: %ds", args.delay)

    # Download
    succeeded = 0
    failed = 0
    total_bytes = 0
    start_time = time.time()

    for i, f in enumerate(to_download):
        dest = output_dir / f["filename"]
        logger.info("[%d/%d] %s", i + 1, len(to_download), f["filename"])

        if download_file(f["url"], dest, delay_on_403=args.delay * 6):
            succeeded += 1
            if dest.exists():
                total_bytes += dest.stat().st_size
        else:
            failed += 1

        # Status display
        elapsed = time.time() - start_time
        done = succeeded + failed
        remaining = len(to_download) - done
        dl_gb = total_bytes / (1024 ** 3)
        # Count all files in output dir for total picture
        all_files = list(output_dir.glob("*.parquet"))
        disk_gb = sum(p.stat().st_size for p in all_files) / (1024 ** 3)
        logger.info(
            "  STATUS: %d/%d done (%d ok, %d fail) | %.2f GB this session | "
            "%.2f GB on disk (%d files) | %d remaining",
            done, len(to_download), succeeded, failed, dl_gb,
            disk_gb, len(all_files), remaining,
        )

        # Delay between downloads
        if i < len(to_download) - 1:
            logger.info("  Waiting %ds...", args.delay)
            time.sleep(args.delay)

    elapsed = time.time() - start_time
    logger.info("Done in %.0fs. Downloaded: %d (%.2f GB), Failed: %d, Skipped: %d",
                elapsed, succeeded, total_bytes / (1024**3), failed, skipped)


if __name__ == "__main__":
    main()
