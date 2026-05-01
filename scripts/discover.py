"""Discover available NYC TLC trip data files via HEAD requests."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import requests

logger = logging.getLogger(__name__)


def _build_urls(base_url: str, taxi_types: list[str], months: list[str]) -> list[dict]:
    """Build list of URL candidates to probe."""
    candidates = []
    for taxi_type in taxi_types:
        for month in months:
            filename = f"{taxi_type}_tripdata_{month}.parquet"
            url = f"{base_url}/{filename}"
            candidates.append({
                "taxi_type": taxi_type,
                "month": month,
                "filename": filename,
                "url": url,
            })
    return candidates


def _generate_months(lookback: int) -> list[str]:
    """Generate YYYY-MM strings going back `lookback` months from today."""
    today = date.today()
    months = []
    for i in range(lookback):
        d = today.replace(day=1) - timedelta(days=i * 28)
        # Normalize to first of month to avoid day overflow issues
        ym = d.strftime("%Y-%m")
        if ym not in months:
            months.append(ym)
    return sorted(months)


def _probe_url(candidate: dict, timeout: int = 10) -> dict | None:
    """Send HEAD request; return candidate dict if status 200, else None."""
    try:
        resp = requests.head(candidate["url"], timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            content_length = resp.headers.get("Content-Length")
            candidate["size_bytes"] = int(content_length) if content_length else None
            logger.info("Available: %s (%s bytes)", candidate["filename"],
                        candidate.get("size_bytes", "unknown"))
            return candidate
        logger.debug("Not available (HTTP %d): %s", resp.status_code, candidate["filename"])
    except requests.RequestException as e:
        logger.warning("Error probing %s: %s", candidate["filename"], e)
    return None


def discover_available(config: dict, max_workers: int = 10) -> list[dict]:
    """Discover all available TLC parquet files.

    Returns list of dicts with keys: taxi_type, month, filename, url, size_bytes.
    """
    tlc = config["tlc"]
    base_url = tlc["base_url"]
    taxi_types = tlc["taxi_types"]
    lookback = tlc.get("lookback_months", 18)

    months = _generate_months(lookback)
    candidates = _build_urls(base_url, taxi_types, months)
    logger.info("Probing %d URL candidates across %d months...", len(candidates), len(months))

    available = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_probe_url, c): c for c in candidates}
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                available.append(result)

    available.sort(key=lambda x: (x["taxi_type"], x["month"]))
    logger.info("Found %d available files", len(available))
    return available
