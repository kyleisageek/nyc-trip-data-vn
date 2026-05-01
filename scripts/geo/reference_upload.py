"""Upload precomputed geo reference files to R2."""

import logging
from pathlib import Path

from ..config import load_config
from ..r2_client import R2Client

logger = logging.getLogger(__name__)


def upload_reference_files(config: dict | None = None) -> None:
    """Upload all geo reference files from local cache to R2."""
    if config is None:
        config = load_config()

    geo = config.get("geo", {})
    cache_dir = Path(geo.get("local_cache_dir", "data/taxi_zones"))
    r2_prefix = geo.get("r2_reference_prefix", "reference/taxi-zones")

    r2 = R2Client(config)
    files_to_upload = [
        "zone_h3_mapping.parquet",
        "zone_h3_mapping.json",
    ]

    for filename in files_to_upload:
        local_path = cache_dir / filename
        if local_path.exists():
            r2_key = f"{r2_prefix}/{filename}"
            r2.upload(local_path, r2_key)
            logger.info("Uploaded %s to R2", filename)
        else:
            logger.warning("Reference file not found: %s", local_path)
