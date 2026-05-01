"""Cloudflare R2 client for uploading and listing trip data files."""

import logging
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig

logger = logging.getLogger(__name__)


class R2Client:
    def __init__(self, config: dict):
        r2 = config["r2"]
        self._bucket = r2["bucket"]
        self._prefix = r2.get("prefix", "tlc-trip-data")
        self._client = boto3.client(
            "s3",
            endpoint_url=r2["endpoint_url"],
            aws_access_key_id=r2["access_key_id"],
            aws_secret_access_key=r2["secret_access_key"],
            config=BotoConfig(
                retries={"max_attempts": 3, "mode": "adaptive"},
            ),
            region_name="auto",
        )

    def _object_key(self, filename: str) -> str:
        return f"{self._prefix}/{filename}"

    def list_existing(self) -> set[str]:
        """List filenames already in the R2 bucket under the configured prefix."""
        existing = set()
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix + "/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = key.removeprefix(self._prefix + "/")
                if filename:
                    existing.add(filename)
        logger.info("Found %d existing files in R2", len(existing))
        return existing

    def upload(self, local_path: Path, filename: str) -> None:
        """Upload a local file to R2."""
        key = self._object_key(filename)
        file_size = local_path.stat().st_size
        logger.info("Uploading %s (%d bytes) to R2 key %s", filename, file_size, key)

        # Use multipart upload for files > 100MB
        transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=100 * 1024 * 1024,
            multipart_chunksize=100 * 1024 * 1024,
        )
        self._client.upload_file(
            str(local_path),
            self._bucket,
            key,
            Config=transfer_config,
        )
        logger.info("Upload complete: %s", key)
