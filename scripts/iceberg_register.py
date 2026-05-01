"""Register trip data in an Iceberg table via PyIceberg REST catalog."""

import logging
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    DoubleType,
    LongType,
    IntegerType,
    StringType,
    TimestampType,
    NestedField,
)

from . import schemas as s

logger = logging.getLogger(__name__)

# Map PyArrow types to Iceberg types
_PA_TO_ICEBERG = {
    pa.int32(): IntegerType(),
    pa.int64(): LongType(),
    pa.float64(): DoubleType(),
    pa.large_string(): StringType(),
    pa.timestamp("us"): TimestampType(),
}


def _pa_schema_to_iceberg(pa_schema: pa.Schema) -> IcebergSchema:
    """Convert a PyArrow schema to a PyIceberg schema."""
    fields = []
    for i, field in enumerate(pa_schema):
        iceberg_type = _PA_TO_ICEBERG.get(field.type)
        if iceberg_type is None:
            raise ValueError(f"Unsupported type {field.type} for field {field.name}")
        fields.append(NestedField(
            field_id=i + 1,
            name=field.name,
            field_type=iceberg_type,
            required=False,
        ))
    return IcebergSchema(*fields)


class IcebergRegistrar:
    def __init__(self, config: dict):
        ice = config["iceberg"]
        self._namespace = ice["namespace"]
        self._batch_size = ice.get("batch_size", 500_000)

        catalog_props = {
            "uri": ice["catalog_uri"],
            "warehouse": ice.get("catalog_warehouse", ""),
        }
        if ice.get("catalog_token"):
            catalog_props["token"] = ice["catalog_token"]

        self._catalog = load_catalog("rest", **catalog_props)
        self._ensure_namespace()

    def _ensure_namespace(self):
        """Create the namespace if it doesn't exist."""
        try:
            self._catalog.create_namespace(self._namespace)
            logger.info("Created namespace %s", self._namespace)
        except Exception:
            # Namespace already exists
            pass

    def _table_name(self, taxi_type: str) -> str:
        return f"{self._namespace}.{taxi_type}_tripdata"

    def _ensure_table(self, taxi_type: str) -> object:
        """Create the Iceberg table if it doesn't exist, or load it."""
        table_id = self._table_name(taxi_type)
        pa_schema = s.SCHEMAS[taxi_type]
        iceberg_schema = _pa_schema_to_iceberg(pa_schema)

        try:
            table = self._catalog.create_table(table_id, schema=iceberg_schema)
            logger.info("Created Iceberg table %s", table_id)
        except Exception:
            table = self._catalog.load_table(table_id)
            logger.debug("Loaded existing Iceberg table %s", table_id)
        return table

    def register(self, local_path: Path, taxi_type: str) -> None:
        """Read a parquet file and append it to the corresponding Iceberg table in batches."""
        table = self._ensure_table(taxi_type)
        target_schema = s.SCHEMAS[taxi_type]

        parquet_file = pq.ParquetFile(str(local_path))
        total_rows = parquet_file.metadata.num_rows
        logger.info("Registering %s (%d rows) into Iceberg table %s",
                     local_path.name, total_rows, self._table_name(taxi_type))

        rows_written = 0
        for batch in parquet_file.iter_batches(batch_size=self._batch_size):
            arrow_table = pa.Table.from_batches([batch])
            arrow_table = s.align_table_to_schema(arrow_table, target_schema)
            self._append_with_retry(table, arrow_table)
            rows_written += len(batch)
            logger.info("  Appended batch: %d / %d rows", rows_written, total_rows)

        logger.info("Iceberg registration complete for %s", local_path.name)

    @staticmethod
    def _append_with_retry(table, arrow_table, max_retries=4, backoff=10):
        """Append with retry on rate limit errors."""
        for attempt in range(max_retries):
            try:
                table.append(arrow_table)
                return
            except Exception as e:
                if "TooManyRequests" in str(type(e).__name__) or "rate limit" in str(e).lower() or "too many" in str(e).lower():
                    if attempt < max_retries - 1:
                        wait = backoff * (2 ** attempt)
                        logger.warning("Rate limited, retrying in %ds...", wait)
                        time.sleep(wait)
                    else:
                        raise
                else:
                    raise


def is_configured(config: dict) -> bool:
    """Check if Iceberg catalog is configured (URI is non-empty)."""
    return bool(config.get("iceberg", {}).get("catalog_uri"))
