"""PyArrow schemas for NYC TLC taxi trip data types."""

import pyarrow as pa

YELLOW_SCHEMA = pa.schema([
    pa.field("VendorID", pa.int32()),
    pa.field("tpep_pickup_datetime", pa.timestamp("us")),
    pa.field("tpep_dropoff_datetime", pa.timestamp("us")),
    pa.field("passenger_count", pa.int64()),
    pa.field("trip_distance", pa.float64()),
    pa.field("RatecodeID", pa.int64()),
    pa.field("store_and_fwd_flag", pa.large_string()),
    pa.field("PULocationID", pa.int32()),
    pa.field("DOLocationID", pa.int32()),
    pa.field("payment_type", pa.int64()),
    pa.field("fare_amount", pa.float64()),
    pa.field("extra", pa.float64()),
    pa.field("mta_tax", pa.float64()),
    pa.field("tip_amount", pa.float64()),
    pa.field("tolls_amount", pa.float64()),
    pa.field("improvement_surcharge", pa.float64()),
    pa.field("total_amount", pa.float64()),
    pa.field("congestion_surcharge", pa.float64()),
    pa.field("Airport_fee", pa.float64()),
    pa.field("cbd_congestion_fee", pa.float64()),
])

GREEN_SCHEMA = pa.schema([
    pa.field("VendorID", pa.int32()),
    pa.field("lpep_pickup_datetime", pa.timestamp("us")),
    pa.field("lpep_dropoff_datetime", pa.timestamp("us")),
    pa.field("store_and_fwd_flag", pa.large_string()),
    pa.field("RatecodeID", pa.int64()),
    pa.field("PULocationID", pa.int32()),
    pa.field("DOLocationID", pa.int32()),
    pa.field("passenger_count", pa.int64()),
    pa.field("trip_distance", pa.float64()),
    pa.field("fare_amount", pa.float64()),
    pa.field("extra", pa.float64()),
    pa.field("mta_tax", pa.float64()),
    pa.field("tip_amount", pa.float64()),
    pa.field("tolls_amount", pa.float64()),
    pa.field("ehail_fee", pa.float64()),
    pa.field("improvement_surcharge", pa.float64()),
    pa.field("total_amount", pa.float64()),
    pa.field("payment_type", pa.int64()),
    pa.field("trip_type", pa.int64()),
    pa.field("congestion_surcharge", pa.float64()),
    pa.field("cbd_congestion_fee", pa.float64()),
])

FHVHV_SCHEMA = pa.schema([
    pa.field("hvfhs_license_num", pa.large_string()),
    pa.field("dispatching_base_num", pa.large_string()),
    pa.field("originating_base_num", pa.large_string()),
    pa.field("request_datetime", pa.timestamp("us")),
    pa.field("on_scene_datetime", pa.timestamp("us")),
    pa.field("pickup_datetime", pa.timestamp("us")),
    pa.field("dropoff_datetime", pa.timestamp("us")),
    pa.field("PULocationID", pa.int32()),
    pa.field("DOLocationID", pa.int32()),
    pa.field("trip_miles", pa.float64()),
    pa.field("trip_time", pa.int64()),
    pa.field("base_passenger_fare", pa.float64()),
    pa.field("tolls", pa.float64()),
    pa.field("bcf", pa.float64()),
    pa.field("sales_tax", pa.float64()),
    pa.field("congestion_surcharge", pa.float64()),
    pa.field("airport_fee", pa.float64()),
    pa.field("tips", pa.float64()),
    pa.field("driver_pay", pa.float64()),
    pa.field("shared_request_flag", pa.large_string()),
    pa.field("shared_match_flag", pa.large_string()),
    pa.field("access_a_ride_flag", pa.large_string()),
    pa.field("wav_request_flag", pa.large_string()),
    pa.field("wav_match_flag", pa.large_string()),
    pa.field("cbd_congestion_fee", pa.float64()),
])

SCHEMAS = {
    "yellow": YELLOW_SCHEMA,
    "green": GREEN_SCHEMA,
    "fhvhv": FHVHV_SCHEMA,
}


def align_table_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Align a table to the target schema, adding missing columns as nulls.

    This handles schema evolution — e.g., cbd_congestion_fee was added in 2025,
    so older files won't have it.
    """
    for field in schema:
        if field.name not in table.schema.names:
            null_array = pa.nulls(len(table), type=field.type)
            table = table.append_column(field, null_array)

    # Select only columns in the target schema, in the correct order
    return table.select([f.name for f in schema])
