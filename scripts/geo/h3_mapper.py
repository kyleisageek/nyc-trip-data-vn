"""Build and cache H3 hexagonal mapping for taxi zones."""

import json
import logging
from pathlib import Path

import h3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import mapping

from ..config import load_config
from .taxi_zones import load_taxi_zones

logger = logging.getLogger(__name__)


def _polygon_to_h3_cells(geometry, resolution: int) -> set[str]:
    """Convert a Shapely geometry (Polygon or MultiPolygon) to H3 cell set."""
    geojson = mapping(geometry)

    if geojson["type"] == "MultiPolygon":
        cells = set()
        for polygon_coords in geojson["coordinates"]:
            single_geojson = {"type": "Polygon", "coordinates": polygon_coords}
            cells.update(h3.polygon_to_cells(
                h3.geo_to_h3shape(single_geojson), resolution
            ))
        return cells
    else:
        h3_shape = h3.geo_to_h3shape(geojson)
        return h3.polygon_to_cells(h3_shape, resolution)


def build_h3_mapping(
    config: dict | None = None,
    resolution: int | None = None,
) -> pd.DataFrame:
    """Build a DataFrame mapping each LocationID to its H3 cells.

    Returns DataFrame with columns: LocationID, h3_index, h3_resolution,
    zone, borough, h3_lat, h3_lng.
    """
    if config is None:
        config = load_config()
    if resolution is None:
        resolution = config.get("geo", {}).get("h3", {}).get("resolution", 9)

    gdf = load_taxi_zones(config)
    rows = []

    for _, zone_row in gdf.iterrows():
        loc_id = zone_row["LocationID"]
        zone_name = zone_row["zone"]
        borough = zone_row["borough"]
        geometry = zone_row.geometry

        cells = _polygon_to_h3_cells(geometry, resolution)
        logger.debug("Zone %d (%s): %d H3 cells at res %d",
                      loc_id, zone_name, len(cells), resolution)

        for cell in cells:
            lat, lng = h3.cell_to_latlng(cell)
            rows.append({
                "LocationID": loc_id,
                "h3_index": cell,
                "h3_resolution": resolution,
                "zone": zone_name,
                "borough": borough,
                "h3_lat": lat,
                "h3_lng": lng,
            })

    df = pd.DataFrame(rows)
    logger.info("Built H3 mapping: %d cells across %d zones at resolution %d",
                len(df), gdf["LocationID"].nunique(), resolution)
    return df


def save_h3_mapping(df: pd.DataFrame, output_dir: str | Path) -> dict[str, Path]:
    """Save H3 mapping as both Parquet and JSON. Returns paths dict."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = output_dir / "zone_h3_mapping.parquet"
    json_path = output_dir / "zone_h3_mapping.json"

    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path)

    mapping_dict = df.groupby("LocationID")["h3_index"].apply(list).to_dict()
    mapping_dict = {str(k): v for k, v in mapping_dict.items()}
    with open(json_path, "w") as f:
        json.dump(mapping_dict, f)

    logger.info("Saved H3 mapping to %s and %s", parquet_path, json_path)
    return {"parquet": parquet_path, "json": json_path}


def load_h3_mapping(
    cache_dir: str | Path | None = None,
    config: dict | None = None,
) -> pd.DataFrame:
    """Load precomputed H3 mapping from Parquet cache, or build and cache it."""
    if config is None:
        config = load_config()
    if cache_dir is None:
        cache_dir = Path(config.get("geo", {}).get("local_cache_dir", "data/taxi_zones"))

    parquet_path = Path(cache_dir) / "zone_h3_mapping.parquet"

    if parquet_path.exists():
        logger.info("Loading cached H3 mapping from %s", parquet_path)
        return pd.read_parquet(parquet_path)

    logger.info("No cached H3 mapping found, building...")
    df = build_h3_mapping(config)
    save_h3_mapping(df, cache_dir)
    return df
