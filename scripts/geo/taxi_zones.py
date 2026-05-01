"""Download, cache, and load NYC TLC taxi zone shapefile."""

import io
import logging
import zipfile
from pathlib import Path

import geopandas as gpd
import requests

from ..config import load_config

logger = logging.getLogger(__name__)

_CACHE: dict = {}


def _project_root() -> Path:
    """Return the project root directory (parent of scripts/)."""
    return Path(__file__).resolve().parent.parent.parent


def _ensure_shapefile(config: dict | None = None) -> Path:
    """Download taxi_zones.zip if not already cached locally."""
    if config is None:
        config = load_config()
    geo = config.get("geo", {})

    cache_dir = _project_root() / geo.get("local_cache_dir", "data/taxi_zones")

    # Check if already downloaded
    shp_files = list(cache_dir.rglob("*.shp")) if cache_dir.exists() else []
    if shp_files:
        logger.debug("Shapefile already cached at %s", shp_files[0])
        return shp_files[0]

    url = geo.get("shapefile_url",
                   "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip")
    logger.info("Downloading taxi zone shapefile from %s", url)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    cache_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        zf.extractall(cache_dir)

    # Find the .shp file — zip may contain a nested directory
    shp_files = list(cache_dir.rglob("*.shp"))
    if not shp_files:
        raise FileNotFoundError(f"No .shp file found in {cache_dir}")
    shapefile_path = shp_files[0]

    logger.info("Shapefile extracted to %s", shapefile_path)
    return shapefile_path


def load_taxi_zones(config: dict | None = None) -> gpd.GeoDataFrame:
    """Load taxi zones as a WGS84 GeoDataFrame, filtering unknown zones."""
    if "zones" in _CACHE:
        return _CACHE["zones"]

    if config is None:
        config = load_config()

    shapefile_path = _ensure_shapefile(config)
    gdf = gpd.read_file(shapefile_path)

    # Reproject to WGS84 for H3 and KeplerGL compatibility
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Filter unknown zones
    unknown_ids = config.get("geo", {}).get("unknown_location_ids", [264, 265])
    gdf = gdf[~gdf["LocationID"].isin(unknown_ids)].copy()

    gdf = gdf.set_index("LocationID", drop=False)
    _CACHE["zones"] = gdf
    return gdf


def get_zone_centroids(config: dict | None = None) -> dict:
    """Return {LocationID: (lat, lng)} for each zone centroid."""
    gdf = load_taxi_zones(config)
    # Compute centroids in projected CRS for accuracy, then convert back
    projected = gdf.to_crs(epsg=2263)
    centroids_proj = projected.geometry.centroid
    centroids = centroids_proj.to_crs(epsg=4326)
    return {
        loc_id: (centroids.loc[loc_id].y, centroids.loc[loc_id].x)
        for loc_id in gdf.index
    }


def get_zone_lookup(config: dict | None = None) -> dict:
    """Return {LocationID: {'zone': name, 'borough': borough}} dict."""
    gdf = load_taxi_zones(config)
    return {
        row.LocationID: {"zone": row.zone, "borough": row.borough}
        for row in gdf.itertuples()
    }
