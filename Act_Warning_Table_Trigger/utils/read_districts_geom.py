import geopandas as gpd

from utils.db import get_cris_engine


def read_district_geometry_gdf_and_save_json(logger):
    engine = get_cris_engine()
    schema = "weatherdata"
    table_name = "district_geometry_point"

    query = f"""
        SELECT
            district,
            indus_circle,
            geom
        FROM {schema}.{table_name}
    """

    # Read data
    logger.info("Fetching data from the database for districts.")
    gdf_districts = gpd.read_postgis(
        sql=query,
        con=engine,
        geom_col="geom",
        crs="EPSG:4326",
    )

    logger.info("Data fetched successfully from database.")

    return gdf_districts
    return gdf_districts
