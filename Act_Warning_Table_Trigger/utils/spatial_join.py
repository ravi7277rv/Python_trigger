import geopandas as gpd


def spatially_join_hazard_with_districts(gdf_hazard, gdf_districts, logger):

    # Rename to avoid column collision
    gdf_districts = gdf_districts.rename(columns={"district": "indus_district"})

    # Ensure CRS match
    gdf_hazard = gdf_hazard.to_crs("EPSG:4326")
    gdf_districts = gdf_districts.to_crs("EPSG:4326")

    # Spatial join: hazard POLYGON contains district POINT
    joined_gdf = gpd.sjoin(gdf_hazard, gdf_districts, how="inner", predicate="contains")

    # Drop join index column added by sjoin
    joined_gdf = joined_gdf.drop(columns=["index_right"], errors="ignore")

    # Explicitly keep hazard geometry
    joined_gdf = joined_gdf.set_geometry("geometry")

    return joined_gdf
