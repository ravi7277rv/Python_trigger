from utils.insert_to_table import insert_act_warning1
from utils.logger import setup_logger
from utils.read_act_warning_data import scrap_hazard_master_data_insert_to_db
from utils.read_districts_geom import read_district_geometry_gdf_and_save_json
from utils.spatial_join import spatially_join_hazard_with_districts

# Setting UP logger
logger = setup_logger()


def main():

    gdf_hazard = scrap_hazard_master_data_insert_to_db(logger)

    # Read district points
    gdf_districts = read_district_geometry_gdf_and_save_json(logger)

    # Perfoem Spatial Join for data
    logger.info("Performing spatial join on the districts and act_warning data.")
    joined_gdf = spatially_join_hazard_with_districts(gdf_hazard, gdf_districts, logger)

    insert_act_warning1(joined_gdf, logger)


if __name__ == "__main__":
    logger.info("Act_Warning1 Table data prepration has been started.")
    main()
    logger.info("Act_Warning1 Table data insertation has been finished.")
