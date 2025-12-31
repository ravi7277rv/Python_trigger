from datetime import datetime

import geopandas as gpd
import requests
from shapely.geometry import MultiPolygon, shape


def scrap_hazard_master_data_insert_to_db(logger):
    url = (
        "http://103.215.208.107:8585/geoserver/cite/ows"
        "?service=WFS&version=1.0.0&request=GetFeature"
        "&typeName=cite:act_warning1"
        "&outputFormat=application/json"
    )
    logger.info("fetching data from the url of actwarning")
    geojson = requests.get(url, timeout=30).json()
    features = geojson.get("features", [])
    logger.info("Data fetched Successfully")

    if not features:
        print("No features found")
        return None

    rows = []
    geometries = []

    severity_measure = {1: "Extreme", 2: "High", 3: "Moderate", 4: "Low"}

    for feature in features:
        props = feature.get("properties", {})
        geom = feature.get("geometry")

        if not geom or not geom.get("coordinates") or geom["coordinates"] == [[[]]]:
            continue

        day1_severity = severity_measure.get(props.get("day1_color"))
        day2_severity = severity_measure.get(props.get("day2_color"))
        day3_severity = severity_measure.get(props.get("day3_color"))
        day4_severity = severity_measure.get(props.get("day4_color"))
        day5_severity = severity_measure.get(props.get("day5_color"))

        geometry = shape(geom)
        if geometry.geom_type == "Polygon":
            geometry = MultiPolygon([geometry])

        rows.append(
            {
                "id": props.get("id"),
                "district": props.get("District"),
                "state": props.get("STATE"),
                "Shape_Leng": props.get("Shape_Leng"),
                "Shape_Area": props.get("Shape_Area"),
                "State_LGD": props.get("State_LGD"),
                "layer": props.get("layer"),
                "remarks": props.get("REMARKS"),
                "date": props.get("Date"),
                "utc": props.get("UTC"),
                "district_1": props.get("DISTRICT_1"),
                "day_1": props.get("Day_1"),
                "day_2": props.get("Day_2"),
                "day_3": props.get("Day_3"),
                "day_4": props.get("Day_4"),
                "day_5": props.get("Day_5"),
                "day1_text": props.get("Day1_text"),
                "day2_text": props.get("Day2_text"),
                "day3_text": props.get("Day3_text"),
                "day4_text": props.get("Day4_text"),
                "day5_text": props.get("Day5_text"),
                "day1_color": props.get("day1_color"),
                "day2_color": props.get("day2_color"),
                "day3_color": props.get("day3_color"),
                "day4_color": props.get("day4_color"),
                "day5_color": props.get("day5_color"),
                "day1_severity": day1_severity,
                "day2_severity": day2_severity,
                "day3_severity": day3_severity,
                "day4_severity": day4_severity,
                "day5_severity": day5_severity,
                "inserted_at": datetime.now(),
            }
        )

        geometries.append(geometry)
    gdf_hazard = gpd.GeoDataFrame(rows, geometry=geometries, crs="EPSG:4326")
    logger.info("Hazard data has been prepared")

    return gdf_hazard
