import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple

import geopandas as gpd
import pandas as pd
import requests
from geoalchemy2 import Geometry
from shapely.geometry import shape
from shapely.geometry.multipolygon import MultiPolygon
from utils.helper import cat_text, clean_int, clean_text, severity_from_color
from utils.scrap_from_html import scrape_all_days

# ------------------ URL's FOR GETTING THE HAZARD DATA -----------------------------------
URLS = [
    {
        "id": 1,
        "url": f"""https://reactjs.imd.gov.in/geoserver/imd/wfs?service=WFS
                        &version=1.0.0&request=GetFeature&typeName=imd:district_warnings_india
                        &outputFormat=application/json&srsName=EPSG:4326""",
    },
    {
        "id": 2,
        "url": f"""http://103.215.208.107:8585/geoserver/cite/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=cite:act_warning1
                        &outputFormat=application/json""",
    },
]


class HazardSource:
    def fetch(self, logger):
        raise NotImplementedError


class ScrapFromUrlSource(HazardSource):
    def __init__(self, urls):
        self.urls = urls

    def fetch(self, logger):
        features, source_id = fetch_geojson_features(self.urls, logger)
        logger.info("GeoJSON source succeeded: source_id=%s", source_id)

        if source_id == 1:
            return append_features_from_url_1(features)
        elif source_id == 2:
            return append_features_from_url_2(features)
        else:
            raise ValueError("Unknown source_id")


class ScrapeAllDaysFromHTMLSource(HazardSource):
    def fetch(self, logger):
        logger.info("Trying scrape_all_days() source")

        df = scrape_all_days()

        if df is None or df.empty:
            raise ValueError("scrape_all_days returned empty dataframe")

        logger.info("scrape_all_days succeeded")
        return df


# ---------- FETCHING THE HAZARD DATA FEATURES -----------------
def fetch_geojson_features(
    urls: List[Dict[str, Any]],
    logger: logging.Logger,
    timeout: int = 30,
    min_features: int = 1,
) -> Tuple[List[Dict[str, Any]], int, str]:

    errors = []

    logger.info("Starting GeoJSON fetch from %d sources", len(urls))

    for idx, source in enumerate(urls, start=1):
        source_id = source.get("id")
        url = source.get("url")

        logger.info(
            "Trying source %d/%d | id=%s | url=%s", idx, len(urls), source_id, url
        )

        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()

            geojson = response.json()

            if not isinstance(geojson, dict):
                raise ValueError("Response is not a JSON object")

            features = geojson.get("features")

            if not isinstance(features, list):
                raise ValueError("'features' key missing or invalid")

            if len(features) < min_features:
                raise ValueError("Empty features list")

            logger.info(
                "Successfully fetched %d features from source_id=%s",
                len(features),
                source_id,
            )

            return features, source_id

        except requests.exceptions.Timeout:
            msg = f"Timeout while accessing source_id={source_id}"
            logger.warning(msg)
            errors.append(msg)

        except requests.exceptions.ConnectionError:
            msg = f"Connection error for source_id={source_id}"
            logger.warning(msg)
            errors.append(msg)

        except requests.exceptions.HTTPError as e:
            msg = f"HTTP error for source_id={source_id}: {e}"
            logger.error(msg)
            errors.append(msg)

        except ValueError as e:
            msg = f"Invalid data from source_id={source_id}: {e}"
            logger.warning(msg)
            errors.append(msg)

        except Exception as e:
            msg = f"Unexpected error from source_id={source_id}: {e}"
            logger.exception(msg)
            errors.append(msg)

    error_message = "All data sources failed:\n" + "\n".join(errors)
    logger.critical(error_message)

    raise RuntimeError(error_message)


# --------- APPEND FEATURES FROM THE URL 1 ------------------
def append_features_from_url_2(features):
    if not features:
        print("No features found")
        return

    rows = []

    for feature in features:
        props = feature.get("properties", {})
        geom = feature.get("geometry")

        if not geom or not geom.get("coordinates") or geom["coordinates"] == [[[]]]:
            continue

        geometry = shape(geom)

        if geometry.geom_type == "Polygon":
            geometry = MultiPolygon([geometry])

        rows.append(
            {
                "district_id": clean_int(props.get("Obj_id")),
                "district": props.get("District"),
                "date": props.get("Date"),
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
                "day1_severity": severity_from_color(props.get("day1_color")),
                "day2_severity": severity_from_color(props.get("day2_color")),
                "day3_severity": severity_from_color(props.get("day3_color")),
                "day4_severity": severity_from_color(props.get("day4_color")),
                "day5_severity": severity_from_color(props.get("day5_color")),
                "inserted_at": datetime.now(),
            }
        )

    return rows


# --------- APPEND FEATURES FROM THE URL 2 ------------------
def append_features_from_url_1(features):
    if not features:
        print("No features found")
        return

    rows = []
    geometries = []

    for feature in features:
        props = feature.get("properties", {})
        geom = feature.get("geometry")

        if not geom or not geom.get("coordinates") or geom["coordinates"] == [[[]]]:
            continue

        geometry = shape(geom)

        if geometry.geom_type == "Polygon":
            geometry = MultiPolygon([geometry])

        rows.append(
            {
                "district_id": clean_int(props.get("Obj_id")),
                "district": clean_text(props.get("District")),
                "date": props.get("Date"),
                "day_1": cat_text(props.get("Day_1")),
                "day_2": cat_text(props.get("Day_2")),
                "day_3": cat_text(props.get("Day_3")),
                "day_4": cat_text(props.get("Day_4")),
                "day_5": cat_text(props.get("Day_5")),
                "day1_text": cat_text(props.get("Day_1")),
                "day2_text": cat_text(props.get("Day_2")),
                "day3_text": cat_text(props.get("Day_3")),
                "day4_text": cat_text(props.get("Day_4")),
                "day5_text": cat_text(props.get("Day_5")),
                "day1_color": clean_int(props.get("Day1_Color")),
                "day2_color": clean_int(props.get("Day2_Color")),
                "day3_color": clean_int(props.get("Day3_Color")),
                "day4_color": clean_int(props.get("Day4_Color")),
                "day5_color": clean_int(props.get("Day5_Color")),
                "day1_severity": severity_from_color(props.get("Day1_Color")),
                "day2_severity": severity_from_color(props.get("Day2_Color")),
                "day3_severity": severity_from_color(props.get("Day3_Color")),
                "day4_severity": severity_from_color(props.get("Day4_Color")),
                "day5_severity": severity_from_color(props.get("Day5_Color")),
                "inserted_at": datetime.now(),
            }
        )

    df_2 = pd.DataFrame(rows)

    return df_2


# ----------------- SCRAPPING THE HAZARD DATA FROM THE DIFFERETN SOURCES -------------------
def scrap_hazard_master_data(cris_engine, logger):

    sources = [
        ScrapFromUrlSource(URLS),
        ScrapeAllDaysFromHTMLSource(),
    ]

    last_error = None

    for source in sources:
        try:
            logger.info("Trying hazard source: %s", source.__class__.__name__)

            df_hazard = source.fetch(logger)

            logger.info(
                "Hazard data obtained successfully from %s | rows=%d",
                source.__class__.__name__,
                len(df_hazard),
            )

            insert_hazard_gdf_to_db(df_hazard, cris_engine, logger, db="cris_engine")

            logger.info("Hazard data inserted into both databases")

            return df_hazard

        except Exception as e:
            logger.warning(
                "Hazard source failed: %s | error=%s",
                source.__class__.__name__,
                str(e),
            )
            logger.exception(e)
            last_error = e

    # If everything failed:
    logger.critical("ALL hazard sources failed")
    raise RuntimeError("All hazard data sources failed") from last_error


# ----------- INSERTING THE HAZARD DATA TO THE ACT_WARNING TABLE ----------
# def insert_hazard_gdf_to_db(df_hazard, engine, logger, db):
#     if df_hazard.empty:
#         logger.warning("DataFrame is empty. Nothing to insert.")
#         return

#     logger.info(f"Inserting %d hazard records into {db} database", len(df_hazard))

#     df_hazard.to_sql(
#         name="act_warning",
#         con=engine,
#         schema="weatherdata",  # optional
#         if_exists="replace",  # append | replace | fail
#         index=False,
#         method="multi",  # faster bulk insert
#         chunksize=1000,
#     )

#     logger.info(f"Hazard data inserted successfully to the {db} database")


def insert_hazard_gdf_to_db(df_hazard, engine, logger, db):
    try:
        if df_hazard.empty:
            logger.warning("DataFrame is empty. Nothing to insert.")
            return

        logger.info(f"Inserting %d hazard records into {db} database", len(df_hazard))
        logger.info(f"DB URL = {engine.url}")

        with engine.begin() as conn:  # <-- THIS AUTO-COMMITS
            df_hazard.to_sql(
                name="act_warning",
                con=conn,
                schema="weatherdata",
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=1000,
            )

        logger.info(f"Hazard data inserted successfully to the {db} database")
    except Exception as e:
        logger.error(f"Error while inserting to db {e}")
        print(f"Exception while inserting to {db} : {e}")
