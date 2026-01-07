import logging
import os
import sys

import geopandas as gpd
import requests
from dotenv import load_dotenv
from shapely import MultiPolygon, Point

load_dotenv()
import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import wraps

import geopandas as gpd
import pandas as pd
import requests
from psycopg2.extras import execute_values
from shapely.geometry import Point, shape
from utils.db import get_cris_engine, get_weather_engine
from utils.scrap_from_html import scrape_all_days
from utils.scrap_hazard_data import scrap_hazard_master_data

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "error.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)

logger = logging.getLogger("hazard-pipeline")


# Decorator
def log_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        logger.info(f"{func_name} execution started")
        try:
            result = func(*args, **kwargs)
            logger.info(f"{func_name} execution completed successfully")
            return result
        except Exception:
            logger.exception(f"{func_name} execution failed with error")
            raise

    return wrapper


# region =============== HAZARD DATA (RAINFALL, SNOWFALL, LIGHTNING, WIND, FOG ) ========================
@log_execution
def read_district_geometry_gdf_and_save_json():
    engine = get_weather_engine()
    schema = "weatherdata"
    table_name = "district_geometry"

    query = f"""
        SELECT
            district,
            telecom_circle,
            state_ut,
            indus_circle,
            indus_zone,
            indus_circle_name,
            geometry
        FROM {schema}.{table_name}
        WHERE indus_circle IS NOT NULL
          AND indus_circle <> ''
    """

    # Read data
    gdf = gpd.read_postgis(
        sql=query,
        con=engine,
        geom_col="geometry",
        crs="EPSG:4326",
    )

    # ---- FIX: Project -> Centroid -> Re-project ----
    gdf_projected = gdf.to_crs(epsg=3857)  # metric CRS
    gdf_projected["geometry"] = gdf_projected.geometry.centroid
    gdf_centroid = gdf_projected.to_crs(epsg=4326)

    return gdf_centroid


# --------- New approach 2nd source --------------- #
@log_execution
def read_hazard_data_from_db():
    engine = get_weather_engine()

    # SQL query: join act_warning with imd_district on district_id
    query = """
                    SELECT DISTINCT ON (hw.district_id)
                hw.district_id,
                hw.district,
                hw.date,
                hw.day_1, hw.day_2, hw.day_3, hw.day_4, hw.day_5,
                hw.day1_text, hw.day2_text, hw.day3_text, hw.day4_text, hw.day5_text,
                hw.day1_color, hw.day2_color, hw.day3_color, hw.day4_color, hw.day5_color,
                d.geom
            FROM weatherdata.act_warning hw
            JOIN weatherdata.imd_district d
                ON hw.district_id = d.district_id
            WHERE hw.district_id <> 0
            ORDER BY hw.district_id;
    """

    # Read as GeoDataFrame
    gdf_hazard = gpd.read_postgis(
        sql=query,
        con=engine,
        geom_col="geom",
        crs="EPSG:4326",
    )

    return gdf_hazard


@log_execution
def create_district_hazard_geojson(gdf_districts, gdf_hazard):

    # Ensure CRS consistency
    if gdf_districts.crs != gdf_hazard.crs:
        gdf_hazard = gdf_hazard.to_crs(gdf_districts.crs)

    districts = gdf_districts.copy()
    hazards = gdf_hazard.copy()

    # Fields to keep (excluding geometry)
    HAZARD_FIELDS = [
        "date",
        "day_1",
        "day_2",
        "day_3",
        "day_4",
        "day_5",
        "day1_text",
        "day2_text",
        "day3_text",
        "day4_text",
        "day5_text",
        "day1_color",
        "day2_color",
        "day3_color",
        "day4_color",
        "day5_color",
    ]

    # Keep attributes only
    hazards = hazards[HAZARD_FIELDS]

    # Re-attach geometry correctly
    hazards = gpd.GeoDataFrame(
        hazards, geometry=gdf_hazard.geometry, crs=gdf_hazard.crs
    )

    # Spatial join: district points inside hazard polygons
    joined = gpd.sjoin(districts, hazards, how="left", predicate="within")

    # Cleanup
    joined = joined.drop(columns=["index_right"], errors="ignore")

    return joined


hazard_configs = {
    "lightning": {
        "keywords": [
            ("Thunderstorm & Lightning"),
            ("Lightning"),
        ],
    },
    "rainfall": {
        "keywords": [
            ("Extremely Heavy Rain"),
            ("Very Heavy Rain"),
            ("Heavy Rain"),
            ("Light Rain"),
            ("Rain"),
        ],
    },
    "wind": {
        "keywords": [
            ("Strong Surface Winds"),
            ("Thunderstorm & Lightning, Squall etc"),
        ],
    },
    "fog": {
        "keywords": [
            ("Fog"),
        ],
    },
    "coldhot": {
        "keywords": [
            ("Severe Cold Wave"),
            ("Severe Hot Wave"),
            ("Cold Wave"),
            ("Hot Wave"),
            ("Cold Day"),
            ("Hot Day"),
        ],
    },
}


COLOR_SEVERITY_MAP = {
    1: "Extreme",
    2: "High",
    3: "Moderate",
    4: "Low",
}


def detect_hazard_type(desc: str):
    if not desc:
        return None

    desc_l = desc.lower()

    for hazard_type, config in hazard_configs.items():
        # If no keywords list
        if "keywords" not in config:
            if hazard_type in desc_l:
                return hazard_type
            continue

        for keyword in config["keywords"]:
            if keyword.lower() in desc_l:
                return hazard_type

    return None


@log_execution
def build_hazard_records(final_gdf):
    if final_gdf is None or final_gdf.empty:
        raise ValueError("final_gdf is empty or None")

    final_gdf = final_gdf.copy()

    final_gdf["date"] = pd.to_datetime(final_gdf["date"], errors="coerce")
    final_gdf = final_gdf.dropna(subset=["date"])

    if final_gdf.empty:
        raise ValueError("No valid date values found")

    now_ts = datetime.now()
    hazard_records = defaultdict(list)

    # Base (current) date
    base_date = final_gdf["date"].iloc[0].normalize()

    # 🔹 First group by indus_circle
    for indus_circle, df_circle in final_gdf.groupby("indus_circle"):

        for day in range(1, 6):
            day_col = f"day_{day}"
            color_col = f"day{day}_color"

            if day_col not in df_circle.columns:
                continue

            # Split DF row by condition text
            df_day = df_circle.dropna(subset=[day_col, color_col]).copy()
            df_day[day_col] = (
                df_day[day_col].astype(str).str.split(r"\s*[,+]\s*", regex=True)
            )
            df_day = df_day.explode(day_col)
            df_day[day_col] = df_day[day_col].str.strip()

            df_day["hazard_type"] = df_day[day_col].apply(detect_hazard_type)
            df_day = df_day.dropna(subset=["hazard_type"])

            for hazard_type, group in df_day.groupby("hazard_type"):

                severity = COLOR_SEVERITY_MAP.get(int(group[color_col].iloc[0]), "Low")

                districts = sorted(set(group["district"].dropna().astype(str)))
                if not districts:
                    continue

                hazard_value = ", ".join(sorted(set(group[day_col].dropna())))

                hazard_records[hazard_type].append(
                    {
                        "indus_circle": indus_circle,
                        "days": f"Day{day}",
                        "date": (base_date + timedelta(days=day - 1)).date(),
                        "district": ", ".join(districts),
                        "hazard_value": hazard_value,
                        "description": hazard_value,
                        "severity": severity,
                        "insert_at": now_ts,
                    }
                )

    return hazard_records


# endregion ========================================================================


# region ==================== SAVE TO DATABASE ==================================
@log_execution
def resolve_tables_from_hazard_value(hazard_value, table_map):
    """
    Returns a set of table names based on hazard keywords
    found inside hazard_value.
    """
    tables = set()

    if not hazard_value:
        return tables

    hazard_value_lower = hazard_value.lower()

    for keyword, table in table_map.items():
        if keyword.lower() in hazard_value_lower:
            tables.add(table)

    return tables


@log_execution
def insert_hazards_forecast(hazard_records, engine):

    KEY_TABLE_MAP = {
        "fog": "hazard_fog",
        "lightning": "hazard_lightning",
        "snowfall": "hazard_snowfall",
        "coldhot": "hazard_coldwave_hotwave",
        "flood": "hazard_flood",
        "rainfall": "hazard_rainfall",
        "wind": "hazard_wind",
        "avalanche": "hazard_avalanche",
        "cloudburst": "hazard_cloudburst",
        "landslide": "hazard_landslide",
    }

    try:
        table_rows_map = {}

        # 🔑 USE hazard_records KEY
        for hazard_key, records in hazard_records.items():

            table_name = KEY_TABLE_MAP.get(hazard_key.lower())
            if not table_name:
                # print(f"No table mapping for hazard key: {hazard_key}")
                continue

            for rec in records:
                table_rows_map.setdefault(table_name, []).append(
                    (
                        rec["days"],
                        rec["date"],
                        rec["indus_circle"],
                        rec["district"],
                        rec["hazard_value"],
                        rec["description"],
                        rec["severity"],
                        rec["insert_at"],
                    )
                )

        # --- ENGINE-BASED INSERT ---
        with engine.begin() as connection:
            raw_conn = connection.connection
            cursor = raw_conn.cursor()

            for table_name, rows in table_rows_map.items():
                insert_query = f"""
                    INSERT INTO weatherdata.{table_name}
                    (days, "date", indus_circle, district,
                     hazard_value, description, severity, insert_at)
                    VALUES %s
                """
                execute_values(cursor, insert_query, rows)

    except Exception as e:
        return {"msg": f"Internal Server error: {str(e)}"}, 500


# endregion ========================================================================


# =======================================================================

if __name__ == "__main__":
    try:
        logger.info("Hazard processing started")
        cris_engine = get_cris_engine()

        scrap_hazard_master_data(cris_engine, logger)

        logger.info("Hazard table data formation started")
        gdf_districts = read_district_geometry_gdf_and_save_json()

        gdf_hazard = read_hazard_data_from_db()

        final_gdf = create_district_hazard_geojson(
            gdf_districts=gdf_districts,
            gdf_hazard=gdf_hazard,
        )

        hazard_records = build_hazard_records(final_gdf)

        insert_hazards_forecast(hazard_records, cris_engine)

        logger.info("Hazard data saved successfully")

    except Exception as e:
        logger.exception("Fatal error in main execution")
