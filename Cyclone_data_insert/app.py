import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from functools import wraps

import geopandas as gpd
import pandas as pd
import requests
from db import db_engine
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from shapely import MultiPolygon
from shapely.geometry import shape

load_dotenv()


# -------------------------------------------------
# Ensure logs directory exists
# -------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "hazard_pipeline.txt")

# -------------------------------------------------
# Logging configuration (append mode)
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)

logger = logging.getLogger("hazard-pipeline")


# -------------------------------------------------
# Decorator
# -------------------------------------------------
def log_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_name = func.__name__

        logger.info(f"▶️ {func_name} execution started")

        try:
            result = func(*args, **kwargs)
            logger.info(f"✅ {func_name} execution completed successfully")
            return result

        except Exception:
            logger.exception(f"❌ {func_name} execution failed with error")
            raise

    return wrapper


# =====================================================================


# region =============== HAZARD DATA CYCLONE ========================
@log_execution
def get_impacted_circles():
    try:
        IMPACTED_CIRCLES = []

        engine = db_engine()
        schema = "weatherdata"
        table_name = "cyclone_impacted_circles"

        query = f"""
            SELECT name
            FROM {schema}.{table_name}
            WHERE inserted_at = (
                SELECT MAX(inserted_at)
                FROM {schema}.{table_name}
            )
        """

        df = pd.read_sql(query, engine)

        if not df.empty:
            for names in df["name"].dropna():
                IMPACTED_CIRCLES.extend(
                    [n.strip() for n in names.split(",") if n.strip()]
                )

        # remove duplicates while preserving order
        IMPACTED_CIRCLES = list(dict.fromkeys(IMPACTED_CIRCLES))

        return IMPACTED_CIRCLES

    except Exception as e:
        print("Error in get_impacted_circles:", e)
        return []


@log_execution
def read_district_geometry_gdf_and_save_json():
    try:
        engine = db_engine()
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

        # Read data from PostGIS
        gdf = gpd.read_postgis(
            sql=query,
            con=engine,
            geom_col="geometry",
            crs="EPSG:4326",
        )

        # ---- FILTER ONLY IMPACTED CIRCLES ----
        IMPACTED_CIRCLES = get_impacted_circles()

        if not IMPACTED_CIRCLES:
            return gdf.iloc[0:0]  # empty GeoDataFrame

        gdf_impacted = gdf[gdf["indus_circle"].isin(IMPACTED_CIRCLES)].reset_index(
            drop=True
        )

        if gdf_impacted.empty:
            return gdf_impacted

        # ---- CONVERT GEOMETRY TO CENTROID ----
        gdf_impacted = gdf_impacted.copy()
        gdf_impacted["geometry"] = gdf_impacted.geometry.centroid

        return gdf_impacted

    except Exception as e:
        print("Error in read_district_geometry_gdf_and_save_json:", e)
        return gpd.GeoDataFrame()

    except Exception as e:
        print("Error in read_district_geometry_gdf_and_save_json:", e)
        return gpd.GeoDataFrame()


@log_execution
def scrap_hazard_master_data_insert_to_db():

    url = (
        "http://103.215.208.107:8585/geoserver/cite/ows"
        "?service=WFS&version=1.0.0&request=GetFeature"
        "&typeName=cite:act_warning1"
        "&outputFormat=application/json"
    )

    geojson = requests.get(url, timeout=30).json()
    features = geojson.get("features", [])

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
                "district": props.get("District"),
                "state": props.get("STATE"),
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
                "inserted_at": datetime.now(),
            }
        )
        geometries.append(geometry)

    # --- Convert to proper GeoDataFrame ---
    gdf_hazard = gpd.GeoDataFrame(rows, geometry=geometries, crs="EPSG:4326")

    return gdf_hazard


@log_execution
def create_district_hazard_geojson(
    gdf_districts,
    gdf_hazard,
):
    # 1️⃣ Ensure CRS consistency
    if gdf_districts.crs != gdf_hazard.crs:
        gdf_hazard = gdf_hazard.to_crs(gdf_districts.crs)

    districts = gdf_districts.copy()
    hazards = gdf_hazard.copy()

    # 2️⃣ Select hazard fields (keep geometry for spatial join)
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
        "geometry",
    ]

    hazards = hazards[HAZARD_FIELDS]

    # 3️⃣ Spatial join: centroid points within hazard polygons
    joined = gpd.sjoin(districts, hazards, how="left", predicate="within")

    # 4️⃣ Clean up join artifacts
    joined = joined.drop(columns=["index_right"], errors="ignore")

    # 5️⃣ Ensure geometry is centroid (district geometry)
    joined = joined.set_geometry(districts.geometry)

    return joined


hazard_configs = {
    "cyclone": {
        "keywords": [("Thunderstorm"), ("Lightning"), ("Wind"), ("Rain"), ("Squall")],
    },
}

COLOR_SEVERITY_MAP = {
    1: "Extreme",
    2: "High",
    3: "Moderate",
    4: "Low",
}


def contains_any_keyword(text, keywords):
    if not isinstance(text, str):
        return False
    text_lower = text.lower()
    return any(k.lower() in text_lower for k in keywords)


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

    base_date = final_gdf["date"].iloc[0].normalize()

    cyclone_keywords = hazard_configs["cyclone"]["keywords"]
    cyclone_keywords = [k[0] if isinstance(k, tuple) else k for k in cyclone_keywords]

    for indus_circle, df_circle in final_gdf.groupby("indus_circle"):

        for day in range(1, 6):
            day_col = f"day_{day}"
            color_col = f"day{day}_color"

            if day_col not in df_circle.columns:
                continue

            # --------------------------------
            # 🔹 STEP 1: explode comma-separated hazards
            # --------------------------------
            df_expanded = df_circle.copy()
            df_expanded[day_col] = (
                df_expanded[day_col].fillna("").astype(str).str.split(",")
            )
            df_expanded = df_expanded.explode(day_col)
            df_expanded[day_col] = df_expanded[day_col].str.strip()
            df_expanded = df_expanded[df_expanded[day_col] != ""]

            if df_expanded.empty:
                continue

            # --------------------------------
            # 🔹 STEP 2: process EACH keyword independently
            # --------------------------------
            for hazard_text, df_hazard in df_expanded.groupby(day_col):

                # ---------- STRICT LOGIC ----------
                if color_col in df_hazard.columns:
                    df_valid = df_hazard.dropna(subset=[color_col]).copy()

                    df_valid["is_cyclone"] = contains_any_keyword(
                        hazard_text, cyclone_keywords
                    )

                    df_valid = df_valid[
                        (df_valid["is_cyclone"])
                        & (df_valid[color_col].astype(int).isin([1, 2, 3]))
                    ]
                else:
                    df_valid = pd.DataFrame()

                if not df_valid.empty:
                    severity = COLOR_SEVERITY_MAP.get(
                        int(df_valid[color_col].iloc[0]), None
                    )
                    districts = sorted(set(df_valid["district"].dropna().astype(str)))
                else:
                    # ---------- FALLBACK ----------
                    severity = None
                    districts = None

                NULL_HAZARD_TEXTS = {"no warning", "ground frost"}

                hazard_text_clean = str(hazard_text).strip()
                hazard_text_lower = hazard_text_clean.lower()

                is_null_hazard = hazard_text_lower in NULL_HAZARD_TEXTS

                hazard_value = None if is_null_hazard else hazard_text_clean
                description = None if is_null_hazard else hazard_text_clean

                # --------------------------------
                # 🔹 FINAL RECORD
                # --------------------------------
                hazard_records["cyclone"].append(
                    {
                        "indus_circle": indus_circle,
                        "days": f"Day{day}",
                        "date": (base_date + timedelta(days=day - 1)).date(),
                        "district": ", ".join(districts) if districts else None,
                        "hazard_value": hazard_value,
                        "description": description,
                        "severity": severity,
                        "insert_at": now_ts,
                    }
                )

    return hazard_records


# endregion ========================================================================


# region ==================== SAVE TO DATABASE ==================================
@log_execution
def insert_hazards_forecast(hazard_records):
    engine = db_engine()

    KEY_TABLE_MAP = {"cyclone": "hazard_cyclone", "ground_frost": "hazard_ground_frost"}

    try:
        table_rows_map = {}

        # 🔑 USE hazard_records KEY
        for hazard_key, records in hazard_records.items():

            table_name = KEY_TABLE_MAP.get(hazard_key.lower())
            if not table_name:
                print(f"⚠️ No table mapping for hazard key: {hazard_key}")
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

        print(
            "tables_inserted",
            {table: len(rows) for table, rows in table_rows_map.items()},
        )

    except Exception as e:
        return {"msg": f"Internal Server error: {str(e)}"}, 500


# endregion ========================================================================


# =======================================================================

if __name__ == "__main__":
    try:
        logger.info("Hazard processing started")

        impacted_districts_gdf = read_district_geometry_gdf_and_save_json()
        gdf_hazard = scrap_hazard_master_data_insert_to_db()

        impacted_circle_with_district = create_district_hazard_geojson(
            impacted_districts_gdf, gdf_hazard
        )
        cyclone_hazard_records = build_hazard_records(impacted_circle_with_district)

        insert_hazards_forecast(cyclone_hazard_records)

        logger.info("Hazard data saved to db successfully")

    except Exception as pe:
        logger.exception("Fatal error in main execution")
