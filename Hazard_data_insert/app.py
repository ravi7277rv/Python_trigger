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
from db import db_engine
from psycopg2.extras import execute_values
from shapely.geometry import Point, shape

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


# region =============== HAZARD DATA (RAINFALL, SNOWFALL, LIGHTNING, WIND, FOG ) ========================


@log_execution
def read_district_geometry_gdf_and_save_json():
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

    # Save as GeoJSON
    # output_path="district_geometry_centroids.json"
    # gdf_centroid.to_file(output_path, driver="GeoJSON")

    return gdf_centroid


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
    "lightning": {
        "keywords": [
            ("Thunderstorm & Lightning", "High"),
            ("Lightning", "Moderate"),
        ],
    },
    "rainfall": {
        "keywords": [
            ("Extremely Heavy Rain", "Extreme"),
            ("Very Heavy Rain", "High"),
            ("Heavy Rain", "Moderate"),
            ("Light Rain", "Low"),
            ("Rain", "Low"),
        ],
    },
    "wind": {
        "keywords": [
            ("Strong Surface Winds", "High"),
            ("Thunderstorm & Lightning, Squall etc", "Low"),
        ],
    },
    "fog": {
        "keywords": [
            ("Fog", "Moderate"),
        ],
    },
    "coldhot": {
        "keywords": [
            ("Cold Wave", "Moderate"),
            ("Hot Wave", "Moderate"),
        ],
    },
    "snowfall": {"keywords": [("Heavy Snow", "High"), ("Snow", "moderate")]},
}


COLOR_SEVERITY_MAP = {
    1: "Extreme",
    2: "High",
    3: "Moderate",
    4: "Low",
}


@log_execution
def detect_hazard_type(desc: str):
    if not desc:
        return None

    desc_l = desc.lower()

    for hazard_type, config in hazard_configs.items():
        # Cyclone has no keywords list
        if "keywords" not in config:
            if hazard_type in desc_l:
                return hazard_type
            continue

        for keyword, _severity in config["keywords"]:
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
            text_col = f"day{day}_text"

            if day_col not in df_circle.columns:
                continue

            # Split DF row by condition text
            df_day = df_circle.dropna(subset=[day_col, color_col]).copy()
            df_day[day_col] = df_day[day_col].astype(str).str.split(",")
            df_day = df_day.explode(day_col)
            df_day[day_col] = df_day[day_col].str.strip()

            df_day["hazard_type"] = df_day[day_col].apply(detect_hazard_type)
            df_day = df_day.dropna(subset=["hazard_type"])

            with open("df_day.json", "w") as f:
                json.dump(df_day, f, indent=4, default=str)

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


# def build_hazard_records(final_gdf):
#     if final_gdf is None or final_gdf.empty:
#         raise ValueError("final_gdf is empty or None")

#     final_gdf = final_gdf.copy()
#     final_gdf["date"] = pd.to_datetime(final_gdf["date"], errors="coerce")
#     final_gdf = final_gdf.dropna(subset=["date"])

#     if final_gdf.empty:
#         raise ValueError("No valid date values found")

#     now_ts = datetime.now()
#     hazard_records = defaultdict(list)

#     # Base (current) date
#     base_date = final_gdf["date"].iloc[0].normalize()

#     # 🔹 First group by indus_circle
#     for indus_circle, df_circle in final_gdf.groupby("indus_circle"):

#         for day in range(1, 6):
#             day_col = f"day_{day}"
#             color_col = f"day{day}_color"
#             text_col = f"day{day}_text"

#             if day_col not in df_circle.columns:
#                 continue

#             df_day = df_circle.dropna(subset=[day_col, color_col]).copy()

#             df_day["hazard_type"] = df_day[day_col].apply(detect_hazard_type)
#             df_day = df_day.dropna(subset=["hazard_type"])

#             for hazard_type, group in df_day.groupby("hazard_type"):

#                 severity = COLOR_SEVERITY_MAP.get(int(group[color_col].iloc[0]), "Low")

#                 districts = sorted(set(group["district"].dropna().astype(str)))
#                 if not districts:
#                     continue

#                 hazard_value = ", ".join(sorted(set(group[day_col].dropna())))

#                 hazard_records[hazard_type].append(
#                     {
#                         "indus_circle": indus_circle,
#                         "days": f"Day{day}",
#                         "date": (base_date + timedelta(days=day - 1)).date(),
#                         "district": ", ".join(districts),
#                         "hazard_value": hazard_value,
#                         "description": hazard_value,
#                         "severity": severity,
#                         "insert_at": now_ts,
#                     }
#                 )

#     return hazard_records


# endregion ========================================================================


# region ============================ FLOOD DATA ===============================
@log_execution
def read_district_geometry_gdf():
    engine = db_engine()
    schema = "weatherdata"
    table_name = "district_geometry"

    query = f"""
        SELECT district, indus_circle, geometry FROM weatherdata.district_geometry 
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

    return gdf


@log_execution
def get_flood_severity(condition, data_type):
    if condition == "Extreme":
        return "Extreme" if data_type == "flood_level" else "High"
    if condition == "Severe":
        return "High" if data_type == "flood_level" else "Moderate"
    if condition == "Above Normal":
        return "Low"
    return None


@log_execution
def build_flood_forecast_gdf():
    urls_flood = [
        f"https://aff.india-water.gov.in/textdata/Floodday{d}.geojson"
        for d in range(1, 8)
    ]
    urls_inflow = [
        f"https://aff.india-water.gov.in/textdata/Floodday{d}I.geojson"
        for d in range(1, 8)
    ]

    rows = []

    for day in range(1, 8):
        for url, dtype in (
            (urls_flood[day - 1], "flood_level"),
            (urls_inflow[day - 1], "inflow"),
        ):
            r = requests.get(url, timeout=30)
            if r.status_code != 200:
                continue

            for feature in r.json().get("features", []):
                p = feature.get("properties", {})
                g = feature.get("geometry", {})
                c = g.get("coordinates", [None, None])

                inflow_val = None
                if dtype == "inflow" and p.get("Inflow"):
                    try:
                        inflow_val = float(p.get("Inflow"))
                    except ValueError:
                        inflow_val = None

                condition = (
                    p.get("FloodCondition", "Unknown")
                    if dtype == "flood_level"
                    else (
                        "Extreme"
                        if inflow_val and inflow_val > 500
                        else (
                            "Severe"
                            if inflow_val and inflow_val >= 350
                            else (
                                "Above Normal"
                                if inflow_val and inflow_val >= 200
                                else "Normal" if inflow_val else "Unknown"
                            )
                        )
                    )
                )

                rows.append(
                    {
                        "site_id": p.get("id"),
                        "site_name": p.get("SiteName"),
                        "river": p.get("river"),
                        "district": p.get("District"),
                        "state": p.get("State"),
                        "forecast_day": day,
                        "data_type": dtype,
                        "value": inflow_val,
                        "condition": condition,
                        "geometry": Point(c[0], c[1]) if c and None not in c else None,
                        "fetched_at": datetime.utcnow(),
                    }
                )

    df = pd.DataFrame(rows)

    flood_gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    return flood_gdf


@log_execution
def build_flood_daywise_hazards(
    flood_gdf: gpd.GeoDataFrame, district_gdf: gpd.GeoDataFrame
):
    """
    Builds day-wise flood hazards.
    Returns empty DataFrame safely if no valid data exists.
    """

    # -------------------------------
    # 1. Ensure CRS
    # -------------------------------
    if flood_gdf.crs is None:
        flood_gdf = flood_gdf.set_crs("EPSG:4326", allow_override=True)
    elif flood_gdf.crs.to_string() != "EPSG:4326":
        flood_gdf = flood_gdf.to_crs("EPSG:4326")

    if district_gdf.crs is None:
        district_gdf = district_gdf.set_crs("EPSG:4326", allow_override=True)
    elif district_gdf.crs.to_string() != "EPSG:4326":
        district_gdf = district_gdf.to_crs("EPSG:4326")

    # -------------------------------
    # 2. Spatial join
    # -------------------------------
    final_flood_gdf = gpd.sjoin(
        flood_gdf,
        district_gdf[["district", "indus_circle", "geometry"]],
        how="left",
        predicate="within",
    )

    # 🔒 KEEP ONLY VALID DISTRICT + INDUS CIRCLE
    final_flood_gdf = final_flood_gdf[
        final_flood_gdf["district_right"].notna()
        & final_flood_gdf["indus_circle"].notna()
    ]

    # 🚨 EARLY EXIT
    if final_flood_gdf.empty:
        return pd.DataFrame()

    # -------------------------------
    # 3. Filter alert conditions
    # -------------------------------
    final_flood_gdf = final_flood_gdf[
        final_flood_gdf["condition"].notna()
        & (~final_flood_gdf["condition"].isin(["Normal", "Unknown"]))
    ]

    # 🚨 EARLY EXIT
    if final_flood_gdf.empty:
        return pd.DataFrame()

    # -------------------------------
    # 4. Compute severity
    # -------------------------------
    final_flood_gdf["severity"] = final_flood_gdf.apply(
        lambda r: get_flood_severity(r["condition"], r["data_type"]), axis=1
    )

    final_flood_gdf = final_flood_gdf[final_flood_gdf["severity"].notna()]

    # 🚨 EARLY EXIT
    if final_flood_gdf.empty:
        return pd.DataFrame()

    # -------------------------------
    # 5. Aggregate → day-wise hazards
    # -------------------------------
    alerts = final_flood_gdf.groupby(
        ["indus_circle", "forecast_day", "severity"], as_index=False
    ).agg({"district": lambda x: ", ".join(sorted(set(x.astype(str))))})

    if alerts.empty:
        return pd.DataFrame()

    # -------------------------------
    # 6. Build full day-wise matrix
    # -------------------------------
    today = datetime.utcnow().date()
    final_rows = []
    now_ts = datetime.now()

    all_circles = district_gdf["indus_circle"].dropna().unique().tolist()

    for circle in all_circles:
        for day in range(1, 8):
            match = alerts[
                (alerts["indus_circle"] == circle) & (alerts["forecast_day"] == day)
            ]

            if not match.empty:
                r = match.iloc[0]
                final_rows.append(
                    {
                        "days": f"Day{day}",
                        "date": (today + timedelta(days=day - 1)).strftime("%Y-%m-%d"),
                        "indus_circle": circle,
                        "district": r["district"],
                        "hazard_value": r["severity"],
                        "description": f"{r['severity']} flood/inflow risk",
                        "severity": r["severity"],
                        "insert_at": now_ts,
                    }
                )

    final_df = pd.DataFrame(final_rows)

    if final_df.empty:
        return pd.DataFrame()

    final_df = final_df.sort_values(["indus_circle", "days"]).reset_index(drop=True)

    final_df.to_json("final_df.json", orient="records", indent=2)

    return final_df


# endregion =======================================================================


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
def insert_hazards_forecast(hazard_records):
    engine = db_engine()

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


# =========== PROCESSING THE DATA =====================================
@log_execution
def process_hazards():
    gdf_hazard = scrap_hazard_master_data_insert_to_db()
    gdf_districts = read_district_geometry_gdf_and_save_json()

    final_gdf = create_district_hazard_geojson(
        gdf_districts=gdf_districts,
        gdf_hazard=gdf_hazard,
    )

    return final_gdf


# =======================================================================


if __name__ == "__main__":
    try:
        logger.info("Hazard processing started")

        final_gdf = process_hazards()
        hazard_records = build_hazard_records(final_gdf)

        district_gdf = read_district_geometry_gdf()
        flood_gdf = build_flood_forecast_gdf()

        final_flood_df = build_flood_daywise_hazards(flood_gdf, district_gdf)

        if final_flood_df is not None and not final_flood_df.empty:
            hazard_records["flood"] = final_flood_df.to_dict(orient="records")

        insert_hazards_forecast(hazard_records)

        logger.info("Hazard data saved successfully")

    except Exception as pe:
        logger.exception("Fatal error in main execution")
