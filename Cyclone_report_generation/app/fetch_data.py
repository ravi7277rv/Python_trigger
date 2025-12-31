import base64
import json
import os
from collections import defaultdict
from datetime import date, datetime, timedelta

import geopandas as gpd
import pandas as pd
import psycopg2
import pytz
import requests

from app import config
from app.db import db_connection

static_dir = os.path.join(os.path.dirname(__file__), "..", "static")


def date_format(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d %B %Y")
    return formatted_date


def get_today_temp_rh(district):
    conn = db_connection()
    sql = """select a.*, b.geometry from weatherdata.weather_imd a
                    left join weatherdata.imd_weather_towers b
                    on a.station_code = b."Station_Code"
                    WHERE TO_DATE(a.forecast_date, 'DD-Mon-YYYY') = CURRENT_DATE;"""
    gdf = gpd.read_postgis(sql, conn, geom_col="geometry")
    conn.dispose()

    region_geojson_path = f"{static_dir}/GeoJSON/Regions.geojson"
    region_gdf = gpd.read_file(region_geojson_path)
    filtered_region_gdf = region_gdf[region_gdf["NAME_LG"] == district]

    # filtered_region_gdf = filtered_region_gdf.to_crs(gdf.crs)
    points_inside = gpd.sjoin(
        gdf, filtered_region_gdf, how="inner", predicate="intersects"
    )
    return points_inside


def get_data():
    conn = db_connection()
    df = pd.read_sql(
        f"select * from weatherdata.disaster_ndma where event = 'Lightning'", conn
    )
    conn.dispose()
    return df


def fetch_district_barchart_data():
    # Simulated data fetch (replace with DB query as needed)
    data = {"Location": ["Delhi", "Mumbai", "Chennai"], "Value": [85, 70, 65]}
    df = pd.DataFrame(data)
    return df


def fetch_district_count_saverity_wise(circle):

    conn = db_connection()
    sql = f""" SELECT 
            days,
            severity_type,
            SUM(CASE WHEN severity = 'Extreme' THEN 1 ELSE 0 END) AS extreme,
            SUM(CASE WHEN severity = 'High' THEN 1 ELSE 0 END) AS high,
            SUM(CASE WHEN severity = 'Moderate' THEN 1 ELSE 0 END) AS moderate,
            SUM(CASE WHEN severity = 'Low' THEN 1 ELSE 0 END) AS low
        FROM (
            SELECT days, 'Temperature_Max' AS severity_type, temp_max_severity AS severity FROM weatherdata.district_wise_7dayfc_severity where indus_circle = '{circle}'
            UNION ALL
            SELECT days, 'Temperature_Min', temp_min_severity FROM weatherdata.district_wise_7dayfc_severity where indus_circle = '{circle}'
            UNION ALL
            SELECT days, 'Rainfall', rain_severity FROM weatherdata.district_wise_7dayfc_severity where indus_circle = '{circle}'
            UNION ALL
            SELECT days, 'Wind', wind_severity FROM weatherdata.district_wise_7dayfc_severity where indus_circle = '{circle}'
            UNION ALL
            SELECT days, 'Visibility', visibility_severity FROM weatherdata.district_wise_7dayfc_severity where indus_circle = '{circle}'
            UNION ALL
            SELECT days, 'Humidity', humidity_severity FROM weatherdata.district_wise_7dayfc_severity where indus_circle = '{circle}'
        ) AS severity_data
        GROUP BY days, severity_type
        ORDER BY days, severity_type;
        """

    df = pd.read_sql_query(sql, conn)

    merged = {}  # Temporary dict to merge per day

    for idx, row in df.iterrows():
        day = row["days"]
        severity = row["severity_type"]

        # Initialize day if not exists
        if day not in merged:
            merged[day] = {}

        # Add/merge severity data
        merged[day][severity] = {
            "extreme": row["extreme"],
            "high": row["high"],
            "moderate": row["moderate"],
            "low": row["low"],
        }

    severity_color = fetch_kpi_severity_control(circle)
    return merged, severity_color


def fetch_district_names_saverity_wise_7days(circle):
    conn = db_connection()
    sql = f""" 
           SELECT 
                days,
                "date",
                severity_type,
                STRING_AGG(CASE WHEN severity = 'Extreme' THEN district END, ', ') AS extreme_districts,
                STRING_AGG(CASE WHEN severity = 'High' THEN district END, ', ') AS high_districts,
                STRING_AGG(CASE WHEN severity = 'Moderate' THEN district END, ', ') AS moderate_districts,
                STRING_AGG(CASE WHEN severity = 'Low' THEN district END, ', ') AS low_districts
            FROM (
                SELECT days,"date", district, 'Temperature_Max' AS severity_type, temp_max_severity AS severity 
                FROM weatherdata.district_wise_7dayfc_severity 
                WHERE indus_circle = '{circle}'
                
                UNION ALL
                
                SELECT days,"date", district, 'Temperature_Min', temp_min_severity 
                FROM weatherdata.district_wise_7dayfc_severity 
                WHERE indus_circle = '{circle}'
                
                UNION ALL
                
                SELECT days,"date", district, 'Rainfall', rain_severity 
                FROM weatherdata.district_wise_7dayfc_severity 
                WHERE indus_circle = '{circle}'
                
                UNION ALL
                
                SELECT days,"date", district, 'Wind', wind_severity 
                FROM weatherdata.district_wise_7dayfc_severity 
                WHERE indus_circle = '{circle}'
                
                UNION ALL
                
                SELECT days,"date", district, 'Visibility', visibility_severity 
                FROM weatherdata.district_wise_7dayfc_severity 
                WHERE indus_circle = '{circle}'
                
                UNION ALL
                
                SELECT days,"date", district, 'Humidity', humidity_severity 
                FROM weatherdata.district_wise_7dayfc_severity 
                WHERE indus_circle = '{circle}'
            ) AS severity_data
            GROUP BY days, severity_type,"date"
            ORDER BY days, severity_type;
        """

    df = pd.read_sql_query(sql, conn)

    data_dict = {}
    for _, row in df.iterrows():
        severity_type = row["severity_type"]
        day = row["days"]

        # Initialize nested levels
        data_dict.setdefault(severity_type, {})
        data_dict[severity_type].setdefault(day, {})

        # For each severity column, split by comma and strip spaces
        for sev_level in [
            "extreme_districts",
            "high_districts",
            "moderate_districts",
            "low_districts",
        ]:
            val = row.get(sev_level)
            if pd.notna(val) and val.strip():
                district_list = [d.strip() for d in val.split(",")]
            else:
                district_list = []
            data_dict[severity_type][day][sev_level] = district_list

        # # Optional: Convert to JSON string for output or saving
        # result_json = json.dumps(data_dict, indent=4, ensure_ascii=False)
    # print(data_dict["Rainfall"]["day1"]["low_districts"])

    # severity_color = fetch_kpi_severity_control(circle)
    return data_dict


def fetch_district_wise_KPI_values_7days(circle):
    conn = db_connection()
    sql = f""" 
           select * from weatherdata.district_wise_7dayfc_severity dwds where indus_circle = '{circle}';
        """

    df = pd.read_sql_query(sql, conn)

    result = []
    for district, group in df.groupby("district"):
        district_data = {"district": district}

        for _, row in group.iterrows():
            district_data[row["days"]] = {
                "date": row["date"],
                "temp_min": row["temp_min"],
                "temp_max": row["temp_max"],
                "rain_percent": row["rain_percent"],
                "rain_precip": row["rain_precip"],
                "wind": row["wind"],
                "visibility": row["visibility"],
                "humidity": row["humidity"],
                "temp_max_severity": row["temp_max_severity"],
                "temp_min_severity": row["temp_min_severity"],
                "rain_severity": row["rain_severity"],
                "wind_severity": row["wind_severity"],
                "visibility_severity": row["visibility_severity"],
                "humidity_severity": row["humidity_severity"],
                "indus_circle": row["indus_circle"],
            }
        result.append(district_data)

    return result


def fetch_accomudated_rainfall_next3days(circle):
    conn = db_connection()
    sql = f"""  select * from weatherdata.district_wise_accum_rainfall where indus_circle = '{circle}' order by district asc;"""

    df = pd.read_sql_query(sql, conn)
    data = df.to_dict(orient="records")

    return data


def fetch_kpi_severity_control(circle):
    conn = db_connection()
    sql = f"""select * from weatherdata.weather_kpi_controls wkc
            where indus_circle = '{circle}'; """

    df = pd.read_sql_query(sql, conn)
    data = df.to_dict(orient="records")

    return data[0]


def get_cyclone_data():
    conn = db_connection()

    # Today's start in IST
    ist = pytz.timezone("Asia/Kolkata")
    today_start = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)

    sql = """
        SELECT *
        FROM weatherdata.hazard_cyclone
        WHERE insert_at >= %s
    """

    df = pd.read_sql_query(sql, conn, params=(today_start,))

    cyclone_data = df.to_dict(orient="records")
    return cyclone_data


def prepare_cyclone_data(rows):
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = today + timedelta(days=3)
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999)

    grouped = {}

    for row in rows:
        raw_date = row.get("date")

        # Handle both Python date & string types
        if isinstance(raw_date, (datetime, date)):
            cyclone_date = datetime.combine(raw_date, datetime.min.time())
        else:
            try:
                cyclone_date = datetime.strptime(str(raw_date), "%Y-%m-%d")
            except:
                continue

        if not (today <= cyclone_date <= end_date):
            continue

        circle = (row.get("indus_circle") or "").strip()
        severity = (row.get("severity") or "other").strip()
        district_data = row.get("district") or ""

        if not circle or not severity:
            continue

        date_str = cyclone_date.strftime("%d-%b-%y")

        grouped.setdefault(circle, {})
        grouped[circle].setdefault(date_str, {})
        grouped[circle][date_str].setdefault(
            severity, {"description": row.get("description") or "", "districts": []}
        )

        for dist in district_data.split(","):
            dist = dist.strip()
            if dist:
                grouped[circle][date_str][severity]["districts"].append(dist)

    # Cleanup empty branches
    for circle in list(grouped.keys()):
        for date_key in list(grouped[circle].keys()):
            for sev in list(grouped[circle][date_key].keys()):
                if not grouped[circle][date_key][sev]["districts"]:
                    del grouped[circle][date_key][sev]

            if not grouped[circle][date_key]:
                del grouped[circle][date_key]

        if not grouped[circle]:
            del grouped[circle]

    return grouped


def get_mail_address(circle):
    conn = db_connection()
    sql = f"""select  name, mail, to_cc, team from weatherdata.master_users 
            where status = 'active' and indus_circle = '{circle}' and team = 'indus'; """
    df = pd.read_sql_query(sql, conn)

    to_mail_list = df[df["to_cc"] == "to"]["mail"].dropna().tolist()
    cc_mail_list = df[df["to_cc"] == "cc"]["mail"].dropna().tolist()

    return to_mail_list, cc_mail_list


def get_mobile_numbers(circle):
    conn = db_connection()
    sql = f"""select  name, mobile, team from weatherdata.master_users 
            where status = 'active' and mobile is not null and indus_circle = '{circle}' """
    df = pd.read_sql_query(sql, conn)

    mobile_list = df.to_dict(orient="records")

    return mobile_list


######### Fetch Hazards Data with severirty ##########
def fet_hazard_affected_districts(circle, hazard_type):
    conn = db_connection()
    table_map = {
        "Flood": "hazard_flood",
        "Cyclone": "hazard_cyclone",
        "Snowfall": "hazard_snowfall",
        "Avalanche": "hazard_avalanche",
        "Cloudburst": "hazard_cloudburst",
        "Lightning": " hazard_lightning",
        "Landslide": "hazard_landslide",
    }
    table_name = table_map.get(hazard_type, None)

    sql = f""" 
           select days, date, indus_circle, district, severity, '{hazard_type}' as hazard from weatherdata.{table_name}
                where DATE(insert_at) = CURRENT_DATE - 1 and indus_circle = '{circle}' and days = 'Day1';
          """
    df = pd.read_sql_query(sql, conn)

    if df.empty:
        return pd.DataFrame({"hazard": [hazard_type]})

    df["hazard"] = hazard_type

    # print(result)
    return df


def get_hazard_affected_districts(circle, hazard_type_list):
    final_result = []

    for hazard in hazard_type_list:
        df = fet_hazard_affected_districts(circle, hazard)

        severity_levels = ["Extreme", "High", "Moderate", "Low"]
        hazard_dict = {hazard: {sev: [] for sev in severity_levels}}
        if df.empty or set(df.columns) == {"hazard"}:
            final_result.append(hazard_dict)
            continue

        for _, row in df.iterrows():
            sev = row.get("severity")

            if not sev or sev not in severity_levels:
                continue

            districts = (
                row["district"].split(",")
                if isinstance(row["district"], str) and row["district"].strip()
                else []
            )

            hazard_dict[hazard][sev] = districts

        final_result.append(hazard_dict)

    final_obj = {}
    for item in final_result:
        final_obj.update(item)
    # print(final_obj)

    return final_obj


def get_hazards(circle, hazard_type):
    conn = db_connection()

    table_map = {
        "Flood": "hazard_flood",
        "Cyclone": "hazard_cyclone",
        "Snowfall": "hazard_snowfall",
        "Avalanche": "hazard_avalanche",
        "Cloudburst": "hazard_cloudburst",
        "Lightning": "hazard_lightning",
        "Landslide": "hazard_landslide",
    }
    table_name = table_map.get(hazard_type, None)

    sql = f""" 
            WITH days AS (
                SELECT 'Day1' AS day UNION ALL
                SELECT 'Day2' UNION ALL
                SELECT 'Day3' UNION ALL
                SELECT 'Day4' UNION ALL
                SELECT 'Day5' UNION ALL
                SELECT 'Day6' UNION ALL
                SELECT 'Day7'
            ),

            exploded AS (
                SELECT
                    hs.indus_circle,
                    hs.days,
                    hs.date,
                    TRIM(district_item) AS district,
                    hs.severity,
                    CASE hs.severity
                        WHEN 'Extreme'  THEN 1
                        WHEN 'High'     THEN 2
                        WHEN 'Moderate' THEN 3
                        WHEN 'Low'      THEN 4
                        ELSE 5
                    END AS sev_rank
                FROM weatherdata.{table_name} hs
                CROSS JOIN LATERAL (
                    SELECT unnest(string_to_array(hs.district, ',')) AS district_item
                ) u
                WHERE DATE(hs.insert_at) = CURRENT_DATE - 1
                AND hs.indus_circle = '{circle}'
                AND hs.district IS NOT NULL
                AND hs.district <> ''
            ),

            best_per_district AS (
                SELECT indus_circle, days, district, date, severity
                FROM (
                    SELECT
                        indus_circle,
                        days,
                        district,
                        date,
                        severity,
                        sev_rank,
                        ROW_NUMBER() OVER (PARTITION BY indus_circle, days, district ORDER BY sev_rank) AS rn
                    FROM exploded
                ) t
                WHERE rn = 1
            )

            SELECT
                a.district,
                a.indus_circle,
                d.day AS days,
                b.date,
                COALESCE(b.severity, 'Other') AS severity
            FROM weatherdata.district_geometry a
            CROSS JOIN days d
            LEFT JOIN best_per_district b
                ON b.indus_circle = a.indus_circle
            AND b.days = d.day
            AND b.district = a.district
            WHERE a.district <> 'Data Not Available'
            AND a.indus_circle = '{circle}'
            ORDER BY a.district, d.day;
        """

    df = pd.read_sql_query(sql, conn)

    result = []
    for district, group in df.groupby("district"):
        district_data = {"district": district}

        for _, row in group.iterrows():
            district_data[row["days"]] = {
                "date": row["date"],
                "severity": (
                    "No Risk"
                    if row["severity"] == "Other" or row["severity"] == "Low"
                    else f"{row['severity']} Risk"
                ),
                "for_color": row["severity"],
                "indus_circle": row["indus_circle"],
            }
        result.append(district_data)

    # print(result)
    return result


# Get One Pager Hazards
def get_district_hazards_wise(circle, hazard_type):
    conn = db_connection()

    table_map = {
        "Flood": "hazard_flood",
        "Cyclone": "hazard_cyclone",
        "Snowfall": "hazard_snowfall",
        "Avalanche": "hazard_avalanche",
        "Cloudburst": "hazard_cloudburst",
        "Lightning": "hazard_lightning",
        "Landslide": "hazard_landslide",
    }
    table_name = table_map.get(hazard_type, None)

    sql = f""" 
           WITH ranked AS (
                SELECT 
                    *,
                    CASE severity
                        WHEN 'Extreme' THEN 1
                        WHEN 'High'    THEN 2
                        WHEN 'Moderate' THEN 3
                        WHEN 'Low'     THEN 4
                        ELSE 5
                    END AS severity_rank
                FROM weatherdata.{table_name}
                WHERE DATE(insert_at) = CURRENT_DATE - 1
                AND indus_circle = '{circle}'
                AND district IS NOT NULL
                AND district <> ''  
            )
            SELECT DISTINCT ON (days)
                days, date, district, severity
            FROM ranked
            ORDER BY days, severity_rank;
            WITH ranked AS (
                SELECT 
                    *,
                    CASE severity
                        WHEN 'Extreme' THEN 1
                        WHEN 'High'    THEN 2
                        WHEN 'Moderate' THEN 3
                        WHEN 'Low'     THEN 4
                        ELSE 5
                    END AS severity_rank
                FROM weatherdata.{table_name}
                WHERE DATE(insert_at) = CURRENT_DATE - 1
                AND indus_circle = '{circle}'
                AND district IS NOT NULL
                AND district <> ''  
            )
            SELECT DISTINCT ON (days)
                days, date, district, severity
            FROM ranked where severity <> '' and severity is not null and severity <> 'Low'
            ORDER BY days, severity_rank;"""

    df = pd.read_sql_query(sql, conn)

    if df.empty:
        return pd.DataFrame({"hazard": [hazard_type]})

    df["hazard"] = hazard_type
    # result = df.to_dict(orient='records')

    return df


def get_onepager_district_hazards(circle, hazard_type_list):
    result = []
    for hazard in hazard_type_list:
        df = get_district_hazards_wise(circle, hazard)

        if set(df.columns) == {"hazard"}:
            result.append({"hazard": hazard})
            continue

        for hazard, group in df.groupby("hazard"):
            hazard = {"hazard": hazard}

            for _, row in group.iterrows():
                hazard[row["days"]] = {
                    "date": row["date"],
                    "districts": row["district"].split(","),
                    "for_color": row["severity"],
                }
            result.append(hazard)

    # print(result)
    return result
