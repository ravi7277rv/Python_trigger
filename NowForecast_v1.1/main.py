import json

import requests

from utils.db import get_cris_engine, get_weather_engine
from utils.fetch_alert import fetch_alerts, get_subject_time
from utils.helper import exec_on_both
from utils.insert_now_cast import insert_nowcast
from utils.logger import setup_logger
from utils.send_mail import build_html, send_mail, send_no_alert_mail

# Table and Schema
SCHEMA = "weatherdata"
TABLE = "realtime_hazard_district"


# ======================================================================
# SQL
# ======================================================================

CREATE_SCHEMA_SQL = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"
CREATE_POSTGIS_SQL = "CREATE EXTENSION IF NOT EXISTS postgis;"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
    fid TEXT PRIMARY KEY,
    date DATE,
    message TEXT,
    toi INTEGER,
    vupto INTEGER,
    color INTEGER,
    update_time TIMESTAMP,
    district TEXT,
    indus_circle TEXT,
    geom GEOMETRY(MultiPolygon,4326)
);
"""

INSERT_SQL = f"""
INSERT INTO {SCHEMA}.{TABLE}
(fid, date, message, toi, vupto, color, update_time, geom)
VALUES (:id,:date,:message,:toi,:vupto,:color,:created_at,
ST_Multi(ST_GeomFromText(:geometry, 4326))
)
ON CONFLICT (fid) DO NOTHING;
"""

UPDATE_SPATIAL_SQL = f"""
UPDATE {SCHEMA}.{TABLE} n
SET district = p.district,
    indus_circle = p.indus_circle
FROM {SCHEMA}.district_geometry_point p
WHERE ST_Contains(n.geom, p.geom);
"""

DELETE_NO_CIRCLE_SQL = f"""
DELETE FROM {SCHEMA}.{TABLE}
WHERE indus_circle IS NULL;
"""


# ======================================================================
# WFS CONFIG
# ======================================================================

WFS_URL = (
    "https://reactjs.imd.gov.in/geoserver/imd/wfs?"
    "service=WFS&version=1.0.0&request=GetFeature"
    "&typeName=imd:NowcastWarningDistrict"
    "&outputFormat=application/json&srsName=EPSG:4326"
)

# Setting UP logger
logger = setup_logger()


# Main function
def main():

    try:
        logger.info("Fetching IMD Nowcast...")
        resp = requests.get(WFS_URL, timeout=60)
        resp.raise_for_status()
        features = resp.json().get("features", [])
    except Exception as e:
        logger.info(f"Failed to fetch IMD Nowcast: {e}")
        print(f"Failed to fetch IMD Nowcast: {e}")
        return

    logger.info("🗄️ Preparing DB...")
    print("🗄️ Preparing DB...")
    exec_on_both(CREATE_SCHEMA_SQL, logger)
    exec_on_both(CREATE_POSTGIS_SQL, logger)
    exec_on_both(CREATE_TABLE_SQL, logger)

    print("Inserting alerts...")
    logger.info("Inserting alerts...")
    insert_nowcast(features, get_weather_engine(), INSERT_SQL, logger)
    insert_nowcast(features, get_cris_engine(), INSERT_SQL, logger)

    logger.info("Spatial tagging...")
    print("Spatial tagging...")
    exec_on_both(UPDATE_SPATIAL_SQL, logger)
    exec_on_both(DELETE_NO_CIRCLE_SQL, logger)

    print("Evaluating alert trigger...")
    logger.info("Evaluating alert trigger...")
    rows = fetch_alerts(get_weather_engine(), SCHEMA, TABLE)

    if rows:
        subject_time = get_subject_time(get_weather_engine(), SCHEMA, TABLE)
        html = build_html(rows)
        logger.info("Sending mail......")
        send_mail(html, subject_time, logger)
        print("Severe alert email sent successfully")
        logger.info("Email sent successfully")

    else:
        # Optional: send "No Severe Weather" mail
        logger.info("Sending no alert mail...")
        subject_time = get_subject_time()
        send_no_alert_mail(subject_time)
        print("No severe weather alerts – informational mail sent")
        logger.info("No severe weather alerts – informational mail sent")


if __name__ == "__main__":
    logger.info("Nowforecast process has been started")
    main()
    logger.info("Nowforecast process has been ended")
    logger.info("Nowforecast process has been ended")
