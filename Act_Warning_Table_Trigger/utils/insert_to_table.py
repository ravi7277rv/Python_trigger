import json

from sqlalchemy import text

from utils.db import get_cris_engine

INSERT_SQL = """
INSERT INTO weatherdata.act_warning1 (
    district,
    shape_leng,
    shape_area,
    state,
    remarks,
    state_lgd,
    layer,
    id_val,
    "Date",
    "UTC",
    "DISTRICT_1",
    "Day_1",
    "Day_2",
    "Day_3",
    "Day_4",
    "Day_5",
    day1_color,
    day2_color,
    day3_color,
    day4_color,
    day5_color,
    "Day1_text",
    "Day2_text",
    "Day3_text",
    "Day4_text",
    "Day5_text",
    geom_json,
    geom,
    indus_district,
    indus_circle,
    insert_at,
    day1_severity,
    day2_severity,
    day3_severity,
    day4_severity,
    day5_severity
)
VALUES (
    :district,
    :shape_leng,
    :shape_area,
    :state,
    :remarks,
    :state_lgd,
    :layer,
    :id_val,
    :date,
    :utc,
    :district_1,
    :day_1,
    :day_2,
    :day_3,
    :day_4,
    :day_5,
    :day1_color,
    :day2_color,
    :day3_color,
    :day4_color,
    :day5_color,
    :day1_text,
    :day2_text,
    :day3_text,
    :day4_text,
    :day5_text,
    :geom_json,
    ST_SetSRID(ST_GeomFromText(:geom_wkt), 4326),
    :indus_district,
    :indus_circle,
    :insert_at,
    :day1_severity,
    :day2_severity,
    :day3_severity,
    :day4_severity,
    :day5_severity
);
"""


def insert_act_warning1(joined_gdf, logger):
    try:
        engine = get_cris_engine()
        with engine.begin() as conn:
            # 1️⃣ Truncate table first
            logger.info("✅ Table weatherdata.act_warning1 truncated successfully")
            conn.execute(text("TRUNCATE TABLE weatherdata.act_warning1;"))
            logger.info("✅ Table weatherdata.act_warning1 truncated successfully")

            # 2️⃣ Insert rows
            logger.info("✅ Table weatherdata.act_warning1 Insertion of data started")
            for idx, row in joined_gdf.iterrows():
                geom = row.geometry

                conn.execute(
                    text(INSERT_SQL),
                    {
                        "district": row.get("district"),
                        "shape_leng": row.get("Shape_Leng"),
                        "shape_area": row.get("Shape_Area"),
                        "state": row.get("state"),
                        "remarks": row.get("remarks"),
                        "state_lgd": row.get("State_LGD"),
                        "layer": row.get("layer"),
                        "id_val": None,
                        "date": row.get("date"),
                        "utc": row.get("utc"),
                        "district_1": row.get("district_1"),
                        "day_1": row.get("day_1"),
                        "day_2": row.get("day_2"),
                        "day_3": row.get("day_3"),
                        "day_4": row.get("day_4"),
                        "day_5": row.get("day_5"),
                        "day1_color": row.get("day1_color"),
                        "day2_color": row.get("day2_color"),
                        "day3_color": row.get("day3_color"),
                        "day4_color": row.get("day4_color"),
                        "day5_color": row.get("day5_color"),
                        "day1_text": row.get("day1_text"),
                        "day2_text": row.get("day2_text"),
                        "day3_text": row.get("day3_text"),
                        "day4_text": row.get("day4_text"),
                        "day5_text": row.get("day5_text"),
                        "geom_json": json.dumps(geom.__geo_interface__),
                        "geom_wkt": geom.wkt,
                        "indus_district": row.get("indus_district"),
                        "indus_circle": row.get("indus_circle"),
                        "insert_at": row.get("inserted_at"),
                        "day1_severity": row.get("day1_severity"),
                        "day2_severity": row.get("day2_severity"),
                        "day3_severity": row.get("day3_severity"),
                        "day4_severity": row.get("day4_severity"),
                        "day5_severity": row.get("day5_severity"),
                    },
                )

            logger.info(f"✅ Insertion completed successfully.")

    except Exception as e:
        logger.info(
            f"❌ Error occurred during insertion into weatherdata.act_warning1,{e}"
        )
