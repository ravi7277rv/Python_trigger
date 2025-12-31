import os

from dotenv import load_dotenv
from sqlalchemy import text

from utils.db import get_cris_engine, get_weather_engine

load_dotenv()

TABLE = os.environ.get("TABLE")
SCHEMA = os.environ.get("SCHEMA")

CREATE_TABLE_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};

CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
    id SERIAL PRIMARY KEY,

    bulletin_date DATE,
    issued_at_utc TIMESTAMP,
    based_on_utc TIMESTAMP,
    pdf_timestamp_ist TIMESTAMP,

    bay_cloud_summary TEXT,
    arabian_cloud_summary TEXT,
    remarks TEXT,

    bay_0_24 INT, bay_0_24_date DATE,
    bay_24_48 INT, bay_24_48_date DATE,
    bay_48_72 INT, bay_48_72_date DATE,
    bay_72_96 INT, bay_72_96_date DATE,
    bay_96_120 INT, bay_96_120_date DATE,
    bay_120_144 INT, bay_120_144_date DATE,
    bay_144_168 INT, bay_144_168_date DATE,

    arab_0_24 INT, arab_0_24_date DATE,
    arab_24_48 INT, arab_24_48_date DATE,
    arab_48_72 INT, arab_48_72_date DATE,
    arab_72_96 INT, arab_72_96_date DATE,
    arab_96_120 INT, arab_96_120_date DATE,
    arab_120_144 INT, arab_120_144_date DATE,
    arab_144_168 INT, arab_144_168_date DATE,

    created_at TIMESTAMP DEFAULT NOW()
);
"""


def create_table_if_not_exists(logger):
    engines = {
        "weather": get_weather_engine(),
        "cris": get_cris_engine(),
    }

    for name, engine in engines.items():
        try:
            with engine.begin() as conn:
                conn.execute(text(CREATE_TABLE_DDL))

            logger.info(f"Ensured table exists in {name} database")

        except Exception as e:
            logger.exception(
                "Error creating table in %s database: %s",
                name,
                e,
            )
            raise


def bulletin_exists(pdf_ts_ist, logger):
    query = text(
        f"""
        SELECT 1
        FROM {SCHEMA}.{TABLE}
        WHERE pdf_timestamp_ist = :pdf_ts_ist
        LIMIT 1
        """
    )

    engines = {
        "weather": get_weather_engine(),
        "cris": get_cris_engine(),
    }

    for name, engine in engines.items():
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    query,
                    {"pdf_ts_ist": pdf_ts_ist},
                ).fetchone()

                if result:
                    logger.info(
                        "Bulletin already exists in %s database.",
                        name,
                    )
                    return True

        except Exception as e:
            logger.exception(
                "Error checking bulletin existence in %s database: %s",
                name,
                e,
            )

    return False


def insert_bulletin_row(data, logger):
    cols = list(data.keys())
    col_sql = ", ".join(cols)
    bind_sql = ", ".join([f":{c}" for c in cols])

    sql = text(
        f"""
        INSERT INTO {SCHEMA}.{TABLE} ({col_sql})
        VALUES ({bind_sql})
        """
    )

    engines = {
        "weather": get_weather_engine(),
        "cris": get_cris_engine(),
    }

    for name, engine in engines.items():
        try:
            with engine.begin() as conn:
                conn.execute(sql, data)

            logger.info("Inserted bulletin into %s database", name)

        except Exception as e:
            logger.exception(
                "Failed insert into %s database: %s",
                name,
                e,
            )
            raise
