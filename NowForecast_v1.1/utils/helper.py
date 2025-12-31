from sqlalchemy import text

from utils.db import get_cris_engine, get_weather_engine

# ======================================================================
# CATEGORY → MESSAGE MAP
# ======================================================================

CAT_MESSAGE_MAP = {
    "cat2": "Light Rain",
    "cat3": "Light Snowfall",
    "cat4": "Light Thunderstorms",
    "cat5": "Slight Dust Storm",
    "cat6": "Low Probability of Lightning",
    "cat7": "Moderate Rainfall",
    "cat8": "Moderate Snowfall",
    "cat9": "Moderate Thunderstorms",
    "cat10": "Moderate Dust Storm",
    "cat11": "Moderate Probability of Lightning",
    "cat12": "Heavy Rainfall",
    "cat13": "Heavy Snowfall",
    "cat14": "Severe Thunderstorms",
    "cat15": "Very Severe Thunderstorms",
    "cat17": "Thunderstorms with Hail",
    "cat18": "Severe Dust Storm",
    "cat19": "Very High Probability of Lightning",
}


def exec_sql(engine, sql):
    with engine.begin() as conn:
        conn.execute(text(sql))


def exec_on_both(sql, logger):
    logger.info("Execution of query on db1")
    exec_sql(get_weather_engine(), sql)
    logger.info("Execution of query on db2")
    exec_sql(get_cris_engine(), sql)


def clean_int(v):
    try:
        return int(v)
    except Exception:
        return None


def build_message(props):
    for cat, msg in CAT_MESSAGE_MAP.items():
        if clean_int(props.get(cat)):
            return msg
    return None
