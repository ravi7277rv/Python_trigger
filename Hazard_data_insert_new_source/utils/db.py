import os
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()


def _create_engine(host, database, user, password, port):
    """
    Internal helper to create SQLAlchemy engine
    """

    print(user, password, host, port, database)
    password = quote_plus(password)
    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

    return create_engine(
        db_url,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        pool_recycle=1800,
    )


# ---------- DB 1 : weather ----------
# engine_weather = _create_engine(
#     host=os.environ.get("DB_HOST_2"),
#     database=os.environ.get("DB_NAME_2"),
#     user=os.environ.get("DB_USER"),
#     password=os.environ.get("DB_PASSWORD"),
#     port=os.environ.get("DB_PORT"),
# )


# ---------- DB 2 : cris ----------
engine_cris = _create_engine(
    host=os.environ.get("DB_HOST"),
    database=os.environ.get("DB_NAME"),
    user=os.environ.get("DB_USER"),  # keep separate users if needed
    password=os.environ.get("DB_PASSWORD"),
    port=os.environ.get("DB_PORT"),
)


# ---------- Public functions ----------
# def get_weather_engine():
#     return engine_weather


def get_cris_engine():
    return engine_cris
