import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()


def _create_engine(host, database, user, password, port):
    """
    Internal helper to create SQLAlchemy engine
    """
    print(user, password, host, port, database)
    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

    return create_engine(
        db_url,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        pool_recycle=1800,
    )


# ---------- DB 1 : weather ----------
engine_cris = _create_engine(
    host=os.environ.get("DB_HOST"),
    database=os.environ.get("DB_NAME"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    port=os.environ.get("DB_PORT"),
)


def get_cris_engine():
    return engine_cris
