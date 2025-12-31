import os
from app import config

from sqlalchemy import (
    create_engine
)

host=config.DB_HOST
database=config.DB
port=config.DB_PORT
user=config.DB_USER
password=config.DB_PASSWORD

DB_URL = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

# engine = create_engine(DB_URL)

engine = create_engine(
DB_URL,
pool_size=10,
max_overflow=20,
pool_timeout=30,
pool_recycle=1800
)

def db_connection():
    return engine