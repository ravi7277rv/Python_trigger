import os
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine
)
from urllib.parse import quote_plus
load_dotenv()

db_user = os.getenv("DB_USER")
db_password = quote_plus(os.getenv("DB_PASSWORD")) 
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT")
DB_URL = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

# engine = create_engine(DB_URL)

engine = create_engine(
DB_URL,
pool_size=10,
max_overflow=20,
pool_timeout=30,
pool_recycle=1800
)

def db_engine():
    return engine