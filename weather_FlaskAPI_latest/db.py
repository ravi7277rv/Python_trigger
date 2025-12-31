import os
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv
load_dotenv()  

db_pool = None
def init_db_pool():
    global db_pool
    if db_pool is None:
        db_pool = SimpleConnectionPool(
            1,                
            10,               
            host=os.environ.get("DB_HOST"),
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASS"),
            port=os.environ.get("DB_PORT", 5432)
        )
    return db_pool


def get_db_conn():
    pool = init_db_pool()
    return pool.getconn()   

def release_db_conn(conn):
    pool = init_db_pool()
    pool.putconn(conn)


# def db_connection():
#     try:
#         conn = psycopg2.connect(
#             host=os.environ.get("DB_HOST"),
#             database=os.environ.get("DB_NAME"),
#             user=os.environ.get("DB_USER"),
#             password=os.environ.get("DB_PASS"),
#             port=os.environ.get("DB_PORT", 5432)
#         )
#         return conn
#     except Exception as e:
#         print("Database connection error:", e)
#         return None