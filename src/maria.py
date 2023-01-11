import pymysql

from config import settings
from logger import logger

def fetch(query):
    logger.info(f"fetch {query}")
    maria = settings["maria"]
    with pymysql.connect(**maria) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            cols = [i[0] for i in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
    return cols, rows

