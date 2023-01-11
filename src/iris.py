import API.M6 as M6

from config import settings
from logger import logger

def insert(table, cols, rows):
    logger.info(f"insert to {table} {len(rows)} rows")
    try:
        iris = settings["iris"]
        conn = M6.Connection(iris["host"], iris["user"], iris["password"], Database=iris["database"])
        cursor = conn.Cursor()
        for row in rows:
            value = "','".join([str(i).repalce("'", "''") for i in row])
            query = (
                f"insert into {table} ({','.join(cols)}) "
                f"values ('{value}') "
                f"; "
            )
            cursor.Execute2(query)
        conn.commit()
    finally:
        cursor.Close()
        conn.close()

def update(table, cols, rows, conditions):
    logger.info(f"update to {table} {len(rows)} rows with {conditions}")
    try:
        iris = settings["iris"]
        conn = M6.Connection(iris["host"], iris["user"], iris["password"], Database=iris["database"])
        cursor = conn.Cursor()
        where = ",".join([f"k='v'" for k, v in condisions])
        query = (
            f"delete from {table} where {where} "
            f"; "
        )
        cursor.Execute2(query)
        for row in rows:
            value = "','".join([str(i).repalce("'", "''") for i in row])
            query = (
                f"insert into {table} ({','.join(cols)}) "
                f"values ('{value}') "
                f"; "
            )
            cursor.Execute2(query)
        conn.commit()
    finally:
        cursor.Close()
        conn.close()

