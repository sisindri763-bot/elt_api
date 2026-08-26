import pymysql
from fastapi import HTTPException
from app.core.config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from typing import List, Dict, Any

def get_db():
    try:
        return pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset="utf8mb4",
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

def query(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except Exception as e:
        print(f"SQL Error: {e} | Query: {sql} | Params: {params}")
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")
    finally:
        conn.close()
