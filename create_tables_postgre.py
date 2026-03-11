import psycopg2
import pandas as pd
import logging 
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
conn = None
cur = None 

def create_postgre_database(db_name, db_user, db_password, host, port):
    try:
        # Connect to default 'postgres' database to create a new database
        conn = psycopg2.connect(
            dbname="postgres",
            user=db_user,
            password=db_password,
            host=host,
            port=port
        )
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
        exists = cur.fetchone()

        if not exists:
            cur.execute(f"CREATE DATABASE {db_name};")
            logger.info(f"Database {db_name} created successfully.")
        else:
            logger.info(f"Database {db_name} already exists, skipping creation.")

    except Exception as e:
        logger.error(f"Error creating database: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def main():

    load_dotenv()
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    sql_file_path = "sql/create_tables.sql"

    create_postgre_database(db_name, db_user, db_password, host, port)

    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=host,
        port=port
    )

    try: 
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_database;")
        databases = cur.fetchall()
        for db in databases:
            logger.info(db[0])

        # Read the SQL file
        with open(sql_file_path, 'r') as f:
            sql_script = f.read()
        cur.execute(sql_script)
        conn.commit()
        logger.info("All CREATE TABLE statements executed successfully")
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'postgre_db'
        """)
        tables = cur.fetchall()
        for table in tables:
            logger.info(table[0])
    except Exception as e:
        conn.rollback()
        logger.error(f"Error executing SQL: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()