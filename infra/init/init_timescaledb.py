# infra/init/init_timescaledb.py
import boto3
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from stocklib.config import load_config

ENV = os.getenv("STOCKAPP_ENV", "devtest")
config = load_config(ENV)

DB_CONFIG = {
    "dbname": config["PGDATABASE"],
    "user": config["PGUSER"],
    "password": config["PGPASSWORD"],
    "host": config["PGHOST"],
    "port": int(config["PGPORT"])
}

def connect(db=DB_CONFIG["dbname"], autocommit=False):
    conn = psycopg2.connect(
        dbname=db,
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"]
    )
    if autocommit:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

def ensure_database_exists():
    conn = connect(db="postgres", autocommit=True)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_CONFIG["dbname"],))
    exists = cur.fetchone()
    if not exists:
        print(f"Creating database {DB_CONFIG['dbname']}...")
        cur.execute(f"CREATE DATABASE {DB_CONFIG['dbname']};")
    cur.close()
    conn.close()

def init_schema():
    conn = connect()
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_ohlcv (
            ticker    TEXT        NOT NULL,
            interval  TEXT        NOT NULL,
            ts        TIMESTAMPTZ NOT NULL,
            open      DOUBLE PRECISION,
            high      DOUBLE PRECISION,
            low       DOUBLE PRECISION,
            close     DOUBLE PRECISION,
            volume    BIGINT,
            PRIMARY KEY (ticker, interval, ts)
        );
    """)
    cur.execute("""
        SELECT create_hypertable('stock_ohlcv', 'ts', if_not_exists => TRUE);
    """)
    cur.execute("ALTER TABLE stock_ohlcv SET (timescaledb.compress);")
    cur.execute("""
        SELECT add_compression_policy('stock_ohlcv', INTERVAL '30 days')
        ON CONFLICT DO NOTHING;
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("TimescaleDB schema initialised with compression.")

if __name__ == "__main__":
    ensure_database_exists()
    init_schema()