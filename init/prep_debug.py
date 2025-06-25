import os
import random
import subprocess
import time
import psycopg2
from pubsub_wrapper import load_config


def get_db_config(env: str):
    cfg = load_config(env)
    return {
        "dbname": cfg["PGDATABASE"],
        "user": cfg["PGUSER"],
        "password": cfg["PGPASSWORD"],
        "host": cfg["PGHOST"],
        "port": int(cfg["PGPORT"]),
    }, cfg


def delete_recent_rows(conn, table: str, ticker: str, interval: str, limit: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH recent AS (
                SELECT ts FROM {table}
                WHERE ticker = %s AND interval = %s
                ORDER BY ts DESC
                LIMIT %s
            )
            DELETE FROM {table}
            WHERE ticker = %s AND interval = %s AND ts IN (SELECT ts FROM recent)
            """,
            (ticker, interval, limit, ticker, interval),
        )
        return cur.rowcount


def clean_database(env: str):
    db_config, _ = get_db_config(env)
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT ticker, interval FROM stock_ohlcv")
            pairs = cur.fetchall()
        for ticker, interval in pairs:
            n = random.randint(10, 1000)
            deleted_ohlcv = delete_recent_rows(conn, "stock_ohlcv", ticker, interval, n)
            deleted_macd = delete_recent_rows(
                conn, "stock_ta_macd", ticker, interval, n
            )
            print(
                f"Deleted {deleted_ohlcv} ohlcv and {deleted_macd} macd rows for {ticker} ({interval})"
            )
        conn.commit()
    finally:
        conn.close()


def build_image(env: str):
    _, cfg = get_db_config(env)
    registry = cfg.get("container_registry", "")
    image = f"{registry}/ta-service:latest" if registry else "ta-service:latest"
    subprocess.run(
        [
            "docker",
            "buildx",
            "--platform",
            "linux/am64",
            "-t",
            image,
            "-f",
            "services/ta/Dockerfile",
            ".",
            "--push",
        ]
    )
    return image, cfg


def deploy(image: str, cfg):
    env = os.getenv("STOCKAPP_ENV", "devtest")
    os.environ["TA_SERVICE_IMAGE"] = image
    subprocess.run(["python", "services/ta/init/deploy_ta_services.py"], check=True)

    algos = cfg.get("TA", [])
    if not algos:
        algos = ["macd"]
    for alg in algos:
        subprocess.run(
            [
                "kubectl",
                "rollout",
                "status",
                f"deployment/ta-service-{alg}",
                "--timeout=120s",
            ],
            check=True,
        )


def main():
    env = os.getenv("STOCKAPP_ENV", "devtest")
    clean_database(env)
    image, cfg = build_image(env)
    deploy(image, cfg)


if __name__ == "__main__":
    main()
