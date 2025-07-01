import os
import random
import subprocess
from pathlib import Path
import psycopg2
import yaml
import logging
from pubsub_wrapper import load_config, configure_json_logger

configure_json_logger()
logger = logging.getLogger(__name__)


def docker_build(image: str, dockerfile: str, context: str = "."):
    subprocess.run(
        [
            "docker",
            "buildx",
            "build",
            "--platform",
            "linux/amd64",
            "-t",
            image,
            "-f",
            dockerfile,
            context,
            "--push",
        ],
        check=True,
    )


def helm_upgrade_install(release: str, chart_dir: Path, values_path: Path):
    subprocess.run(
        [
            "helm",
            "upgrade",
            "--install",
            release,
            str(chart_dir),
            "-f",
            str(values_path),
        ],
        check=True,
    )


def rollout_restart(deployments: list[str]):
    for dep in deployments:
        subprocess.run(
            ["kubectl", "rollout", "restart", f"deployment/{dep}"],
            check=True,
        )


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
            logger.info(
                f"Deleted {deleted_ohlcv} ohlcv and {deleted_macd} macd rows for {ticker} ({interval})"
            )
        conn.commit()
    finally:
        conn.close()


def build_image(env: str, service: str, dockerfile: str) -> tuple[str, dict]:
    _, cfg = get_db_config(env)
    registry = cfg.get("container_registry", "")
    image = f"{registry}/{service}:latest" if registry else f"{service}:latest"
    docker_build(image, dockerfile)
    return image, cfg


def deploy_ta(image: str, cfg):
    env = os.getenv("STOCKAPP_ENV", "devtest")
    algos = cfg.get("TA", []) or ["macd"]

    values = {
        "image": image,
        "algorithms": algos,
        "replicas": 1,
        "env": env,
    }

    values_path = Path(__file__).resolve().parent / "ta_values.yaml"
    with values_path.open("w") as f:
        yaml.safe_dump(values, f)

    chart_dir = Path(__file__).resolve().parents[1] / "services/ta/helm"
    helm_upgrade_install("ta-services", chart_dir, values_path)

    rollout_restart([f"ta-service-{alg}" for alg in algos])

    subprocess.run(["kubectl", "get", "pods"], check=True)


def deploy_put(image: str, cfg):
    env = os.getenv("STOCKAPP_ENV", "devtest")
    intervals = ["1m", "5m", "1h", "1d"]

    values = {
        "image": image,
        "intervals": intervals,
        "replicas": 1,
        "env": env,
    }

    values_path = Path(__file__).resolve().parent / "put_values.yaml"
    with values_path.open("w") as f:
        yaml.safe_dump(values, f)

    chart_dir = Path(__file__).resolve().parents[1] / "services/put/helm"
    helm_upgrade_install("put-services", chart_dir, values_path)

    rollout_restart([f"put-service-{i}" for i in intervals])

    subprocess.run(["kubectl", "get", "pods"], check=True)


def deploy_strategy(image: str, cfg):
    env = os.getenv("STOCKAPP_ENV", "devtest")
    strategies = cfg.get("STRATEGIES", [])

    values = {
        "image": image,
        "strategies": strategies,
        "replicas": 1,
        "env": env,
    }

    values_path = Path(__file__).resolve().parent / "strategy_values.yaml"
    with values_path.open("w") as f:
        yaml.safe_dump(values, f)

    chart_dir = Path(__file__).resolve().parents[1] / "services/strategy/helm"
    helm_upgrade_install("strategy-services", chart_dir, values_path)

    rollout_restart([f"strategy-service-{s.lower().replace('_','-')}" for s in strategies])

    subprocess.run(["kubectl", "get", "pods"], check=True)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Prepare debug environment")
    # parser.add_argument("--service", choices=["ta", "put"], default="ta")
    # args = parser.parse_args()

    env = os.getenv("STOCKAPP_ENV", "devtest")
    clean_database(env)

    image, cfg = build_image(env, "ta-service", "services/ta/Dockerfile")
    deploy_ta(image, cfg)

    image, cfg = build_image(env, "put-service", "services/put/Dockerfile")
    deploy_put(image, cfg)

    image, cfg = build_image(env, "strategy-service", "services/strategy/Dockerfile")
    deploy_strategy(image, cfg)


if __name__ == "__main__":
    main()
