import os
from pathlib import Path
import subprocess
import yaml

from pubsub_wrapper import load_config


def main():
    env = os.getenv("STOCKAPP_ENV", "devtest")
    cfg = load_config(env)
    registry = cfg.get("container_registry", "")
    image = os.getenv(
        "STRATEGY_SERVICE_IMAGE",
        f"{registry}/strategy-service:latest" if registry else "strategy-service:latest",
    )
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

    chart_dir = Path(__file__).resolve().parent
    subprocess.run(
        [
            "helm",
            "upgrade",
            "--install",
            "strategy-services",
            str(chart_dir),
            "-f",
            str(values_path),
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
