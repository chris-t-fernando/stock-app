import os
from pathlib import Path
import subprocess
import sys
import yaml

sys.path.append(str(Path(__file__).resolve().parents[2] / "app"))

from stocklib.config import load_config


def main():
    env = os.getenv("STOCKAPP_ENV", "devtest")
    image = os.getenv("TA_SERVICE_IMAGE", "ta-service:latest")
    config = load_config(env)
    algos = config.get("TA", [])

    values = {
        "image": image,
        "algorithms": algos,
        "replicas": 1,
        "env": env,
    }

    values_path = Path(__file__).resolve().parent / "ta_values.yaml"
    with values_path.open("w") as f:
        yaml.safe_dump(values, f)

    chart_dir = Path(__file__).resolve().parents[1] / "helm" / "ta-service"
    subprocess.run(
        ["helm", "upgrade", "--install", "ta-services", str(chart_dir), "-f", str(values_path)],
        check=True,
    )


if __name__ == "__main__":
    main()
