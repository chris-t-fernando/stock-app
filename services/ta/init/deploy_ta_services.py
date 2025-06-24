import os
from pathlib import Path
import subprocess
import yaml


from pubsub_wrapper.config import load_config


def main():
    env = os.getenv("STOCKAPP_ENV", "devtest")
    config = load_config(env)
    registry = config.get("container_registry", "")
    image = os.getenv(
        "TA_SERVICE_IMAGE",
        f"{registry}/ta-service:latest" if registry else "ta-service:latest",
    )
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

    chart_dir = Path(__file__).resolve().parents[1] / "helm"
    subprocess.run(
        [
            "helm",
            "upgrade",
            "--install",
            "ta-services",
            str(chart_dir),
            "-f",
            str(values_path),
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
