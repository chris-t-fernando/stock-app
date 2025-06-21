import os
import subprocess
from app.stocklib.config import load_config


def main():
    env = os.getenv("STOCKAPP_ENV", "devtest")
    config = load_config(env)
    ta_list = config.get("TA", [])
    image = os.getenv("TA_SERVICE_IMAGE", "ta-service:latest")

    for ta in ta_list:
        name = f"ta-service-{ta}"
        cmd = [
            "kubectl",
            "run",
            name,
            "--image",
            image,
            "--restart=Always",
            "--",
            "python",
            "app/services/ta_service.py",
            "-ta_name",
            ta,
        ]
        print(f"Starting {name}...")
        subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
