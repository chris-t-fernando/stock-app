import boto3
import os
import json

DEFAULT_CONFIG = {
    "PGHOST": "k3sn1",
    "PGUSER": "admin",
    "PGPASSWORD": "secret",
    "PGDATABASE": "stockdata",
    "PGPORT": "30432",
    "symbols": [
        ("AAPL", "1m"),
        ("GOOGL", "1m"),
        ("AMZN", "5m"),
        ("AMZN", "1m"),
        ("AMZN", "1h"),
        ("ACN", "5m"),
        ("ACN", "1m"),
        ("ACN", "1h"),
    ],
    "TA": ["macd", "rsi", "sma", "bollingerbands", "obv"],
    "STRATEGIES": [
        "trend_follow_confirmation",
        "rsi_pullback",
        "macd_rsi",
        "bollinger_momentum",
        "triple_confirmation",
        "adx_macd",
        "golden_cross",
    ],
    # Registry that hosts our container images for k3s
    "container_registry": "k3sn1:32000",
    # Redis connection URL used by services
    "redis_url": "redis://k3sn1:30379",
}


def put_parameters(env="devtest", prefix="/stockapp", region="ap-southeast-2"):
    ssm = boto3.client("ssm", region_name=region)

    for key, value in DEFAULT_CONFIG.items():
        param_path = f"{prefix}/{env}/{key}"
        print(f"Setting {param_path}...")
        ssm.put_parameter(
            Name=param_path,
            Value=json.dumps(value),
            Type="SecureString" if "PASSWORD" in key else "String",
            Overwrite=True,
        )
    print("✅ SSM parameters bootstrapped.")


if __name__ == "__main__":
    env = os.getenv("STOCKAPP_ENV", "devtest")
    region = os.getenv("AWS_REGION", "ap-southeast-2")
    put_parameters(env=env, region=region)
