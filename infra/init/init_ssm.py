import boto3
import os

DEFAULT_CONFIG = {
    "PGHOST": "timescaledb",
    "PGUSER": "admin",
    "PGPASSWORD": "secret",
    "PGDATABASE": "stockdata",
    "PGPORT": "30432"
}

def put_parameters(env="devtest", prefix="/stockapp", region="ap-southeast-2"):
    ssm = boto3.client("ssm", region_name=region)

    for key, value in DEFAULT_CONFIG.items():
        param_path = f"{prefix}/{env}/{key}"
        print(f"Setting {param_path}...")
        ssm.put_parameter(
            Name=param_path,
            Value=value,
            Type="SecureString" if "PASSWORD" in key else "String",
            Overwrite=True
        )
    print("✅ SSM parameters bootstrapped.")

if __name__ == "__main__":
    env = os.getenv("STOCKAPP_ENV", "devtest")
    region = os.getenv("AWS_REGION", "ap-southeast-2")
    put_parameters(env=env, region=region)
