import boto3
import os

def load_config(env="devtest", prefix="/stockapp"):
    ssm = boto3.client("ssm", region_name=os.getenv("AWS_REGION", "ap-southeast-2"))
    keys = ["PGHOST", "PGUSER", "PGPASSWORD", "PGDATABASE", "PGPORT"]
    result = {}
    for key in keys:
        name = f"{prefix}/{env}/{key}"
        resp = ssm.get_parameter(Name=name, WithDecryption=True)
        result[key] = resp["Parameter"]["Value"]
    return result