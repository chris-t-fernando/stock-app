import boto3
import os
import json

def load_config(env="devtest", prefix="/stockapp"):
    ssm = boto3.client("ssm", region_name=os.getenv("AWS_REGION", "ap-southeast-2"))
    keys = [
        "PGHOST",
        "PGUSER",
        "PGPASSWORD",
        "PGDATABASE",
        "PGPORT",
        "symbols",
        "TA",
        "container_registry",
        "redis_url",
    ]
    result = {}
    for key in keys:
        name = f"{prefix}/{env}/{key}"
        resp = ssm.get_parameter(Name=name, WithDecryption=True)
        value = resp["Parameter"]["Value"]
        try:
            result[key] = json.loads(value)
        except json.JSONDecodeError:
            result[key] = value
    return result
