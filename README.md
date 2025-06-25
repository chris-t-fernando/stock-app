# Stock App Infrastructure

This repository contains infrastructure and application code for the stock app.

Each service lives in the `services` directory with its own `requirements.txt` and `Dockerfile`.
The configuration stored in AWS SSM now includes a `container_registry` key which points to the local registry used by k3s (`k3sn1:32000`) and a `redis_url` key for the message bus.
## Requirements

- Python 3.10+
- [Helm](https://helm.sh/) installed and available in your `PATH`
- `kubectl` configured to access your cluster via `~/.kube/config`
- A Kubernetes secret named `aws-credentials` containing your AWS access key and secret

## Local Development

Install the shared pubsub wrapper package in editable mode:

```bash
pip install -e ./common/
```

## Init Scripts

Bootstrapping scripts for shared infrastructure live in the top level
`init` directory. These include parameter store setup and database schema
initialisation:

```bash
python init/init_ssm.py
python init/init_timescaledb.py
```

## Deploying TA Services

Use `services/ta/helm/deploy_ta_services.py` to deploy technical analysis services.
The script reads configuration from AWS SSM, writes a `values.yaml` file for the
Helm chart and then runs `helm upgrade --install`.
The Helm chart is located alongside the service code under `services/ta/helm`.

```bash
python services/ta/helm/deploy_ta_services.py
```

Ensure Helm and `kubectl` are installed and your Kubernetes credentials are
available before running the script.

### AWS Credentials Secret

The TA deployments expect a Kubernetes secret named `aws-credentials` containing
your AWS access key and secret key. You can create it with:

```bash
kubectl create secret generic aws-credentials \
  --from-literal=aws_access_key_id=YOUR_KEY_ID \
  --from-literal=aws_secret_access_key=YOUR_SECRET
```

## Building Service Images

Build each service image from the repository root so that the Docker build
context includes the shared `pubsub_wrapper` package. When building on ARM hardware,
use `buildx` to cross build for the x86 architecture and push the image to your
local registry. For example, to build and publish the TA service image:

```bash
docker buildx build --platform linux/amd64 \
  -t k3sn1:32000/ta-service:latest \
  -f services/ta/Dockerfile . --push
```

Adjust the registry prefix as needed for your environment.
