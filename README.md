# Stock App Infrastructure

This repository contains infrastructure and application code for the stock app.

Each service lives in the `services` directory with its own `requirements.txt` and `Dockerfile`.
The configuration stored in AWS SSM now includes a `container_registry` key which points to the local registry used by k3s (`k3sn1:32000`).
## Requirements

- Python 3.10+
- [Helm](https://helm.sh/) installed and available in your `PATH`
- `kubectl` configured to access your cluster via `~/.kube/config`

## Deploying TA Services

Use `services/ta/init/deploy_ta_services.py` to deploy technical analysis services.
The script reads configuration from AWS SSM, writes a `values.yaml` file for the
Helm chart and then runs `helm upgrade --install`.
The Helm chart is located alongside the service code under `services/ta/helm`.

```bash
python services/ta/init/deploy_ta_services.py
```

Ensure Helm and `kubectl` are installed and your Kubernetes credentials are
available before running the script.

## Building Service Images

Build each service image from the repository root so that the Docker build
context includes the shared `stocklib` package. For example, to build the TA
service image that will be pushed to the local registry:

```bash
docker build -t k3sn1:32000/ta-service:latest -f services/ta/Dockerfile .
```

Adjust the registry prefix as needed for your environment.
