# Stock App Infrastructure

This repository contains infrastructure and application code for the stock app.

Each service lives in the `services` directory with its own `requirements.txt` and `Dockerfile`.
## Requirements

- Python 3.10+
- [Helm](https://helm.sh/) installed and available in your `PATH`
- `kubectl` configured to access your cluster via `~/.kube/config`

## Deploying TA Services

Use `infra/init/deploy_ta_services.py` to deploy technical analysis services.
The script reads configuration from AWS SSM, writes a `values.yaml` file for the
Helm chart and then runs `helm upgrade --install`.

```bash
python infra/init/deploy_ta_services.py
```

Ensure Helm and `kubectl` are installed and your Kubernetes credentials are
available before running the script.
