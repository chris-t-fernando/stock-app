FROM python:3.12-bullseye AS build-ta-lib

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

RUN wget https://github.com/ta-lib/ta-lib/releases/download/v0.6.4/ta-lib_0.6.4_amd64.deb
RUN dpkg -i ta-lib_0.6.4_amd64.deb

RUN pip install --upgrade --no-cache-dir pip setuptools wheel
COPY services/strategy/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ARG CACHEBUST=1
COPY services/strategy/strategy_service.py ./
COPY common/pubsub_wrapper/ pubsub_wrapper/

CMD ["python", "strategy_service.py"]
