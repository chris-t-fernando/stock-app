# Stage 1: Build TA-Lib C library
FROM python:3.12-bullseye AS build-ta-lib

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

RUN wget https://github.com/ta-lib/ta-lib/releases/download/v0.6.4/ta-lib_0.6.4_amd64.deb
RUN dpkg -i ta-lib_0.6.4_amd64.deb

RUN pip install --upgrade --no-cache-dir pip setuptools wheel

# Copy requirements to leverage cache
COPY services/ta/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY services/ta/ta_service.py ./
COPY common/pubsub_wrapper/ pubsub_wrapper/

CMD ["python", "ta_service.py"]
