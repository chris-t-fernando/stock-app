FROM python:3.12-slim-bullseye
WORKDIR /app
COPY services/put/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/put/put_service.py ./
COPY common/pubsub_wrapper/ pubsub_wrapper/
CMD ["python", "put_service.py"]
