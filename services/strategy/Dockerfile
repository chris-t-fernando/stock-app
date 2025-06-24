FROM python:3.12-slim-bullseye
WORKDIR /app
COPY services/strategy/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/strategy/strategy_service.py ./
COPY common/pubsub_wrapper/ pubsub_wrapper/
CMD ["python", "strategy_service.py"]
