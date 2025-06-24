FROM python:3.12-slim-bullseye
WORKDIR /app
COPY services/order/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/order/order_service.py ./
COPY common/pubsub_wrapper/ pubsub_wrapper/
CMD ["python", "order_service.py"]
