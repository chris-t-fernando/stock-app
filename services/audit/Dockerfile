FROM python:3.12-slim-bullseye
WORKDIR /app
COPY services/audit/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/audit/audit_service.py ./
COPY common/pubsub_wrapper/ pubsub_wrapper/
CMD ["python", "audit_service.py"]
