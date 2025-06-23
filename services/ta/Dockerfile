FROM python:3.12-slim-bullseye
WORKDIR /app
COPY services/ta/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/ta/ta_service.py ./
COPY stocklib ./stocklib
ENV PYTHONPATH=/app
CMD ["python", "ta_service.py"]
