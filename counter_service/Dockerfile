FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8002
# Keep a single worker to avoid starting multiple Kafka consumers per pod.
CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:8002", "--capture-output", "--error-logfile", "-", "app:app"]
