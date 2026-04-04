FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8002
CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:8002", "app:app"]
