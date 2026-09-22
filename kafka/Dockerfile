FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir kafka-python==2.0.2

COPY kafka/mock_producer.py .

CMD ["python", "mock_producer.py"]
