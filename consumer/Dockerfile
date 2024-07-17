# consumer/Dockerfile
FROM bitnami/spark:latest

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "data_consumer.py"]
