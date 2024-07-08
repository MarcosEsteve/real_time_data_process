# producer/Dockerfile
<<<<<<< HEAD
FROM python:3.9.0
=======
FROM python:3.8-slim
>>>>>>> 9bc0750 (New structure, everything automatic using just 1 command)

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "data_producer.py"]
