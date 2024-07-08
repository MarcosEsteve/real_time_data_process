# producer/Dockerfile
<<<<<<< HEAD
<<<<<<< HEAD
FROM python:3.9.0
=======
FROM python:3.8-slim
>>>>>>> 9bc0750 (New structure, everything automatic using just 1 command)
=======
FROM python:3.9.0
>>>>>>> d9288ff (data processing added)

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "data_producer.py"]
