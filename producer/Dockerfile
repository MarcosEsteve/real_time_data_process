# producer/Dockerfile
FROM python:3.9.0

WORKDIR /producer

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "data_producer.py"]
