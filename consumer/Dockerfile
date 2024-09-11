# consumer/Dockerfile
FROM bitnami/spark:latest

WORKDIR /consumer

COPY requirements.txt .
RUN pip install -r requirements.txt

# Get the JAR controler for PostgreSQL
RUN wget https://jdbc.postgresql.org/download/postgresql-42.3.1.jar -P /opt/spark/jars/

COPY . .

CMD ["python", "data_consumer.py"]
