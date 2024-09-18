# consumer/Dockerfile
FROM python:3.9.0

WORKDIR /consumer

COPY requirements.txt .
RUN pip install -r requirements.txt

# Install Java
RUN apt-get update && apt-get install -y --fix-missing openjdk-11-jdk

# Get the JAR controler for PostgreSQL
RUN wget https://jdbc.postgresql.org/download/postgresql-42.3.1.jar -P /opt/spark/jars/

# Set JAVA_HOME environment variable for pyspark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

COPY . .

CMD ["python", "data_consumer.py"]
