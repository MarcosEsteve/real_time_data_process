# General description of the project

This project is developed using microservices with Docker, orchestrated via a docker-compose.yml file.
Three base images are used: wurstmeister/Kafka, wurstmeister/Zookeeper, and bitnami/Spark. Four additional custom 
images are created for the producer, consumer, PostgreSQL database, and the machine learning application (ml_app).

Execution is initiated through the docker-compose up command. The producer reads data from a CSV file and sends each 
row as a message to Kafka. Kafka, managed by Zookeeper, forwards these messages to the consumer, which processes each 
message (one row at a time) using PySpark functions by transforming the data, handling missing values, and selecting 
relevant features. The processed data is then stored in a PostgreSQL database.

The machine learning application waits for a sufficient amount of data, loads it from the database, processes it 
further by converting string variables to integers, and either trains a Random Forest model or loads a pre-trained 
model. The app predicts bus arrival times at the next stop based on real-time data regarding bus locations and shows 
these predictions in a web interface available in localhost:8501 using Streamlit.

## How to run the project

1. Clone the repository:
   ```bash
   git clone https://github.com/MarcosEsteve/real_time_data_process
2. Install Docker Desktop
3. Download prebuilt images: 
   - wurstmeister/Kafka
   - wursmeister/Zookeeper
   - bitnami/Spark
4. Open a terminal in the cloned repository and run:
   ```bash
   docker-compose up
5. Once everything is running, go to localhost:8501 in your browser to see the application
