# 🚀 General Description of the Project

This project is developed using microservices with Docker, orchestrated via a `docker-compose.yml` file. Three base images are used: `wurstmeister/Kafka`, `wurstmeister/Zookeeper`, and `bitnami/Spark`. Four additional custom images are created for the producer, consumer, PostgreSQL database, and the machine learning application (ml_app).

Execution is initiated through the `docker-compose up` command. The producer reads data from a CSV file and sends each row as a message to Kafka. Kafka, managed by Zookeeper, forwards these messages to the consumer, which processes each message (one row at a time) using PySpark functions by transforming the data, handling missing values, and selecting relevant features. The processed data is then stored in a PostgreSQL database.

The machine learning application waits for a sufficient amount of data, loads it from the database, processes it further by converting string variables to integers, and either trains a Random Forest model or loads a pre-trained model. The app predicts bus arrival times at the next stop based on real-time data regarding bus locations and shows these predictions in a web interface available in `localhost:8501` using Streamlit.

## 🏗️ Proposed Architecture for the Real-Time Data Processing System

- **Data Source**: A CSV file with data on bus traffic, containing over 6 million entries and 17 columns, which are read by a Python script in near real-time.
- **Data Ingestion**: A microservice that, using a Kafka producer, handles the transmission of records, managed by Zookeeper, ensuring a reliable, consistent, and efficient data flow.
- **Data Processing**: A microservice that uses Apache Spark to consume the data from Kafka and applies the necessary transformations to make the data usable in the frontend application.
- **Data Storage**: The data processed by Spark is stored in a microservice with PostgreSQL so that it can be accessed by other microservices.
- **Delivery to ML App**: The frontend application microservice consists of a Python script responsible for training an ML model on the transformed data and generating a prediction shown in a web page using the Streamlit library.
- **Reliability, Scalability, and Maintainability**: Data integrity and fault tolerance, scalability through load distribution and parallel processing with Kafka and Spark, and maintainability through containerization with Docker as IaC and Git.
- **Data Security, Governance, and Protection**: Use of SSL/TLS for communication between microservices; security and governance with PostgreSQL through roles and permissions.

## 🌟 Potential Applications

The backend of this project opens up numerous exciting possibilities! Here are a few ideas to explore:

- 🚍 **Real-Time Public Transportation**: Predict bus arrival times and improve passenger information systems.
- 📦 **Logistics and Delivery Services**: Optimize routes and delivery times based on real-time traffic data.
- 📊 **Data Analytics Dashboards**: Create visualizations and analytics tools for transportation data insights.
- 🏙️ **Smart City Solutions**: Integrate with city infrastructure for improved urban mobility and planning.
- 🛠️ **Predictive Maintenance for Fleet Management**: Analyze vehicle data to predict maintenance needs and reduce downtime.

## 🚀 How to Run the Project

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
