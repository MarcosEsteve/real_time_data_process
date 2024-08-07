import streamlit as st
import os
from pyspark.sql import SparkSession
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import joblib
import time

# Initialize Spark session
spark = SparkSession.builder.appName("RealTimeBusDelayPrediction").getOrCreate()


# Function to load data from PostgreSQL
def load_data():
    jdbc_url = "jdbc:postgresql://postgres:5432/mydb"
    properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    df = spark.read.jdbc(url=jdbc_url, table="bus_traffic_processed", properties=properties)
    return df.toPandas()


# Function to make the last necessary changes in data
def process_data(df):
    # Drop columns with all null values, according to dropped columns from feature selection
    df = df.dropna(axis=1, how='all')
    return df


# Function to train model
def train_model(df):
    X = df.drop(columns=['Delay'])
    y = df['Delay']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=16)
    model = RandomForestRegressor(n_estimators=100, random_state=16)
    model.fit(X_train, y_train)
    joblib.dump(model, '../model/bus_delay_model.pkl')
    return model


# Function to load trained model
def load_model():
    return joblib.load('../model/bus_delay_model.pkl')


# Function to predict delay
def predict_delay(model, data):
    predictions = model.predict(data)
    return predictions


# Function to check if there is enough data to train the model
def wait_data(threshold=100):
    while True:
        df = load_data()
        if len(df) >= threshold:
            return df
        else:
            st.warning(f"Gathering data... Currently, there are {len(df)} records. Waiting until there are at least {threshold} records.")
            time.sleep(5)


# Streamlit UI
st.title("Real-Time Bus Delay Prediction")

# Ensure there is sufficient data to train the model
bus_df = wait_data()
bus_df = process_data(bus_df)

# Train model if not already trained
if 'bus_delay_model.pkl' not in os.listdir():
    with st.spinner('Training model...'):
        bus_delay_model = train_model(bus_df)
        st.success('Model trained successfully!')
else:
    bus_delay_model = load_model()

# Show latest data and predictions
st.header("Latest Bus Predictions")
latest_data = bus_df.tail(5)
latest_data['Predicted_Delay'] = predict_delay(bus_delay_model, latest_data.drop(columns=['Delay']))

for index, row in latest_data.iterrows():
    st.write(f"VehicleRef: {row['VehicleRef']}, NextStopPointName: {row['NextStopPointName']}, Predicted Delay: {row['Predicted_Delay']} minutes")

# Real-time data prediction
st.header("Real-Time Prediction")


def predict_real_time_data():
    new_data = load_data().tail(5).drop(columns=['Delay'])  # Load the latest record from the database
    predicted_delay = predict_delay(bus_delay_model, new_data)
    for i, new_row in new_data.iterrows():
        st.write(
            f"VehicleRef: {new_row['VehicleRef']}, NextStopPointName: {new_row['NextStopPointName']}, Predicted Delay: {predicted_delay[i]} minutes")


if st.button("Predict for new data"):
    predict_real_time_data()

