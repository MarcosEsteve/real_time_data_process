import streamlit as st
import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, StringIndexerModel
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import joblib
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RealTimeBusDelayPrediction") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.3.1.jar") \
    .getOrCreate()


# Function to load data from PostgreSQL
def load_data(num_records=100):
    jdbc_url = "jdbc:postgresql://postgres:5432/mydb"
    properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    df = spark.read.jdbc(url=jdbc_url, table="bus_traffic_processed", properties=properties)
    # Get only the last num_records, ordered by date
    df = df.orderBy(["recordedattime_year", "recordedattime_month", "recordedattime_day",
                     "recordedattime_hour", "recordedattime_minute", "recordedattime_second"],
                    ascending=[False, False, False, False, False, False]).limit(num_records)
    return df


# Define string columns to index, these columns will need to be converted to integer to fit the model, as they are strings
string_columns = ['publishedlinename', 'vehicleref', 'originname', 'destinationname', 'nextstoppointname', 'arrivalproximitytext']


# Function to make the last necessary changes in data
def process_data(df):
    # Apply StringIndexer to string columns and save the models
    for col in string_columns:
        indexer = StringIndexer(inputCol=col, outputCol=f"{col}_indexed", handleInvalid="keep")
        indexer_model = indexer.fit(df)
        df = indexer_model.transform(df)
        indexer_model.write().overwrite().save(f'./model/string_indexer_{col}')  # Save the model for future use
    # Drop columns with all null values, according to dropped columns from feature selection
    df = df.dropna(how='all', subset=df.columns)
    return df


# Function to train model
def train_model(df):
    df = df.drop(*string_columns)
    print(df.columns)
    df = df.toPandas()
    X = df.drop(columns=['delay'])  # Exclude original string columns
    y = df['delay']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=16)
    model = RandomForestRegressor(n_estimators=100, random_state=16)
    model.fit(X_train, y_train)
    joblib.dump(model, './model/bus_delay_model.pkl')
    return model


# Function to load trained model
def load_model():
    return joblib.load('./model/bus_delay_model.pkl')


# Function to apply saved StringIndexer models during prediction
def process_new_data(df):
    # Load and apply saved StringIndexer models
    for col in string_columns:
        indexer_model = StringIndexerModel.load(f'./model/string_indexer_{col}')
        df = indexer_model.transform(df)

    # Drop columns with all null values, according to dropped columns from feature selection
    df = df.dropna(how='all', subset=df.columns)
    return df


# Function to predict delay
def predict_delay(model, data):
    # data must be a Pandas dataframe
    predictions = model.predict(data)
    return predictions


# Function to check if there is enough data to train the model
def wait_data(threshold=100):
    warning_placeholder = st.empty()
    while True:
        warning_placeholder.empty()
        df = load_data()
        if df.count() >= threshold:
            return df
        else:
            warning_placeholder.warning(f"Gathering data... Currently, there are {df.count()} records."
                                        f" Waiting until there are at least {threshold} records.")
            time.sleep(5)


# Function to display predictions in a table format
def display_predictions(data):
    # Convert Predicted Delay from seconds to minutes, rounded down to the nearest whole number
    data['Predicted_Delay'] = (data['Predicted_Delay'] // 60).astype(int)
    # Create a dictionary to map the current column names to custom names to display
    column_names = {
        'vehicleref': 'Vehicle Reference',
        'nextstoppointname': 'Next Stop Point',
        'Predicted_Delay': 'Predicted Delay (minutes)'
    }
    # Rename the columns in the DataFrame using the mapping
    data_renamed = data[['vehicleref', 'nextstoppointname', 'Predicted_Delay']].rename(
        columns=column_names)
    # Display the renamed DataFrame in a table
    st.table(data_renamed)


# Streamlit UI
st.title("Real-Time Bus Delay Prediction")

# Ensure there is sufficient data to train the model
bus_df = wait_data(20)
bus_df = process_data(bus_df)

# Train model if not already trained
if not os.path.exists('./model/bus_delay_model.pkl'):
    with st.spinner('Training model...'):
        bus_delay_model = train_model(bus_df)
        st.success('Model trained successfully!')
else:
    bus_delay_model = load_model()
    st.success('Model loaded successfully!')

# Get last records and predict
st.header("Latest Bus Predictions")
latest_data = bus_df.toPandas().tail(5)
latest_data['Predicted_Delay'] = predict_delay(bus_delay_model, latest_data.drop(columns=['delay'] + string_columns))

# Display initial predictions in a table
display_predictions(latest_data)


def predict_real_time_data():
    new_data = load_data(num_records=5)  # Load the latest records from the database
    new_data = process_new_data(new_data)  # Process them
    new_data = new_data.toPandas().drop(columns=['delay'])  # Drop delay and convert to Pandas for predicting
    new_data['Predicted_Delay'] = predict_delay(bus_delay_model, new_data.drop(columns=string_columns))  # Predict dropping string columns
    display_predictions(new_data)  # Display the new predictions in the same table format


if st.button("Predict for new data"):
    predict_real_time_data()

