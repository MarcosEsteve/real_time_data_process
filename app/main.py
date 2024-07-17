import streamlit as st
import pandas as pd
import psycopg2
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression


def load_data():
    conn = psycopg2.connect(
        dbname="mydb",
        user="user",
        password="password",
        host="postgres",
        port="5432"
    )
    query = "SELECT * FROM bus_traffic"
    bus_data = pd.read_sql(query, conn)
    conn.close()
    return bus_data


def train_model(df):
    X = df[['feature1', 'feature2']]  # Sustituye con las características adecuadas
    y = df['target']  # Sustituye con el objetivo adecuado
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = LinearRegression() # Cambiar y meter un random forest o gradient boosting
    model.fit(X_train, y_train)
    return model, X_test, y_test


if __name__ == "__main__":
    st.title('Real-Time Data Processing and ML Prediction')
    bus_df = load_data()
    if not bus_df.empty:
        model, X_test, y_test = train_model(bus_df)
        predictions = model.predict(X_test)
        st.write("Predictions:")
        st.write(predictions)
