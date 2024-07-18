import numpy as np
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, mean, lit, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import json


def consume_data(topic, bootstrap_servers):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    spark = SparkSession.builder.appName("DataConsumer").getOrCreate()

    # Define schema for DataFrame
    schema = StructType([
        StructField("RecordedAtTime", StringType(), True),
        StructField("DirectionRef", IntegerType(), True),
        StructField("PublishedLineName", StringType(), True),
        StructField("OriginName", StringType(), True),
        StructField("OriginLat", DoubleType(), True),
        StructField("OriginLong", DoubleType(), True),
        StructField("DestinationName", StringType(), True),
        StructField("DestinationLat", DoubleType(), True),
        StructField("DestinationLong", DoubleType(), True),
        StructField("VehicleRef", StringType(), True),
        StructField("VehicleLocation_Latitude", DoubleType(), True),
        StructField("VehicleLocation_Longitude", DoubleType(), True),
        StructField("NextStopPointName", StringType(), True),
        StructField("ArrivalProximityText", StringType(), True),
        StructField("DistanceFromStop", IntegerType(), True),
        StructField("ExpectedArrivalTime", StringType(), True),
        StructField("ScheduledArrivalTime", StringType(), True)
    ])

    # Process each message (each row) independently
    for message in consumer:
        # Get the data from the message
        record = message.value
        # Create a dataframe with that data
        df = spark.createDataFrame([record], schema=schema)
        # Process the data
        df_processed = preprocess_data(df)
        # Check if df_processed is None to skip the save
        if df_processed is not None:
            save_to_postgres(df_processed)


def preprocess_data(df):
    # Drop the row if either ExpectedArrivalTime or ScheduledArrivalTime is null
    df = df.dropna(subset=["ExpectedArrivalTime", "ScheduledArrivalTime"], how="any")
    # Check if resulting DataFrame is empty
    if df.count() == 0:
        return None  # Return None if DataFrame is empty after dropping rows

    # Convert RecordedAtTime, ExpectedArrivalTime and ScheduledArrivalTime to timestamp
    df = df.withColumn("RecordedAtTime", to_timestamp("RecordedAtTime", "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("ExpectedArrivalTime", to_timestamp("ExpectedArrivalTime", "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("ScheduledArrivalTime", to_timestamp("ScheduledArrivalTime", "HH:mm:ss"))

    # Handle missing values in numeric columns
    numeric_columns = [col_name for col_name, dtype in df.dtypes if dtype in ['integer', 'double']]
    for col_name in numeric_columns:
        mean_value = df.select(mean(col_name)).collect()[0][0]
        # Convert mean_value to integer if original column type is integer
        if df.schema[col_name].dataType.typeName() == "integer":
            mean_value = int(mean_value)
        df = df.withColumn(col_name, when(df[col_name].isNull(), mean_value).otherwise(df[col_name]))

    # Handle missing values in string columns
    string_columns = [col_name for col_name, dtype in df.dtypes if
                      dtype == 'string' and col_name not in ['ExpectedArrivalTime', 'ScheduledArrivalTime']]
    for col_name in string_columns:
        df = df.withColumn(col_name, when(df[col_name].isNull(), '').otherwise(df[col_name]))

    # Convert timestamps to numeric features (e.g., year, month, day, hour, minute, second)
    timestamp_columns = ['RecordedAtTime', 'ExpectedArrivalTime']
    for col_name in timestamp_columns:
        df = df.withColumn(col_name + "_year", col(col_name).cast("string").substr(1, 4).cast(DoubleType()))
        df = df.withColumn(col_name + "_month", col(col_name).cast("string").substr(6, 2).cast(DoubleType()))
        df = df.withColumn(col_name + "_day", col(col_name).cast("string").substr(9, 2).cast(DoubleType()))
        df = df.withColumn(col_name + "_hour", col(col_name).cast("string").substr(12, 2).cast(DoubleType()))
        df = df.withColumn(col_name + "_minute", col(col_name).cast("string").substr(15, 2).cast(DoubleType()))
        df = df.withColumn(col_name + "_second", col(col_name).cast("string").substr(18, 2).cast(DoubleType()))
        df = df.drop(col_name)  # drop original timestamp column once all columns with features are created
    # ScheduledArrivalTime does not contain year-month-day
    half_timestamp_columns = ['ScheduledArrivalTime']
    for col_name in half_timestamp_columns:
        df = df.withColumn(col_name + "_hour", col(col_name).substr(1, 2).cast(DoubleType()))
        df = df.withColumn(col_name + "_minute", col(col_name).substr(4, 2).cast(DoubleType()))
        df = df.withColumn(col_name + "_second", col(col_name).substr(7, 2).cast(DoubleType()))
        df = df.drop(col_name)

    # Calculate Delay, our feature to predict, in minutes
    df = df.withColumn("Delay",
                       ((col("ExpectedArrivalTime_hour") * 60 + col("ExpectedArrivalTime_minute") + col("ExpectedArrivalTime_second") / 60)
                        - (col("ScheduledArrivalTime_hour") * 60 + col("ScheduledArrivalTime_minute") + col("ScheduledArrivalTime_second") / 60)))

    # Transform non-numeric columns to numeric
    non_numeric_columns = ['PublishedLineName', 'OriginName', 'DestinationName', 'VehicleRef',
                           'NextStopPointName', 'ArrivalProximityText']
    for col_name in non_numeric_columns:
        df = df.withColumn(col_name, df[col_name].cast(DoubleType()))

    # Feature engineering: Combine latitude and longitude
    df = df.withColumn("OriginCoordinates", col("OriginLat") + col("OriginLong"))
    df = df.withColumn("DestinationCoordinates", col("DestinationLat") + col("DestinationLong"))
    df = df.withColumn("VehicleCoordinates", col("VehicleLocation.Latitude") + col("VehicleLocation.Longitude"))
    # Drop columns used in combination
    df = df.drop("OriginLat", "OriginLong", "DestinationLat", "DestinationLong", "VehicleLocation_Latitude",
                 "VehicleLocation_Longitude")

    # Feature selection
    # Compute correlation matrix
    correlation_matrix = df.select([col(c).cast('double') for c in df.columns]).toPandas().corr().abs()

    # Select upper triangle of correlation matrix
    upper = correlation_matrix.where(np.triu(np.ones(correlation_matrix.shape), k=1).astype(bool))

    # Find features with correlation greater than 0.9 (exclude Delay because it is our feature to predict)
    to_drop = [column for column in upper.columns if any(upper[column] > 0.9) and column != 'Delay']

    # Instead of dropping features, to avoid problems with database, I set discarded columns to NULL
    # This approach, although is not very efficient, avoids changing the table in db dynamically based on
    # feature selection. I left that dropping for the application that will use the data
    df_selected = df  # Define df_selected in case any column in to_drop
    for column in to_drop:
        df_selected = df.withColumn(column, lit(None).cast(df.schema[column].dataType))

    return df_selected


def save_to_postgres(df):
    # Save DataFrame to PostgreSQL
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/mydb") \
        .option("dbtable", "bus_traffic_processed") \
        .option("user", "user") \
        .option("password", "password") \
        .save()


if __name__ == "__main__":
    consume_data('bus_traffic_data', ['kafka:9092'])
