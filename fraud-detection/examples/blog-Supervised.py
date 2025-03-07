#!/bin/python3

from kafka import KafkaConsumer
import json
import joblib
import numpy as np
import xgboost as xgb
import pandas as pd

# Load the trained XGBoost model
model = joblib.load('xgboost_model.pkl')

# Kafka Consumer Configuration
topic_name = 'ccfd_11'
bootstrap_servers = ['192.168.1.130:9092']  # Replace with your Kafka broker addresses
group_id = 'ccfd-consumer-group'  # Consumer group ID

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='latest',  # Start from the latest message when joining the group
    enable_auto_commit=True,
    group_id=group_id,  # Set the consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Kafka Consumer started. Waiting for messages...")

# Define the column names (the same as the ones used in your model training)
column_names = ['time'] + [f'pca_{i}' for i in range(1, 29)] + ['amount']

# Consume messages indefinitely
try:
    for message in consumer:
        print(f"Received message from offset {message.offset} : {message.value}")
        data = message.value

        try:
            # Extract features from the message (ensure keys match your schema)
            features = [
                data.get('time', 0.0),  # Include 'time' if it's used in training
            ]
            # Add pca features 1-28 (default to 0.0 if missing)
            features += [data.get(f'pca_{i}', 0.0) for i in range(1, 29)]
            # Add 'amount' field (default to 0.0 if missing)
            features.append(data.get('amount', 0.0))
            
            print(f"Extracted Features: {features}")
            
            # Ensure features are in the correct format for the model
            features_df = pd.DataFrame([features], columns=column_names)
            print(features_df)
            
            # Convert to DMatrix with column names
            dmatrix = xgb.DMatrix(features_df)
            print(dmatrix)
            
            # Make a prediction
            prediction = model.predict(dmatrix)
            print(f"Prediction: {prediction[0]}")

        except Exception as e:
            print(f"Error processing message: {e}")

except KeyboardInterrupt:
    print("Consumer stopped manually.")
finally:
    consumer.close()
    print("Kafka Consumer closed.")