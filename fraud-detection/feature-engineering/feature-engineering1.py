from bytewax.dataflow import Dataflow
from bytewax.inputs import KafkaInputConfig
from bytewax.outputs import KafkaOutputConfig
import joblib
import numpy as np
import json

# Load the model at startup
model = joblib.load('../fraud_detection_model.pkl')

def deserialize_transaction(kafka_msg):
    """Deserialize Kafka message to transaction object"""
    return json.loads(kafka_msg.decode('utf-8'))

def serialize_transaction(transaction):
    """Serialize transaction object to Kafka message"""
    return json.dumps(transaction).encode('utf-8')

def extract_features(transaction):
    """Extract and transform features from transaction data to match model input"""
    # Extract relevant fields from the transaction
    features = {
        'amount': transaction['value']['transactionInfo']['amount'],
        'entry_mode': transaction['value']['transactionInfo']['entryMode'],
        'is_high_risk_time': transaction['value']['transactionInfo']['isHighRiskTime'],
        'merchant_risk_level': transaction['value']['merchantInfo']['riskLevel'],
        'velocity_1h': transaction['value']['transactionInfo']['velocity']['txn_count_1h'],
        'velocity_24h': transaction['value']['transactionInfo']['velocity']['txn_count_24h'],
        'amount_24h': transaction['value']['transactionInfo']['velocity']['amount_24h'],
        'different_merchants_24h': transaction['value']['transactionInfo']['velocity']['different_merchants_24h'],
        'is_high_velocity': transaction['value']['riskIndicators']['velocityCheck']['isHighVelocity'],
        'is_high_daily_amount': transaction['value']['riskIndicators']['velocityCheck']['isHighDailyAmount'],
        'unusual_merchant_pattern': transaction['value']['riskIndicators']['velocityCheck']['unusualMerchantPattern'],
        'is_high_risk_location': transaction['value']['riskIndicators']['locationRisk']['isHighRiskLocation'],
        'distance_from_last_tx': transaction['value']['riskIndicators']['locationRisk']['distanceFromLastTx'],
        'is_first_chip_failure': transaction['value']['riskIndicators']['cardRisk']['isFirstChipFailure'],
        'is_card_expiring': transaction['value']['riskIndicators']['cardRisk']['isCardExpiringNext30Days']
    }
    
    # Convert categorical variables to numeric
    features['entry_mode_encoded'] = {
        'CHIP_READ': 0,
        'MAG_SWIPE': 1,
        'CONTACTLESS': 2,
        'MANUAL_ENTRY': 3
    }.get(features['entry_mode'], 0)
    
    features['risk_level_encoded'] = {
        'LOW': 0,
        'MEDIUM': 1,
        'HIGH': 2
    }.get(features['merchant_risk_level'], 0)
    
    # Create feature array in the same order as training data
    feature_array = [
        features['amount'],
        features['entry_mode_encoded'],
        features['is_high_risk_time'],
        features['risk_level_encoded'],
        features['velocity_1h'],
        features['velocity_24h'],
        features['amount_24h'],
        features['different_merchants_24h'],
        features['is_high_velocity'],
        features['is_high_daily_amount'],
        features['unusual_merchant_pattern'],
        features['is_high_risk_location'],
        features['distance_from_last_tx'],
        features['is_first_chip_failure'],
        features['is_card_expiring']
    ]
    
    return np.array(feature_array).reshape(1, -1)

def process_transaction(transaction):
    """Process transaction through feature extraction and model inference"""
    # Extract features
    features = extract_features(transaction)
    
    # Make prediction
    prediction = model.predict(features)[0]
    probability = model.predict_proba(features)[0][1]  # Probability of fraud
    
    # Add prediction to transaction
    transaction['value']['fraudPrediction'] = {
        'isFraudulent': bool(prediction),
        'fraudProbability': float(probability),
        'modelVersion': '1.0'
    }
    
    return transaction

def build_dataflow():
    flow = Dataflow()
    
    # Input: Read from Kafka topic with enriched transactions
    flow.input("input", KafkaInputConfig(
        brokers="localhost:9092",
        topic="enriched-transactions",
        deserializer=deserialize_transaction
    ))
    
    # Add processing step
    flow.map(process_transaction)
    
    # Output: Send to Kafka topic with fraud predictions
    flow.output("output", KafkaOutputConfig(
        brokers="localhost:9092",
        topic="fraud-predictions",
        serializer=serialize_transaction
    ))
    
    return flow

if __name__ == "__main__":
    flow = build_dataflow()
    # Run the dataflow
    flow.run()