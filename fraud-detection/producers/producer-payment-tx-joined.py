from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition 
from bytewax.connectors.kafka import KafkaSink
import json
import random
import uuid
from datetime import datetime, timedelta
import time

# Run command (From Documents): 
#  python -m bytewax.run projects.bytewax.fraud-detection.producers.producer-payment-tx-joined:flow

def generate_random_credit_card_transaction():
    """Generate a random credit card transaction with detailed information."""
    transaction_id = str(uuid.uuid4())
    card_number = f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
    card_brand = random.choice(["VISA", "MASTERCARD", "AMEX", "DISCOVER"])
    card_holder = f"Cardholder {random.randint(1, 1000)}"
    amount = round(random.uniform(10.0, 5000.0), 2)
    transaction_time = datetime.utcnow()
    processing_timestamp = transaction_time.isoformat() + "Z"

    # Location details
    locations = [
        {"city": "New York", "state": "NY", "zip": "10001", "country": "US"},
        {"city": "Los Angeles", "state": "CA", "zip": "90001", "country": "US"},
        {"city": "Miami", "state": "FL", "zip": "33101", "country": "US"},
        {"city": "Chicago", "state": "IL", "zip": "60601", "country": "US"}
    ]
    location = random.choice(locations)

    transaction = {
        "key": {
            "transactionId": transaction_id
        },
        "value": {
            "transactionId": transaction_id,
            "timestamp": processing_timestamp,
            "cardInfo": {
                "cardNumber": card_number,
                "brand": card_brand,
                "cardHolder": card_holder,
                "expiryMonth": f"{random.randint(1, 12):02d}",
                "expiryYear": str(random.randint(24, 28)),
                "issuerCountry": "US"
            },
            "transactionInfo": {
                "amount": amount,
                "currency": "USD",
                "type": "PURCHASE",
                "status": "PENDING_AUTHORIZATION"
            },
            "locationInfo": {
                "address": f"{random.randint(100, 999)} Main St",
                "city": location["city"],
                "state": location["state"],
                "postalCode": location["zip"],
                "country": location["country"]
            },
            "riskIndicators": {
                "locationRisk": {
                    "isHighRiskLocation": random.random() < 0.1
                },
                "cardRisk": {
                    "isFirstChipFailure": random.random() < 0.05,
                    "isCardExpiringNext30Days": random.random() < 0.1
                }
            }
        },
        "metadata": {
            "source": "CREDIT_CARD",
            "version": "1.0",
            "processingTimestamp": processing_timestamp
        }
    }
    
    return transaction

class TransactionPartition(StatelessSourcePartition):
    """Generate credit card transactions."""
    
    def next_batch(self):
        """Generate the next batch of transactions."""
        transaction = generate_random_credit_card_transaction()
        time.sleep(random.uniform(0.1, 0.5))  # Random delay between transactions
        return [transaction]

class TransactionInput(DynamicSource):
    """Dynamic source for transaction generation."""
    
    def build(self, step_id, worker_index, worker_count):
        """Build the transaction source partition."""
        return TransactionPartition()

def serialize_transaction(transaction):
    """Serialize the transaction to JSON format"""
    return json.dumps(transaction).encode('utf-8')



class PaymentTransactionData(StatelessSourcePartition):
    """Generate random payment transactions with fraud logic."""

    def next_batch(self):
        """Generate the next batch of transactions."""
        transaction = generate_random_credit_card_transaction()
        
        # Logic to determine if the transaction is fraudulent
        is_fraudulent = random.random() < 0.05  # 5% chance of being fraudulent
        transaction['value']['fraudPrediction'] = {
            'isFraudulent': is_fraudulent,
            'fraudProbability': random.uniform(0.5, 1.0) if is_fraudulent else random.uniform(0.0, 0.5),
            'modelVersion': '1.0'
        }
        
        return [transaction]



def build_dataflow():
    flow = Dataflow("payment_transaction_flow")
    
    # Input: Generate random credit card transactions
    flow.input("input", TransactionInput())
    
    # Output: Send to Kafka
    flow.output(
        "output", 
        KafkaSink(
            brokers=["localhost:9092"],
            topic="payment-tx-dev",
            key_serializer=lambda x: None,  # No key serialization
            value_serializer=serialize_transaction
        )
    )
    
    return flow

# Run the dataflow
flow = build_dataflow()
