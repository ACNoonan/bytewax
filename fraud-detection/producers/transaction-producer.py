from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.connectors.kafka import KafkaSink
from bytewax.connectors.stdio import StdOutSink
import json
import random
import uuid
from datetime import datetime
import time

# Run command (From Documents): 
#  python -m bytewax.run projects.bytewax.fraud-detection.producers.transaction-producer:flow

def generate_transaction():
    """Generate a transaction with the specified schema."""
    transaction_id = str(uuid.uuid4())
    tx_datetime = datetime.utcnow().isoformat() + "Z"
    customer_id = f"CUST-{random.randint(1000, 9999)}"
    terminal_id = f"TERM-{random.randint(100, 999)}"
    tx_amount = round(random.uniform(10.0, 5000.0), 2)
    tx_fraud = random.choice([0, 1])  # 0 for legitimate, 1 for fraudulent

    transaction = {
        "TRANSACTION_ID": transaction_id,
        "TX_DATETIME": tx_datetime,
        "CUSTOMER_ID": customer_id,
        "TERMINAL_ID": terminal_id,
        "TX_AMOUNT": tx_amount,
        "TX_FRAUD": tx_fraud
    }
    
    return transaction

class TransactionPartition(StatelessSourcePartition):
    """Generate transactions."""
    
    def next_batch(self):
        """Generate the next batch of transactions."""
        transaction = generate_transaction()
        time.sleep(random.uniform(0.1, 0.5))  # Random delay between transactions
        return [transaction]

class TransactionInput(DynamicSource):
    """Dynamic source for transaction generation."""
    
    def build(self, step_id, worker_index, worker_count):
        """Build the transaction source partition."""
        return TransactionPartition()

def serialize_transaction(transaction):
    """Serialize the transaction to JSON format."""
    return json.dumps(transaction).encode('utf-8')

def build_dataflow():
    flow = Dataflow("transactions")
    
    # Input: Generate transactions
    flow.input("input", flow, TransactionInput())
    
    # Output: Send to Kafka
    flow.output(
        "output", 
        KafkaSink(
            brokers=["localhost:9092"],
            topic="transactions",
            key_serializer=lambda x: None,  # No key serialization
            value_serializer=serialize_transaction
        )
    )
    
    return flow

# Run the dataflow
flow = build_dataflow()