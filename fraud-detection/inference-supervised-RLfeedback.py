from bytewax.dataflow import Dataflow
from bytewax.inputs import KafkaInputConfig
from bytewax.outputs import KafkaOutputConfig
import joblib
import numpy as np
from stable_baselines3 import DQN

def load_models(supervised_model_path, rl_model_path):
    supervised_model = joblib.load(supervised_model_path)
    rl_model = DQN.load(rl_model_path)
    return supervised_model, rl_model

def preprocess_transaction(transaction):
    # Preprocess transaction for model input
    return np.array([...])

def make_supervised_decision(supervised_model, features):
    # Make prediction with supervised model
    return supervised_model.predict(features)

def collect_feedback(transaction, decision):
    # Collect feedback for RL model
    feedback = {...}
    # Store feedback in S3
    # s3_client.put_object(Bucket='your-bucket', Key='feedback.json', Body=json.dumps(feedback))
    return feedback

def build_dataflow(supervised_model_path, rl_model_path):
    flow = Dataflow()
    
    # Load models
    supervised_model, rl_model = load_models(supervised_model_path, rl_model_path)
    
    # Kafka input
    flow.input("input", KafkaInputConfig("your-kafka-broker", "your-topic"))
    
    # Preprocess and make decisions
    flow.map(preprocess_transaction)
    flow.map(lambda features: make_supervised_decision(supervised_model, features))
    
    # Collect feedback
    flow.map(collect_feedback)
    
    # Kafka output
    flow.output("output", KafkaOutputConfig("your-kafka-broker", "your-output-topic"))
    
    return flow

# Run the dataflow
flow = build_dataflow("supervised_model.pkl", "rl_model.pkl")