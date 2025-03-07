import numpy as np
import pandas as pd
import gym
from gym import spaces
from stable_baselines3 import DQN  # We will use DQN as an example
from stable_baselines3.common.evaluation import evaluate_policy

class FraudEnv(gym.Env):
    """A Simple Fraud Detection Environment"""
    
    def __init__(self, n_transactions=1000):
        super(FraudEnv, self).__init__()
        
        self.n_transactions = n_transactions
        
        # Action space: 2 actions -> 0: Approve, 1: Deny
        self.action_space = spaces.Discrete(2)
        
        # Observation space: [transaction_amount, transaction_risk, ...]
        # We'll keep it simple with 2 features in [0,1]
        self.observation_space = spaces.Box(low=0, high=1, shape=(2,), dtype=np.float32)
        
        self.transactions = None
        self.current_idx = 0
        self.reset()

    def reset(self):
        # Create random data for fraud
        # features = [amount, risk], labels = [fraud: 1/0]
        amounts = np.random.rand(self.n_transactions)  # 0-1 range for amounts
        risks = np.random.rand(self.n_transactions)    # 0-1 range for risk
        
        # Simplified assumption: if risk > 0.8, label it as fraud
        labels = (risks > 0.8).astype(int)
        
        self.transactions = np.column_stack((amounts, risks, labels))
        self.current_idx = 0
        
        return self._get_observation()

    def _get_observation(self):
        return self.transactions[self.current_idx][:2]  # 2 features: amount, risk

    def step(self, action):
        obs = self._get_observation()
        amount, risk, label = self.transactions[self.current_idx]
        
        # Calculate the reward
        reward = 0
        
        if label == 1:  # Fraud
            if action == 1:  # Deny
                reward = +2  # Correctly caught fraud
            else:
                reward = -2  # Missed a fraudulent transaction
        else:  # Legitimate transaction
            if action == 1:  # Deny
                reward = -1  # Customer dissatisfaction
            else:
                reward = +1  # Correct approval
        
        self.current_idx += 1
        
        done = (self.current_idx >= self.n_transactions)
        
        # Next observation
        if not done:
            next_obs = self._get_observation()
        else:
            next_obs = np.zeros((2,), dtype=np.float32)
        
        return next_obs, reward, done, {}
    
    def render(self, mode='human'):
        pass


# Create the environment
env = FraudEnv(n_transactions=1000)

# Create the agent
model = DQN(
    "MlpPolicy", 
    env, 
    verbose=1,
    learning_rate=1e-3,
    batch_size=32,
    buffer_size=10000,
    exploration_fraction=0.1,
    exploration_final_eps=0.02,
    target_update_interval=500
)

# Start training
model.learn(total_timesteps=20000)


mean_reward, std_reward = evaluate_policy(model, env, n_eval_episodes=10)
print(f"Average Reward: {mean_reward} +/- {std_reward}")