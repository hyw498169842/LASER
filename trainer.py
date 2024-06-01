import torch
import logging
import torch.nn.functional as F
from torch.optim import Adam
from copy import deepcopy
from threading import Lock

from config import LR, DEVICE

# Train the models in the background
class Trainer:
    def __init__(self, query_model, lr=LR):
        self.query_model = query_model
        self.optimizer = Adam(self.query_model.parameters(), lr=lr)
        self.checkpoint_lock = Lock()
        self.make_checkpoint()
    
    def update_query_model(self, queries, targets, device=DEVICE):
        # queries: list of Query objects    (batch_size)
        # targets: list of cache states     (batch_size, n_tables + n_indexes, n_buckets)
        predictions = self.query_model.encode_and_predict(queries, device=device)
        targets = torch.tensor(targets, dtype=torch.float32, device=device)
        loss = F.mse_loss(predictions, targets)
        self.optimizer.zero_grad()
        loss.backward()
        self.optimizer.step()
        # logging.info(f"Query model updated, loss: {loss}")
    
    def make_checkpoint(self):
        with self.checkpoint_lock:
            self.checkpoint = deepcopy(self.query_model)
    
    def get_checkpoint(self):
        with self.checkpoint_lock:
            return deepcopy(self.checkpoint)
