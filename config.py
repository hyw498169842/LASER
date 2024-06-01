import os

# Used for local deployment
MASTER_HOST = "localhost"
SLAVE1_HOST = "localhost"
SLAVE2_HOST = "localhost"
MASTER_PORT = 13000
SLAVE1_PORT = 13001
SLAVE2_PORT = 13002

# Used for remote deployment
# MASTER_HOST = # TODO
# SLAVE1_HOST = # TODO
# SLAVE2_HOST = # TODO
# MASTER_PORT = 13000
# SLAVE1_PORT = 13000
# SLAVE2_PORT = 13000

# Other connection parameters
USER = "user"
PASSWORD = "password"
NUM_CONNECTIONS = 8

# Training config
LR = 1e-4
DEVICE = "cpu"
N_WORKERS = 3
N_BUCKETS = 1024
FEATURE_DIM = 128
TRAIN_EPOCHS = 1
TRAIN_BATCH_SIZE = 16

# Trainer setting
N_SNAPSHOTS = 30
SNAPSHOT_INTERVAL = 10
MODEL_COPY_INTERVAL = 300
MODEL_TRAIN_INTERVAL = 180
TRAINING_DATA_WINDOW_SIZE = 256
TRAINING_DATA_EXPIRE_TIME = 3600

# Query allocation setting
COST_WEIGHT = 0.1                   # deprecated
PENALTY_WEIGHT = 0.01               # deprecated
W_A, W_A_STEP = 0, 0.01
W_S, W_S_STEP = 0, 0.05
QUERY_REALLOCATION_INTERVAL = 300
QUERY_QUEUE_MINIMUM_RESERVE = 1

# Streaming query setting
QUERY_ARRIVE_RATE = 0.5

# Avoid busy waiting
IDLE_INTERVAL = 1

# Whether to log errors (for output readability)
LOG_CONNECTION_ERROR = False
LOG_EXECUTION_ERROR = False

# dataset-specific config
if os.environ["DATASET"] == "tpch":
    from dataset.tpch.config import *
elif os.environ["DATASET"] == "tpcds":
    from dataset.tpcds.config import *
elif os.environ["DATASET"] == "job":
    from dataset.job.config import *
elif os.environ["DATASET"] == "sysbench":
    from dataset.sysbench.config import *
else:
    raise NotImplementedError("Dataset not supported")
