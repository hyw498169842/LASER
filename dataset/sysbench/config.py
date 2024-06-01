from itertools import combinations

DATABASE = "sbtest"

# Schema information
ALL_TABLES = [f"sbtest{i}" for i in range(1, 11)]
ALL_INDEXES = [f"{table}_pkey" for table in ALL_TABLES] + [f"k_{i}" for i in range(1, 11)]
N_TABLES = len(ALL_TABLES)
N_INDEXES = len(ALL_INDEXES)

# scan types
ALL_SCAN_TYPES = ["Seq Scan", "Index Scan", "Index Only Scan"]
N_SCAN_TYPES = len(ALL_SCAN_TYPES)

# operator types
ALL_OPERATORS = ["=", "<", ">", "<=", ">=", "<>"]
N_OPERATORS = len(ALL_OPERATORS)

# join conditions
ALL_INDEX_CONDS = list(combinations([f"sbtest{i}.id" for i in range(1, 11)], 2))
ALL_INDEX_CONDS = [set()] + [set(cond) for cond in ALL_INDEX_CONDS]
N_INDEX_CONDS = len(ALL_INDEX_CONDS)

# warm up table
WARM_UP_TABLE = "sbtest1"

# Input queries
QUERY_DIR = "dataset/sysbench/sysbench-queries"
N_QUERIES = 1000

# some override parameters
SNAPSHOT_INTERVAL = 5
MODEL_COPY_INTERVAL = 180
MODEL_TRAIN_INTERVAL = 60
TRAINING_DATA_WINDOW_SIZE = 256
TRAINING_DATA_EXPIRE_TIME = 1800
QUERY_REALLOCATION_INTERVAL = 180
QUERY_ARRIVE_RATE = 1
IDLE_INTERVAL = 0.01