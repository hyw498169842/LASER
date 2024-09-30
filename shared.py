from collections import deque
from multiprocessing.managers import SyncManager

from config import N_WORKERS, N_TABLES

SyncManager.register('Deque', deque, exposed=["__len__", "__iter__", "__getitem__", "__delitem__", \
                                              "append", "pop", "popleft", "clear"])

# use SyncManager to share data between processes
MANAGER = SyncManager()
MANAGER.start()

# used to terminate the workers
FINISHED = [MANAGER.Event() for _ in range(N_WORKERS)]

# one query queue for each worker
QUERY_QUEUES = [MANAGER.Deque() for _ in range(N_WORKERS)]
QUERY_QUEUE_LOCKS = [MANAGER.Lock() for _ in range(N_WORKERS)]

# global training data queue
RAW_TRAINING_DATA_QUEUE = MANAGER.Queue()

# collect queries that fail to execute
FAIL_QUERY_QUEUE = MANAGER.Queue()

# global start time, later set in main.py
START_TIME = MANAGER.Value('d', 0)

# query execution statistics
ACC_EXEC_TIME = [MANAGER.Value('d', 0) for _ in range(N_WORKERS)]
ACC_FINISH_TIME = [MANAGER.Value('d', 0) for _ in range(N_WORKERS)]
MAX_FINISH_TIME = [MANAGER.Value('d', 0) for _ in range(N_WORKERS)]
N_FINISHED_QUERIES = [MANAGER.Value('i', 0) for _ in range(N_WORKERS)]

# last executed query of each worker
LAST_EXECUTED_QUERY_IDS = MANAGER.list([0 for _ in range(N_WORKERS)])

# whether the database is modified
TABLE_IS_MODIFIED = MANAGER.list([0 for _ in range(N_TABLES)])


def get_all_queueing_query_ids():
    all_queueing_query_ids = []
    for i, dq in enumerate(QUERY_QUEUES):
        with QUERY_QUEUE_LOCKS[i]:
            all_queueing_query_ids += [qid for qid, _, _, _, _, _, _ in dq]
            dq.clear()
    return all_queueing_query_ids

def get_cur_queue_length():
    cur_queue_length = []
    for i, dq in enumerate(QUERY_QUEUES):
        with QUERY_QUEUE_LOCKS[i]:
            cur_queue_length.append(len(dq))
    return cur_queue_length
