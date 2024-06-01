import time

from DBworker import DBworker
from model import QueryModel
from trainer import Trainer
from query import Query
from allocator import *
from shared import *
from config import *

for nqueries in (1000, 100, 10, 1):
    for query_queue in QUERY_QUEUES:
        while len(query_queue) > 0:
            del query_queue[-1]
    
    # time plan generation
    t1 = time.time()
    queries = []
    for i in range(nqueries):
        with open(f"{QUERY_DIR}/{i + 1}.sql", "r") as f:
            queries.append(Query(i, f.read(), update_plan_on_init=True))
    t2 = time.time()
    print(f"Plan generation time: {t2 - t1:.4f}s, amortized: {(t2 - t1) / nqueries:.4f}s")

    # time model prediction and allocation
    master = DBworker(wid=0, cluster="master", host=MASTER_HOST, port=MASTER_PORT, use_ml_method=True)
    slave1 = DBworker(wid=1, cluster="slave1", host=SLAVE1_HOST, port=SLAVE1_PORT, use_ml_method=True)
    slave2 = DBworker(wid=2, cluster="slave2", host=SLAVE2_HOST, port=SLAVE2_PORT, use_ml_method=True)
    workers = [master, slave1, slave2]
    for i, worker in enumerate(workers):
        worker.register_global_variables(FINISHED[i], START_TIME, TABLE_IS_MODIFIED, \
                                         ACC_EXEC_TIME[i], ACC_FINISH_TIME[i], MAX_FINISH_TIME[i], \
                                         QUERY_QUEUES[i], QUERY_QUEUE_LOCKS[i], N_FINISHED_QUERIES[i], \
                                         RAW_TRAINING_DATA_QUEUE, FAIL_QUERY_QUEUE, LAST_EXECUTED_QUERY_IDS)
    query_model = QueryModel().to(DEVICE)
    trainer = Trainer(query_model)
    allocator = CacheBasedAllocator()
    allocator.update_models(trainer)
    t1 = time.time()
    allocator.allocate(queries)
    t2 = time.time()
    print(f"Prediction and allocation time: {t2 - t1:.4f}s, amortized: {(t2 - t1) / nqueries:.4f}s")

    # time query re-allocation
    cur_qids = get_all_queueing_query_ids()
    cur_queries = [queries[qid] for qid in cur_qids]
    t1 = time.time()
    allocator.re_allocate(cur_queries, [queries[qid] for qid in LAST_EXECUTED_QUERY_IDS])
    t2 = time.time()
    print(f"Re-allocation time: {t2 - t1:.4f}s, amortized: {(t2 - t1) / nqueries:.4f}s")

    # time query selection
    for idx in range(1, N_WORKERS):
        while len(QUERY_QUEUES[idx]) > 0:
            QUERY_QUEUES[0].append(QUERY_QUEUES[idx][-1])
            del QUERY_QUEUES[idx][-1]
    assert len(QUERY_QUEUES[0]) == nqueries
    for handler in master.handlers:
        handler.init()
        handler.start()
    t1 = time.time()
    master.select_queries()
    t2 = time.time()
    print(f"Selection time: {t2 - t1:.4f}s")

    for handler in master.handlers:
        handler.finished.set()
    print()