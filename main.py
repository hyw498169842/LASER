import time
import logging
import argparse
import coloredlogs
import progressbar
import numpy as np

from repeatingtimer import RepeatingTimer
from train_data import TrainData
from DBmonitor import DB_MONITOR
from DBworker import DBworker
from model import QueryModel
from trainer import Trainer
from query import Query
from allocator import *
from shared import *
from config import *


progressbar.streams.wrap_stdout()
progressbar.streams.wrap_stderr()
coloredlogs.install(level='INFO', fmt="[%(asctime)s][%(levelname)s] %(message)s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--allocator", type=str, default="RR", help="Allocator type")
    parser.add_argument("--no-steal", action="store_true", help="Disable query stealing")
    parser.add_argument("--stream", action="store_true", help="Use streaming query mode")
    args = parser.parse_args()

    allocator_dict = {
        "RR": RRAllocator, 
        "FIX": FIXAllocator, 
        "MIN": MINAllocator,
        "QCK": QCKAllocator,
        "ML": CacheBasedAllocator
    }
    AllocatorClass = allocator_dict[args.allocator]
    use_ml_method = (args.allocator == "ML")

    logging.info(f"Using allocator type: {args.allocator}")
    if args.stream:
        logging.info(f"Using streaming query mode with arrive rate {QUERY_ARRIVE_RATE}")

    # Initialize DB workers
    master = DBworker(wid=0, cluster="master", host=MASTER_HOST, port=MASTER_PORT, use_ml_method=use_ml_method)
    slave1 = DBworker(wid=1, cluster="slave1", host=SLAVE1_HOST, port=SLAVE1_PORT, use_ml_method=use_ml_method)
    slave2 = DBworker(wid=2, cluster="slave2", host=SLAVE2_HOST, port=SLAVE2_PORT, use_ml_method=use_ml_method)
    workers = [master, slave1, slave2]
    for i, worker in enumerate(workers):
        worker.register_global_variables(FINISHED[i], START_TIME, TABLE_IS_MODIFIED, \
                                         ACC_EXEC_TIME[i], ACC_FINISH_TIME[i], MAX_FINISH_TIME[i], \
                                         QUERY_QUEUES[i], QUERY_QUEUE_LOCKS[i], N_FINISHED_QUERIES[i], \
                                         RAW_TRAINING_DATA_QUEUE, FAIL_QUERY_QUEUE, LAST_EXECUTED_QUERY_IDS)

    # Initialize models and trainer
    if use_ml_method:
        query_model = QueryModel().to(DEVICE)
        trainer = Trainer(query_model)

    # Warm up the databases
    for worker in workers:
        worker.handlers[0].connect()
        worker.handlers[0].warm_up(log_output=False)
        worker.handlers[0].conn.close()

    # Start DB workers
    for worker in workers:
        worker.start()
    
    START_TIME.value = time.time()

    # Read queries
    # Note: explain command will (slightly) affect cache states
    queries, visible_idx = [], 0
    for i in range(N_QUERIES):
        with open(f"{QUERY_DIR}/{i + 1}.sql", "r") as f:
            queries.append(Query(i, f.read(), update_plan_on_init=use_ml_method))
    if args.stream:
        # Generate arrival time for each query
        np.random.seed(0)
        last_arrive_time = 0
        for query in queries:
            query.arrive_time = last_arrive_time
            last_arrive_time += np.random.exponential(1 / QUERY_ARRIVE_RATE)
        is_visible = lambda q: q.arrive_time <= time.time() - START_TIME.value

    # Query allocation
    allocator = AllocatorClass()
    if use_ml_method:
        allocator.update_models(trainer)
    if not args.stream:
        allocator.allocate(queries)

    # Start timers for model training, model copy, and query re-allocation
    last_imbalance_time = time.time()
    database_last_modified_time = 0
    train_data_window = deque(maxlen=TRAINING_DATA_WINDOW_SIZE)
    def train_model():
        # extract training data
        while not RAW_TRAINING_DATA_QUEUE.empty():
            qid, new_pages_dict, finish_time = RAW_TRAINING_DATA_QUEUE.get()
            train_data_window.append(TrainData(queries[qid], new_pages_dict, finish_time))
        # eliminate expired data
        current_time_stamp = time.time() - START_TIME.value
        is_expired = lambda data: current_time_stamp - data.finish_time > TRAINING_DATA_EXPIRE_TIME or \
                                  data.finish_time < database_last_modified_time
        while train_data_window and is_expired(train_data_window[0]):
            train_data_window.popleft()
        # train query model
        train_data_list = list(train_data_window)
        for _ in range(TRAIN_EPOCHS):
            for idx in range(0, len(train_data_list), TRAIN_BATCH_SIZE):
                batch = train_data_list[idx: idx + TRAIN_BATCH_SIZE]
                batch_queries = [data.query for data in batch]
                cache_states = [data.cache_states for data in batch]
                trainer.update_query_model(batch_queries, cache_states)
        trainer.make_checkpoint()
    
    def copy_model_from_trainer():
        allocator.update_models(trainer)
    
    def reallocate_queries():
        # no imbalance detected recently
        if time.time() - last_imbalance_time > QUERY_REALLOCATION_INTERVAL:
            allocator.update_alloc_weight(False)
        cur_qids = get_all_queueing_query_ids()
        cur_queries = [queries[qid] for qid in cur_qids]
        allocator.re_allocate(cur_queries, [queries[qid] for qid in LAST_EXECUTED_QUERY_IDS])
    
    model_training_timer = RepeatingTimer(MODEL_TRAIN_INTERVAL, train_model)
    model_copy_timer = RepeatingTimer(MODEL_COPY_INTERVAL, copy_model_from_trainer)
    reallocate_timer = RepeatingTimer(QUERY_REALLOCATION_INTERVAL, reallocate_queries)

    if use_ml_method:
        model_training_timer.start()
        model_copy_timer.start()
        reallocate_timer.start()
        
    # Wait until all queries are done
    widgets = ["[", progressbar.Timer(), "]", " Remaining in queue: ", progressbar.Counter()]
    bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength, redirect_stdout=True, widgets=widgets)
    while True:

        # Check for termination
        n_finished_queries = sum([x.value for x in N_FINISHED_QUERIES])
        if n_finished_queries == N_QUERIES:
            break

        # Check for modified tables
        if use_ml_method:
            for i, is_modified in enumerate(TABLE_IS_MODIFIED):
                if is_modified:
                    database_last_modified_time = time.time() - START_TIME.value
                    DB_MONITOR.mark_as_dirty(ALL_TABLES[i])
                    TABLE_IS_MODIFIED[i] = 0
        
        # Check for failed queries
        failed_queries = []
        while not FAIL_QUERY_QUEUE.empty():
            qid = FAIL_QUERY_QUEUE.get()
            failed_queries.append(queries[qid])
        allocator.allocate(failed_queries)
        
        # Check for incoming queries (in streaming query mode)
        if args.stream:
            new_queries = []
            while visible_idx < N_QUERIES and is_visible(queries[visible_idx]):
                new_queries.append(queries[visible_idx])
                visible_idx += 1
            allocator.allocate(new_queries)
        
        # Check for empty queues and trigger stealing
        cur_queue_length = get_cur_queue_length()
        bar.update(sum(cur_queue_length))
        # logging.info(f"{cur_queue_length}")
        if not args.no_steal:
            if use_ml_method and reallocate_timer.is_waiting:
                min_length = min(cur_queue_length)
                max_length = max(cur_queue_length)
                if min_length == 0 and max_length > 1:
                    # imbalance detected
                    last_imbalance_time = time.time()
                    allocator.update_alloc_weight(True)
                    # trigger an instant re-allocation
                    reallocate_timer.stop_waiting()
                    # wait until re-allocation is done
                    while reallocate_timer.interrupt.is_set():
                        time.sleep(IDLE_INTERVAL)
            # if not use_ml_method, or reallocation does not work,
            # simply steal queries from the longest queue
            if (not use_ml_method) or reallocate_timer.is_waiting:
                cur_queue_length = get_cur_queue_length()
                longest_queue_index = np.argmax(cur_queue_length)
                with QUERY_QUEUE_LOCKS[longest_queue_index]:
                    for i in range(1, N_WORKERS):
                        idx = (longest_queue_index + i) % N_WORKERS # check for worker {idx}
                        if cur_queue_length[idx] == 0 and QUERY_QUEUES[longest_queue_index]:
                            for steal_index, (qid, _, _, _) in enumerate(QUERY_QUEUES[longest_queue_index]):
                                # do not steal write queries (from master)
                                if not queries[qid].is_write:
                                    with QUERY_QUEUE_LOCKS[idx]:
                                        QUERY_QUEUES[idx].append(QUERY_QUEUES[longest_queue_index][steal_index])
                                        del QUERY_QUEUES[longest_queue_index][steal_index]
                                    break
        # Avoid busy waiting
        time.sleep(IDLE_INTERVAL)
    
    if use_ml_method:
        model_training_timer.stop_running()
        model_copy_timer.stop_running()
        reallocate_timer.stop_running()
        model_training_timer.join()
        model_copy_timer.join()
        reallocate_timer.join()
    
    for i, worker in enumerate(workers):
        FINISHED[i].set()
        worker.join()

    # Calculate cache hit rate
    hit_count, miss_count = 0, 0
    for worker in workers:
        worker.handlers[0].connect()
        for table in ALL_TABLES:
            statio = worker.handlers[0].get_table_statio(table, log_output=False)
            hit_count += (statio[1] + statio[3])
            miss_count += (statio[0] + statio[2])
    
    # Report query execution statistics
    logging.info("queries time usage info: " \
                f"(sum_exec){sum([aet.value for aet in ACC_EXEC_TIME]):.2f}, " \
                f"(sum_finish){sum([aft.value for aft in ACC_FINISH_TIME]):.2f}, " \
                f"(max_finish){max([mft.value for mft in MAX_FINISH_TIME]):.2f}, " \
                f"(hit_rate){hit_count / (hit_count + miss_count):.2f}")
    
    # Write query execution statistics to file
    with open("stats.txt", "a") as f:
        f.write(f"Allocator: {args.allocator} \n" \
                f"Stream: {args.stream} \n" \
                f"No-Steal: {args.no_steal} \n" \
                "queries time usage info: " \
                f"(sum_exec){sum([aet.value for aet in ACC_EXEC_TIME]):.2f}, " \
                f"(sum_finish){sum([aft.value for aft in ACC_FINISH_TIME]):.2f}, " \
                f"(max_finish){max([mft.value for mft in MAX_FINISH_TIME]):.2f}, " \
                f"(hit_rate){hit_count / (hit_count + miss_count+1):.2f} \n")