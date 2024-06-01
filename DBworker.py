import time
import logging
import psycopg2
import numpy as np
from collections import deque
from multiprocessing import Process
from bisect import bisect_left, bisect_right

from config import *
from utils import *
from DBhandler import DBhandler
from repeatingtimer import RepeatingTimer

class DBworker(Process):
    def __init__(self, wid, cluster, host, port, use_ml_method=True):
        Process.__init__(self)
        self.wid = wid
        self.use_ml_method = use_ml_method
        self.handlers = [
            DBhandler(cluster=cluster, host=host, port=port, auto_connect=False)
            for _ in range(NUM_CONNECTIONS)
        ]
        self.query_buffer = deque(maxlen=NUM_CONNECTIONS)
        if self.use_ml_method:
            # adaptive greedy selection algorithm
            self.w_s = W_S
            self.w_s_step = W_S_STEP
            self.ema_weight = 0.1
            self.db_cache_state = np.zeros((N_TABLES + N_INDEXES) * N_BUCKETS)
            # used to update w_s
            self.exec_time_records = []
            self.last_update_time = time.time()
            # cache state snapshots
            # this should be maintained at trainer, but we put it here for simplicity
            self.snapshots = deque(maxlen=N_SNAPSHOTS)
            self.query_tables = dict()
            self.query_start_time = dict()
            self.query_finish_time = dict()

    def register_global_variables(self, finished, start_time, table_is_modified, \
                                  acc_exec_time, acc_finish_time, max_finish_time, \
                                  query_queue, query_queue_lock, n_finished_queries, \
                                  raw_training_data_queue, fail_query_queue, last_executed_query_ids):
        self.finished = finished
        # global start time
        self.start_time = start_time
        # flag indicating modified tables
        self.table_is_modified = table_is_modified
        # statistics
        self.acc_exec_time = acc_exec_time
        self.acc_finish_time = acc_finish_time
        self.max_finish_time = max_finish_time
        # shared data structures
        self.query_queue = query_queue
        self.query_queue_lock = query_queue_lock
        self.n_finished_queries = n_finished_queries
        self.raw_training_data_queue = raw_training_data_queue
        self.fail_query_queue = fail_query_queue
        self.last_executed_query_ids = last_executed_query_ids
    
    def connect(self):
        try:
            self.conn = psycopg2.connect(
                database=self.handlers[0].database,
                user=self.handlers[0].user,
                password=self.handlers[0].password,
                host=self.handlers[0].host,
                port=self.handlers[0].port,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=20
            )
            self.conn.set_session(autocommit=True)
        except psycopg2.OperationalError:
            restart_server(self.handlers[0].cluster)
            time.sleep(IDLE_INTERVAL)
            self.connect()

    def get_idle_handler(self):
        for handler in self.handlers:
            if not handler.is_busy:
                return handler
        return None
    
    def cal_inverse_pair_ratio(self):
        # this function returns the ratio between actual inverse pairs and the expectation
        n = len(self.exec_time_records)
        expectation = n * (n - 1) / 4
        # calculate the number of inverse pairs
        total, cnts = 0, [0] * n
        rank = {x: i for i, x in enumerate(sorted(self.exec_time_records))}
        for x in reversed(self.exec_time_records):
            total += sum(cnts[:rank[x]])
            cnts[rank[x]] += 1
        return total / expectation
    
    def update_select_weight(self, is_increase):
        if is_increase:
            self.w_s += self.w_s_step
        else:
            self.w_s -= self.w_s_step
            self.w_s = max(self.w_s, 0)
    
    def select_queries(self):
        # update w_s
        if self.use_ml_method and \
           len(self.exec_time_records) > 1 and \
           time.time() - self.last_update_time > QUERY_REALLOCATION_INTERVAL:
            inverse_pair_ratio = self.cal_inverse_pair_ratio()
            if inverse_pair_ratio < 0.25: # customized threshold
                self.update_select_weight(False)
            elif inverse_pair_ratio > 0.75: 
                self.update_select_weight(True)
            self.exec_time_records = []
            self.last_update_time = time.time()
        # select the best queries
        n_idle_handlers = sum([(not h.is_busy) for h in self.handlers])
        n_select = max(n_idle_handlers, QUERY_QUEUE_MINIMUM_RESERVE)
        with self.query_queue_lock:
            for _ in range(n_select):
                if not self.query_queue:
                    break
                if not self.use_ml_method:
                    self.query_buffer.append(self.query_queue[0][:2])
                    self.query_queue.popleft()
                else:
                    # select the query with the best score
                    dists, costs, total_cost = [], [], 0
                    for _, _, cost, cache_state in self.query_queue:
                        dists.append(np.linalg.norm(self.db_cache_state - cache_state, ord=1))
                        costs.append(cost)
                        total_cost += cost
                    best_idx = np.argmin([dist + self.w_s * (cost / total_cost) for dist, cost in zip(dists, costs)])
                    self.query_buffer.append(self.query_queue[best_idx][:2])
                    # update using ema
                    self.db_cache_state = (1 - self.ema_weight) * self.db_cache_state + \
                                          self.ema_weight * self.query_queue[best_idx][-1]
                    # remove the query from original queue
                    del self.query_queue[best_idx]
    
    def take_snapshot(self):
        def select_left_snapshot(x):
            return max(bisect_right(self.snapshots, x) - 1, 0)
        def select_right_snapshot(x):
            return bisect_left(self.snapshots, x)
        # get current cachestate of database
        timestamp = time.time() - self.start_time.value
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(f"""
                    SELECT c.relname, b.relblocknumber, b.usagecount
                    FROM pg_buffercache b 
                    JOIN pg_class c
                        ON b.relfilenode = pg_relation_filenode(c.oid)
                        AND b.reldatabase IN (0, (SELECT oid FROM pg_database WHERE datname = current_database()))
                    WHERE c.relname in {tuple(ALL_TABLES + ALL_INDEXES)};
                """)
                self.snapshots.append((timestamp, cursor.fetchall()))
            except psycopg2.OperationalError:
                self.connect()
        # check for training data
        qids = list(self.query_start_time.keys())
        for qid in qids:
            if qid in self.query_finish_time.keys():
                tables = self.query_tables[qid]
                start_time = self.query_start_time[qid]
                finish_time = self.query_finish_time[qid]
                old_state_idx = select_left_snapshot((start_time, None))
                new_state_idx = select_right_snapshot((finish_time, None))
                if new_state_idx != len(self.snapshots):
                    new_pages_dict = get_new_cached_pages(
                        self.snapshots[old_state_idx][1],
                        self.snapshots[new_state_idx][1],
                        tables)
                    self.raw_training_data_queue.put((qid, new_pages_dict, finish_time))
                    self.query_tables.pop(qid)
                    self.query_start_time.pop(qid)
                    self.query_finish_time.pop(qid)
        # eliminate useless snapshots
        keep_snapshot_indexes = set()
        keep_snapshot_indexes.add(len(self.snapshots) - 1) # always keep the newest snapshot
        for start_time in self.query_start_time.values():
            keep_snapshot_indexes.add(select_left_snapshot((start_time, None)))
        keep_snapshots = [self.snapshots[i] for i in keep_snapshot_indexes]
        self.snapshots = deque(keep_snapshots, maxlen=N_SNAPSHOTS)

    def run(self):
        # this connection is used only for snapshots and meta operation
        self.connect()
        # reset cache statistics
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT pg_stat_reset();")
        if self.use_ml_method:
            snapshot_timer = RepeatingTimer(SNAPSHOT_INTERVAL, self.take_snapshot)
            snapshot_timer.start()

        # start multi-threading connection
        for handler in self.handlers:
            handler.init()
            handler.start()

        # event loop
        while not self.finished.is_set():
            # send query to the handlers
            while self.query_buffer:
                handler = self.get_idle_handler()
                if not handler:
                    break
                handler.is_busy = True
                with handler.lock:
                    qid, query_str = self.query_buffer.popleft()
                    handler.query_info = qid, query_str
                    if self.use_ml_method:
                        self.query_start_time[qid] = time.time() - self.start_time.value
            # select the new "best" queries
            if not self.query_buffer:
                self.select_queries()
                if not self.query_buffer:
                    # avoid busy waiting
                    time.sleep(IDLE_INTERVAL)
                else:
                    self.last_executed_query_ids[self.wid] = self.query_buffer[-1][0]
            # collect statistics
            stat_list = []
            for handler in self.handlers:
                with handler.lock:
                    stat_list += handler.stat_list
                    handler.stat_list = []
            # Sort completed queries by finish time
            stat_list = sorted(stat_list, key=lambda x: x[4])
            for stat in stat_list:
                qid, query_str, success, exec_time, finish_time = stat
                # subtract start_time to get relative value
                finish_time = finish_time - self.start_time.value
                if self.use_ml_method:
                    tables = extract_tables(query_str)
                    # notify the database change
                    if is_write_query(query_str):           
                        for table in tables:
                            self.table_is_modified[ALL_TABLES.index(table)] = 1
                    # collect training data
                    if success:
                        self.exec_time_records.append(exec_time)
                        self.query_finish_time[qid] = finish_time
                        self.query_tables[qid] = tables
                    else:
                        self.query_start_time.pop(qid)
                # statistics
                if success:
                    self.acc_exec_time.value += exec_time
                    self.acc_finish_time.value += finish_time
                    self.max_finish_time.value = finish_time
                    self.n_finished_queries.value += 1
                else:
                    self.fail_query_queue.put(qid)
        
        # end all the connections
        if self.use_ml_method:
            snapshot_timer.stop_running()
            snapshot_timer.join()
        for handler in self.handlers:
            handler.finished.set()
            handler.join()
