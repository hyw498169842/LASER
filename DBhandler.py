import time
import logging
import psycopg2
from datetime import datetime
from threading import Thread, Event, Lock

from config import *
from utils import restart_server

class DBhandler(Thread):
    def __init__(self, cluster, host, port, database=DATABASE, user=USER, password=PASSWORD, auto_connect=True):
        Thread.__init__(self)
        self.cluster = cluster
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        if auto_connect:
            self.connect()
    
    def init(self):
        self.lock = Lock()
        self.finished = Event()
        self.query_info = -1, "", None, None, None
        self.stat_list = []
        self.is_busy = False
        self.connect()
    
    def run(self):
        while not self.finished.is_set():
            with self.lock:
                qid, query_str, txn_str, txn_args, txn_type = self.query_info
                if qid != -1:
                    self.query_info = -1, "", None, None, None
            if qid == -1:
                # wait for new query
                time.sleep(IDLE_INTERVAL)
                continue
            query_to_execute = query_str if txn_str is None else txn_str
            if txn_type in ["new_order", "payment", "delivery"]:
                txn_args[-1] = datetime.fromtimestamp(txn_args[-1])
            success, exec_time = self.time_single_query(query_to_execute, args=txn_args, log_output=False)
            finish_time = time.time()
            with self.lock:
                self.stat_list.append((qid, query_str, success, exec_time, finish_time))          
            self.is_busy = False
    
    def connect(self):
        try:
            self.conn = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=20
            )
            self.conn.set_session(autocommit=True)
        except psycopg2.OperationalError:
            if LOG_CONNECTION_ERROR:
                logging.info(f"{self.cluster} may be down, restarting...")
            restart_server(self.cluster)
            time.sleep(IDLE_INTERVAL)
            self.connect()
    
    def try_execute(self, query, args=None, max_retry=3):
        for _ in range(max_retry):
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(query, args)
                    if cursor.description:
                        return cursor.description, cursor.fetchall(), cursor.rowcount
                    else:
                        return None, None, cursor.rowcount
            except (psycopg2.OperationalError, psycopg2.DatabaseError):
                if LOG_CONNECTION_ERROR:
                    logging.info(f"{self.cluster} connection error, retrying...")
                self.connect()
        # raise Exception(f"{self.cluster} connection error, max retry exceeded.")
        if LOG_EXECUTION_ERROR:
            logging.error(f"{self.cluster} execution failed: {query}")
        return None, None, -1
    
    def time_single_query(self, query, args=None, log_output=True):
        start = time.time()
        desc, _, rowcount = self.try_execute(query, args=args)
        elapsed = time.time() - start
        if log_output:
            logging.info(f"{self.cluster} query time: {elapsed}")
        success = (desc is not None) or (rowcount != -1)
        return success, elapsed

    def warm_up(self, log_output=True):
        self.try_execute(f"SELECT pg_prewarm('{WARM_UP_TABLE}');")
        if log_output:
            logging.info(f"{self.cluster} warmed up with {WARM_UP_TABLE}")
    
    def clear_table_statio(self):
        self.try_execute("SELECT pg_stat_reset();")
    
    def get_table_statio(self, table_name, log_output=True):
        columns, values, _ = self.try_execute(f"""
            SELECT heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit
            FROM pg_statio_user_tables 
            WHERE relname = '{table_name}';
        """)
        if log_output:
            logging.info(f"{self.cluster} {table_name} statio: {dict(zip([col[0] for col in columns], values[0]))}")
        return values[0]
    
    def get_buffer_status(self, log_output=True):
        _, buffer_status, _ = self.try_execute("""
            SELECT c.relname, count(*) AS buffers
            FROM pg_buffercache b 
            JOIN pg_class c
                ON b.relfilenode = pg_relation_filenode(c.oid)
                AND b.reldatabase IN (0, (SELECT oid FROM pg_database WHERE datname = current_database()))
            JOIN pg_namespace n
                ON n.oid = c.relnamespace
                AND n.nspname = 'public'
            GROUP BY c.relname;
        """)
        if log_output:
            logging.info(f"{self.cluster} buffer status: {buffer_status}")
        return buffer_status
    
    def get_cached_pages_and_usage(self):
        _, result, _ = self.try_execute(f"""
            SELECT c.relname, b.relblocknumber, b.usagecount
            FROM pg_buffercache b 
            JOIN pg_class c
                ON b.relfilenode = pg_relation_filenode(c.oid)
                AND b.reldatabase IN (0, (SELECT oid FROM pg_database WHERE datname = current_database()))
            WHERE c.relname in {tuple(ALL_TABLES + ALL_INDEXES)};
        """)
        return result
    