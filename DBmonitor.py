import psycopg2
from config import *

# Store useful information about the databases
class DBmonitor:
    def __init__(self, database=DATABASE, user=USER, password=PASSWORD, host=MASTER_HOST, port=MASTER_PORT):
        self.conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=20
        )
        self.conn.set_session(autocommit=True)

        # number of pages in each table
        # key is of the form "{table_name}"
        self.num_pages = {}

        # flag indicating whether a table is dirty
        # key is of the form "{table_name}"
        self.table_is_dirty = {}

        # initialization using a lazy strategy
        for table in ALL_TABLES:
            self.mark_as_dirty(table)
    
    def get_num_pages(self, table):
        if self.table_is_dirty[table]:
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT relpages FROM pg_class WHERE relname = '{table}';")
                self.num_pages[table] = cursor.fetchone()[0]
                self.table_is_dirty[table] = False
        return self.num_pages[table]
    
    def mark_as_dirty(self, table):
        self.table_is_dirty[table] = True
        self.table_is_dirty[f"{table}_pkey"] = True
        if os.environ["DATASET"] == "sysbench":
            self.table_is_dirty["k_" + table[-1]] = True
        elif os.environ["DATASET"] == "htap":
            del self.table_is_dirty[f"{table}_pkey"]
            self.table_is_dirty[f"{table}_pk"] = True
            if table == "order_line":
                self.table_is_dirty["fkey_order_line"] = True
            if table == "customer":
                self.table_is_dirty["idx_customer"] = True
            if table == "orders":
                self.table_is_dirty["idx_orders"] = True

    def get_tables_by_col(self, col):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT table_name FROM information_schema.columns WHERE column_name = '{col}';")
                return [t[0] for t in cursor.fetchall()]
        except:
            return []

    def get_max_value(self, table, col):
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT max({col}) FROM {table};")
            return cursor.fetchone()[0]
    
    def get_min_value(self, table, col):
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT min({col}) FROM {table};")
            return cursor.fetchone()[0]

DB_MONITOR = DBmonitor()
