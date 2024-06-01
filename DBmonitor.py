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

DB_MONITOR = DBmonitor()
