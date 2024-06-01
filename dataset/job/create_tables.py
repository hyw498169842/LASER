import os
import psycopg2

DATA_DIR = "./jo-bench/csv/"
DATABASE = "job"
USER = "user"
PASSWORD = "password"
MASTER_HOST = "localhost"
MASTER_PORT = 13000

# connect to database
def connect():
    conn = psycopg2.connect(
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        host=MASTER_HOST,
        port=MASTER_PORT
    )
    conn.set_session(autocommit=True)
    return conn
conn = connect()

# create tables
try:
    sql_path = "./jo-bench/schema.sql"
    with conn.cursor() as cursor:
        cursor.execute(open(sql_path, "r").read())
except psycopg2.errors.DuplicateTable:
    print("Tables already exist, skip creating tables.")

# load data
for filename in os.listdir(DATA_DIR):
    if filename.endswith(".csv"):
        table = filename.split(".")[0]
        print("Loading table {}...".format(table))
        # retry for up to 5 times
        for _ in range(5):
            try:
                with conn.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {table};")
                    cursor.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '', QUOTE '\"', ESCAPE '\\')", open(f"{DATA_DIR}/{filename}"))
                break
            except psycopg2.OperationalError:
                print("Connection error, retrying...")
                conn = connect()
    # table contain multiple files
    else:
        num_parts = len(os.listdir(f"{DATA_DIR}/{filename}/"))
        # retry for up to 5 times
        for _ in range(5):
            try:
                with conn.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {filename};")
                for i in range(1, num_parts + 1):
                    print("Loading part {} of {}...".format(i, filename))
                    partname = f"{filename}_{i}.csv"
                    with conn.cursor() as cursor:
                        cursor.copy_expert(f"COPY {filename} FROM STDIN WITH(FORMAT CSV, DELIMITER ',', NULL '', QUOTE '\"', ESCAPE '\\')", open(f"{DATA_DIR}/{filename}/{partname}"))
                break
            except psycopg2.OperationalError:
                print("Connection error, retrying...")
                conn = connect()

# show table size
with conn.cursor() as cursor:
    cursor.execute("""
    SELECT
        table_name,
        pg_relation_size(quote_ident(table_name)),
        pg_size_pretty(pg_relation_size(quote_ident(table_name)))
    FROM information_schema.tables
    WHERE table_schema = 'public';
    """)
    print(cursor.fetchall())

# close connection
conn.close()