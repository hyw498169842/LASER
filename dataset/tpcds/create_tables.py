import os
import psycopg2

# first generate tpcds tables in './tpcds-tables/10/' folder
# then run this script to create tables in database

DATA_DIR = "./tpcds-tables/10/"
DATABASE = "tpcds10g"
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
    sql_path = "./tpcds-kit/tools/tpcds.sql"
    with conn.cursor() as cursor:
        cursor.execute(open(sql_path, "r").read())
except psycopg2.errors.DuplicateTable:
    print("Tables already exist, skip creating tables.")

# load data
for filename in os.listdir(DATA_DIR):
    if filename.endswith(".dat"):
        table = filename.split(".")[0]
        print("Loading table {}...".format(table))
        # retry for up to 5 times
        for _ in range(5):
            try:
                with conn.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {table};")
                    cursor.copy_from(open(f"{DATA_DIR}/{filename}"), table, sep="|", null="")
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
