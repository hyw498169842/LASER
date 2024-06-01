import psycopg2

# first generate tpch tables in './tpch-tables/10/' folder
# then run this script to create tables in database

DATA_DIR = "./tpch-tables/10/"
DATABASE = "tpch10g"
USER = "user"
PASSWORD = "password"
MASTER_HOST = "localhost"
MASTER_PORT = 13000

# connect to database
conn = psycopg2.connect(
    database=DATABASE,
    user=USER,
    password=PASSWORD,
    host=MASTER_HOST,
    port=MASTER_PORT
)
cursor = conn.cursor()

# create tables
cursor.execute("""
CREATE TABLE IF NOT EXISTS "nation" (
    "n_nationkey"  INT,
    "n_name"       CHAR(25),
    "n_regionkey"  INT,
    "n_comment"    VARCHAR(152),
    PRIMARY KEY ("n_nationkey"));
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS "region" (
    "r_regionkey"  INT,
    "r_name"       CHAR(25),
    "r_comment"    VARCHAR(152),
    PRIMARY KEY ("r_regionkey"));
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS "supplier" (
    "s_suppkey"     INT,
    "s_name"        CHAR(25),
    "s_address"     VARCHAR(40),
    "s_nationkey"   INT,
    "s_phone"       CHAR(15),
    "s_acctbal"     DECIMAL(15,2),
    "s_comment"     VARCHAR(101),
    PRIMARY KEY ("s_suppkey"));
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS "customer" (
    "c_custkey"     INT,
    "c_name"        VARCHAR(25),
    "c_address"     VARCHAR(40),
    "c_nationkey"   INT,
    "c_phone"       CHAR(15),
    "c_acctbal"     DECIMAL(15,2),
    "c_mktsegment"  CHAR(10),
    "c_comment"     VARCHAR(117),
    PRIMARY KEY ("c_custkey"));
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS "part" (
    "p_partkey"     INT,
    "p_name"        VARCHAR(55),
    "p_mfgr"        CHAR(25),
    "p_brand"       CHAR(10),
    "p_type"        VARCHAR(25),
    "p_size"        INT,
    "p_container"   CHAR(10),
    "p_retailprice" DECIMAL(15,2) ,
    "p_comment"     VARCHAR(23) ,
    PRIMARY KEY ("p_partkey"));
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS "partsupp" (
    "ps_partkey"     INT,
    "ps_suppkey"     INT,
    "ps_availqty"    INT,
    "ps_supplycost"  DECIMAL(15,2),
    "ps_comment"     VARCHAR(199),
    PRIMARY KEY ("ps_partkey", "ps_suppkey"));
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS "orders" (
    "o_orderkey"       INT,
    "o_custkey"        INT,
    "o_orderstatus"    CHAR(1),
    "o_totalprice"     DECIMAL(15,2),
    "o_orderdate"      DATE,
    "o_orderpriority"  CHAR(15),
    "o_clerk"          CHAR(15),
    "o_shippriority"   INT,
    "o_comment"        VARCHAR(79),
    PRIMARY KEY ("o_orderkey"));
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS "lineitem"(
    "l_orderkey"          INT,
    "l_partkey"           INT,
    "l_suppkey"           INT,
    "l_linenumber"        INT,
    "l_quantity"          DECIMAL(15,2),
    "l_extendedprice"     DECIMAL(15,2),
    "l_discount"          DECIMAL(15,2),
    "l_tax"               DECIMAL(15,2),
    "l_returnflag"        CHAR(1),
    "l_linestatus"        CHAR(1),
    "l_shipdate"          DATE,
    "l_commitdate"        DATE,
    "l_receiptdate"       DATE,
    "l_shipinstruct"      CHAR(25),
    "l_shipmode"          CHAR(10),
    "l_comment"           VARCHAR(44),
    PRIMARY KEY ("l_orderkey", "l_linenumber"));
""")
conn.commit()

# copy data
tables = ["region", "nation", "customer", "supplier", "part", "partsupp", "orders", "lineitem"]
for table in tables:
    cursor.copy_from(open(f"{DATA_DIR}/{table}.tbl"), table, sep="|")
conn.commit()

# show table size
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
cursor.close()