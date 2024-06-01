from itertools import combinations

DATABASE = "tpch10g"

# Schema information
ALL_TABLES = ["lineitem", "region", "part", "customer", "orders", "partsupp", "nation", "supplier"]
ALL_INDEXES = [f"{table}_pkey" for table in ALL_TABLES]
N_TABLES = len(ALL_TABLES)
N_INDEXES = len(ALL_INDEXES)

# scan types
ALL_SCAN_TYPES = ["Seq Scan", "Index Scan", "Index Only Scan"]
N_SCAN_TYPES = len(ALL_SCAN_TYPES)

# operator types
ALL_OPERATORS = ["=", "<", ">", "<=", ">=", "<>"]
N_OPERATORS = len(ALL_OPERATORS)

# join conditions
ALL_INDEX_CONDS = [
    # No index condition
    [],
    # self join condition
    ["p_partkey"], ["ps_partkey"], ["l_partkey"], 
    ["s_suppkey"], ["ps_suppkey"], ["l_suppkey"],
    ["n_nationkey"], ["c_nationkey"], ["s_nationkey"],
    ["r_regionkey"], ["n_regionkey"],
    ["c_custkey"], ["o_custkey"],
    ["o_orderkey"], ["l_orderkey"],
]
# two-table join condition
ALL_INDEX_CONDS += list(combinations(["p_partkey", "ps_partkey", "l_partkey"], 2)) + \
                   list(combinations(["s_suppkey", "ps_suppkey", "l_suppkey"], 2)) + \
                   list(combinations(["n_nationkey", "c_nationkey", "s_nationkey"], 2)) + \
                   list(combinations(["r_regionkey", "n_regionkey"], 2)) + \
                   list(combinations(["c_custkey", "o_custkey"], 2)) + \
                   list(combinations(["o_orderkey", "l_orderkey"], 2))
ALL_INDEX_CONDS = [set(cond) for cond in ALL_INDEX_CONDS]
N_INDEX_CONDS = len(ALL_INDEX_CONDS)

# warm up table
WARM_UP_TABLE = "lineitem"

# Input queries
QUERY_DIR = "dataset/tpch/tpch-queries/"
N_QUERIES = 1000
