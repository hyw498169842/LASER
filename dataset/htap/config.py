DATABASE = "htap"

# Schema information
ALL_TABLES = [
    "stock", "customer", "order_line", "district", "history", "item",
    "nation", "new_orders", "orders", "region", "supplier", "warehouse"
]
ALL_INDEXES = [f"{table}_pk" for table in ALL_TABLES] + ["fkey_order_line", "idx_customer", "idx_orders"]
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
    # two-table join conditions
    ["ol_supply_w_id", "s_w_id"], ["ol_i_id", "s_i_id"], ["o_w_id", "ol_w_id"], ["o_d_id", "ol_d_id"], ["o_id", "ol_o_id"],
    ["c_w_id", "o_w_id"], ["c_d_id", "o_d_id"], ["c_id", "o_c_id"], ["ol_w_id", "no_w_id"], ["ol_d_id", "no_d_id"], ["ol_o_id", "no_o_id"],
]
ALL_INDEX_CONDS = [set(cond) for cond in ALL_INDEX_CONDS]
N_INDEX_CONDS = len(ALL_INDEX_CONDS)

# warm up table
WARM_UP_TABLE = "order_line"

# Input queries
QUERY_DIR = "dataset/htap/htap-queries"
N_QUERIES = 1000
