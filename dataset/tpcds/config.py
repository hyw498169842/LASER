DATABASE = "tpcds10g"

# Schema information
ALL_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item", "promotion",
    "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim",
    "warehouse", "web_page", "web_returns", "web_sales", "web_site"
]
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
    ["cc_call_center_sk"], ["cp_catalog_page_sk"], ["cr_item_sk"], ["cr_order_number"],
    ["cs_item_sk"], ["cs_order_number"], ["c_customer_sk"], ["ca_address_sk"],
    ["cd_demo_sk"], ["d_date_sk"], ["hd_demo_sk"], ["ib_income_band_sk"],
    ["inv_date_sk"], ["inv_item_sk"], ["inv_warehouse_sk"], ["i_item_sk"],
    ["p_promo_sk"], ["r_reason_sk"], ["sm_ship_mode_sk"], ["s_store_sk"],
    ["sr_item_sk"], ["sr_ticket_number"], ["ss_item_sk"], ["ss_ticket_number"],
    ["t_time_sk"], ["w_warehouse_sk"], ["wp_web_page_sk"], ["wr_item_sk"],
    ["wr_order_number"], ["ws_item_sk"], ["ws_order_number"], ["web_site_sk"],
]

# two-table join condition
ALL_INDEX_CONDS += [
    # store_sales
    ["ss_sold_date_sk", "d_date_sk"], ["ss_sold_time_sk", "t_time_sk"], ["ss_item_sk", "i_item_sk"], ["ss_customer_sk", "c_customer_sk"], 
    ["ss_cdemo_sk", "cd_demo_sk"], ["ss_hdemo_sk", "hd_demo_sk"], ["ss_addr_sk", "ca_address_sk"], ["ss_store_sk", "s_store_sk"],
    ["ss_promo_sk", "p_promo_sk"], ["ss_item_sk", "inv_item_sk"],
    # store_returns
    ["sr_returned_date_sk", "d_date_sk"], ["sr_return_time_sk", "t_time_sk"], ["sr_item_sk", "i_item_sk"], ["sr_item_sk", "ss_item_sk"],
    ["sr_customer_sk", "c_customer_sk"], ["sr_cdemo_sk", "cd_demo_sk"], ["sr_hdemo_sk", "hd_demo_sk"], ["sr_addr_sk", "ca_address_sk"],
    ["sr_store_sk", "s_store_sk"], ["sr_reason_sk", "r_reason_sk"], ["sr_ticket_number", "ss_ticket_number"],
    # catalog_sales
    ["cs_sold_date_sk", "d_date_sk"], ["cs_sold_time_sk", "t_time_sk"], ["cs_item_sk", "i_item_sk"], ["cs_bill_customer_sk", "c_customer_sk"],
    ["cs_bill_cdemo_sk", "cd_demo_sk"], ["cs_bill_hdemo_sk", "hd_demo_sk"], ["cs_bill_addr_sk", "ca_address_sk"], ["cs_ship_customer_sk", "c_customer_sk"],
    ["cs_ship_cdemo_sk", "cd_demo_sk"], ["cs_ship_hdemo_sk", "hd_demo_sk"], ["cs_ship_addr_sk", "ca_address_sk"], ["cs_call_center_sk", "cc_call_center_sk"],
    ["cs_catalog_page_sk", "cp_catalog_page_sk"], ["cs_ship_mode_sk", "sm_ship_mode_sk"], ["cs_warehouse_sk", "w_warehouse_sk"],
    ["cs_promo_sk", "p_promo_sk"], ["cs_ship_date_sk", "d_date_sk"], ["cs_item_sk", "inv_item_sk"],
    # catalog_returns
    ["cr_returned_date_sk", "d_date_sk"], ["cr_returned_time_sk", "t_time_sk"], ["cr_item_sk", "i_item_sk"], ["cr_item_sk", "cs_item_sk"],
    ["cr_refunded_customer_sk", "c_customer_sk"], ["cr_refunded_cdemo_sk", "cd_demo_sk"], ["cr_refunded_hdemo_sk", "hd_demo_sk"], ["cr_refunded_addr_sk", "ca_address_sk"], 
    ["cr_returning_customer_sk", "c_customer_sk"], ["cr_returning_cdemo_sk", "cd_demo_sk"], ["cr_returning_hdemo_sk", "hd_demo_sk"], ["cr_returning_addr_sk", "ca_address_sk"], 
    ["cr_call_center_sk", "cc_call_center_sk"], ["cr_catalog_page_sk", "cp_catalog_page_sk"], ["cr_ship_mode_sk", "sm_ship_mode_sk"], ["cr_warehouse_sk", "w_warehouse_sk"],
    ["cr_reason_sk", "r_reason_sk"], ["cr_order_number", "cs_order_number"],
    # web_sales
    ["ws_sold_date_sk", "d_date_sk"], ["ws_sold_time_sk", "t_time_sk"], ["ws_item_sk", "i_item_sk"], ["ws_bill_customer_sk", "c_customer_sk"],
    ["ws_bill_cdemo_sk", "cd_demo_sk"], ["ws_bill_hdemo_sk", "hd_demo_sk"], ["ws_bill_addr_sk", "ca_address_sk"], ["ws_ship_customer_sk", "c_customer_sk"],
    ["ws_ship_cdemo_sk", "cd_demo_sk"], ["ws_ship_hdemo_sk", "hd_demo_sk"], ["ws_ship_addr_sk", "ca_address_sk"], ["ws_web_page_sk", "wp_web_page_sk"],
    ["ws_web_site_sk", "web_site_sk"], ["ws_ship_mode_sk", "sm_ship_mode_sk"], ["ws_warehouse_sk", "w_warehouse_sk"], ["ws_promo_sk", "p_promo_sk"],
    ["ws_ship_date_sk", "d_date_sk"],
    # web_returns
    ["wr_returned_date_sk", "d_date_sk"], ["wr_returned_time_sk", "t_time_sk"], ["wr_item_sk", "i_item_sk"], ["wr_item_sk", "ws_item_sk"],
    ["wr_refunded_customer_sk", "c_customer_sk"], ["wr_refunded_cdemo_sk", "cd_demo_sk"], ["wr_refunded_hdemo_sk", "hd_demo_sk"], ["wr_refunded_addr_sk", "ca_address_sk"],
    ["wr_returning_customer_sk", "c_customer_sk"], ["wr_returning_cdemo_sk", "cd_demo_sk"], ["wr_returning_hdemo_sk", "hd_demo_sk"], ["wr_returning_addr_sk", "ca_address_sk"],
    ["wr_web_page_sk", "wp_web_page_sk"], ["wr_reason_sk", "r_reason_sk"], ["wr_order_number", "ws_order_number"],
    # inventory
    ["inv_date_sk", "d_date_sk"], ["inv_item_sk", "i_item_sk"], ["inv_warehouse_sk", "w_warehouse_sk"],
    # store
    ["s_closed_date_sk", "d_date_sk"],
    # call_center
    ["cc_closed_date_sk", "d_date_sk"], ["cc_open_date_sk", "d_date_sk"],
    # catalog_page
    ["cp_start_date_sk", "d_date_sk"], ["cp_end_date_sk", "d_date_sk"],
    # web_site
    ["web_open_date_sk", "d_date_sk"], ["web_close_date_sk", "d_date_sk"],
    # web_page
    ["wp_creation_date_sk", "d_date_sk"], ["wp_access_date_sk", "d_date_sk"], ["wp_customer_sk", "c_customer_sk"],
    # customer
    ["c_current_cdemo_sk", "cd_demo_sk"], ["c_current_hdemo_sk", "hd_demo_sk"], ["c_current_addr_sk", "ca_address_sk"],
    ["c_first_shipto_date_sk", "d_date_sk"], ["c_first_sales_date_sk", "d_date_sk"], ["c_last_review_date_sk", "d_date_sk"],
    # household_demographics
    ["hd_income_band_sk", "ib_income_band_sk"],
    # promotion
    ["p_start_date_sk", "d_date_sk"], ["p_end_date_sk", "d_date_sk"], ["p_item_sk", "i_item_sk"],
]

ALL_INDEX_CONDS = [set(cond) for cond in ALL_INDEX_CONDS]
N_INDEX_CONDS = len(ALL_INDEX_CONDS)

# warm up table
WARM_UP_TABLE = "store_sales"

# Input queries
QUERY_DIR = "dataset/tpcds/tpcds-queries"
N_QUERIES = 1000
