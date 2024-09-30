import json
from DBmonitor import DB_MONITOR
from utils import is_write_query, extract_scans_from_plan

class Query:
    def __init__(self, qid, query_str, update_plan_on_init=True):
        self.qid = qid
        # For TPC-DS, some sql files contain multiple queries
        # In such case, we only use the first query
        first_query_end = query_str.find(";")
        self.query_str = query_str[:first_query_end + 1] if first_query_end >= 0 else query_str
        
        try:
            txn_info = json.loads(self.query_str) # Handle htap transactions
            self.txn_str, self.txn_args, self.txn_type = txn_info["sql"], txn_info["args"], txn_info["type"]
            self.is_write = self.txn_type in ["new_order", "payment", "delivery"]
            if self.txn_type == "new_order":
                self.query_str = f"""SELECT c_discount, c_last, c_credit, w_tax
                                    FROM customer, warehouse
                                    WHERE w_id = {self.txn_args[0]}
                                    AND c_w_id = w_id
                                    AND c_d_id = {self.txn_args[2]}
                                    AND c_id = {self.txn_args[1]};"""
            elif self.txn_type == "payment":
                self.query_str = f"""UPDATE warehouse
                                    SET w_ytd = w_ytd + {self.txn_args[5]}
                                    WHERE w_id = {self.txn_args[0]};"""
            elif self.txn_type == "delivery":
                self.query_str = f"""SELECT COALESCE(MIN(no_o_id), 0)
                                    FROM new_orders
                                    WHERE no_d_id = 1
                                    AND no_w_id = {self.txn_args[0]}"""
            elif self.txn_type == "order_status":
                if self.txn_args[-1]:
                    self.query_str = f"""SELECT count(c_id)
                                        FROM customer
                                        WHERE c_w_id = {self.txn_args[0]}
                                        AND c_d_id = {self.txn_args[1]}
                                        AND c_last = '{self.txn_args[3]}';"""
                else:
                    self.query_str = f"""SELECT c_balance, c_first, c_middle, c_last, c_id
                                        FROM customer
                                        WHERE c_w_id = {self.txn_args[0]}
                                        AND c_d_id = {self.txn_args[1]}
                                        AND c_id = {self.txn_args[2]};"""
            elif self.txn_type == "stock_level":
                self.query_str = f"""SELECT d_next_o_id
                                    FROM district
                                    WHERE d_id = {self.txn_args[1]}
                                    AND d_w_id = {self.txn_args[0]};"""
        except json.decoder.JSONDecodeError:
            self.txn_str = None
            self.txn_args = None
            self.txn_type = None
            self.is_write = is_write_query(self.query_str)
                
        if update_plan_on_init:
            self.update_plan()
    
    def update_plan(self):
        with DB_MONITOR.conn.cursor() as cur:
            cur.execute(f"explain (format json) {self.query_str};")
            self.plan = cur.fetchone()[0][0]["Plan"]
        self.scans = extract_scans_from_plan(self.plan)
        self.cost = self.plan["Total Cost"]