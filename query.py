from DBmonitor import DB_MONITOR
from utils import is_write_query, extract_scans_from_plan

class Query:
    def __init__(self, qid, query_str, update_plan_on_init=True):
        self.qid = qid
        # For TPC-DS, some sql files contain multiple queries
        # In such case, we only use the first query
        first_query_end = query_str.find(";")
        self.query_str = query_str[:first_query_end + 1]
        self.is_write = is_write_query(self.query_str)
        if update_plan_on_init:
            self.update_plan()
    
    def update_plan(self):
        with DB_MONITOR.conn.cursor() as cur:
            cur.execute(f"explain (format json) {self.query_str};")
            self.plan = cur.fetchone()[0][0]["Plan"]
        self.scans = extract_scans_from_plan(self.plan)
        self.cost = self.plan["Total Cost"]