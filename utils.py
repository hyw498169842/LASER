import sqlglot
import sqlglot.expressions as exp
from subprocess import Popen, PIPE, DEVNULL

from DBmonitor import DB_MONITOR
from config import *


def restart_server(cluster):
    host_map = {"master": MASTER_HOST, "slave1": SLAVE1_HOST, "slave2": SLAVE2_HOST}
    ssh = Popen(["sshpass", "-p", PASSWORD, "ssh", f"{host_map[cluster]}"], \
                stdin=PIPE, stdout=DEVNULL, stderr=DEVNULL)
    ssh.stdin.write(f"cd workspace; ./pg cgstart {cluster}\n".encode())
    ssh.stdin.close()
    ssh.wait()

def is_write_query(query):
    ''' Check if the given query is a write query '''
    parsed = sqlglot.parse_one(query)
    return parsed.find(exp.Insert) or \
           parsed.find(exp.Update) or \
           parsed.find(exp.Delete)

def extract_tables(query):
    parsed = sqlglot.parse_one(query)
    tables = parsed.find_all(exp.Table)
    tables = [t.name for t in tables]
    return [t for t in tables if t in ALL_TABLES]

def extract_scans_from_plan(plan):
    ''' Extract scan operators from the given plan '''
    result = []
    if isinstance(plan, list):
        for p in plan:
            result += extract_scans_from_plan(p)
    elif isinstance(plan, dict):
        if "Plans" in plan:
            result += extract_scans_from_plan(plan["Plans"])
        if plan["Node Type"] in ALL_SCAN_TYPES:
            result.append((
                plan["Node Type"],
                plan.get("Index Cond", None),
                plan.get("Index Name", None),
                plan.get("Relation Name", None),
                # plan.get("Filter", None)
            ))
    else:
        raise ValueError(f"Invalid plan type: {type(plan)}")
    return result

def get_total_pages(table):
    ''' Get the number of pages in the given table '''
    return DB_MONITOR.get_num_pages(table)

def get_new_cached_pages(cache_state_before, cache_state_after, tables):
    ''' Extract the pages that are newly cached after the query '''
    all_keys = tables + [f"{table}_pkey" for table in tables]
    if os.environ["DATASET"] == "sysbench":
        all_keys += ["k_" + table[-1] for table in tables]
    old_pages_dict = {key: dict() for key in all_keys}
    for relation, pageid, usage in cache_state_before:
        if relation in old_pages_dict.keys():
            old_pages_dict[relation][pageid] = usage
    new_pages_dict = {key: set() for key in all_keys}
    for relation, pageid, usage in cache_state_after:
        if relation not in new_pages_dict.keys():
            continue
        if pageid not in old_pages_dict[relation]:
            new_pages_dict[relation].add(pageid)
        elif old_pages_dict[relation][pageid] < usage:
            new_pages_dict[relation].add(pageid)
    return new_pages_dict

def divide_pages_into_buckets(total_pages, cached_pages, n_buckets=N_BUCKETS):
    ''' Divide the pages into buckets, and return % of cached pages in each bucket '''
    bucket_size = total_pages // n_buckets + 1
    cached_pages, buckets = set(cached_pages), []
    for i in range(n_buckets):
        bucket = set(range(i * bucket_size, (i + 1) * bucket_size))
        buckets.append(len(bucket & cached_pages) / bucket_size)
    return buckets

def one_hot_encoding(targets, references):
    return [float(ref in targets) for ref in references]
