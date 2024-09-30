import re
import time
import torch
import heapq
import sqlglot
import sqlglot.expressions as exp
from threading import Lock
from datetime import datetime
from dateutil.relativedelta import relativedelta

from config import *
from shared import *
from utils import extract_tables
from DBmonitor import DB_MONITOR


def send_to(wid, query, cache_state=None):
    with QUERY_QUEUE_LOCKS[wid]:
        if cache_state is not None:
            QUERY_QUEUES[wid].append((query.qid, query.query_str, query.cost, cache_state.detach().numpy(), query.txn_str, query.txn_args, query.txn_type))
        else:
            QUERY_QUEUES[wid].append((query.qid, query.query_str, None, None, query.txn_str, query.txn_args, query.txn_type))


class RRAllocator:
    def __init__(self, n_workers=N_WORKERS):
        self.n_workers = n_workers
        self.counter = 0
    
    def allocate(self, queries):
        for query in queries:
            if query.is_write:
                send_to(0, query)
            else:
                send_to(self.counter, query)
                self.counter = (self.counter + 1) % self.n_workers


class FIXAllocator:
    def __init__(self, n_workers=N_WORKERS):
        self.n_workers = n_workers
    
    def allocate(self, queries):
        for query in queries:
            if query.is_write:
                send_to(0, query)
            else:
                tables = extract_tables(query.query_str)
                table_idx = ALL_TABLES.index(list(tables)[0])
                send_to(table_idx % self.n_workers, query)


class MINAllocator:
    def __init__(self, n_workers=N_WORKERS):
        self.n_workers = n_workers
    
    def allocate(self, queries):
        for query in queries:
            if query.is_write:
                send_to(0, query)
            else:
                cur_length = get_cur_queue_length()
                send_to(cur_length.index(min(cur_length)), query)


class QCKAllocator:
    def __init__(self, n_workers=N_WORKERS):
        self.n_workers = n_workers
        self.alloc_time = dict()
    
    def allocate(self, queries):
        for query in queries:
            if query.is_write:
                send_to(0, query)
            else:
                queueing_time = [0 for _ in range(self.n_workers)]
                for i in range(self.n_workers):
                    with QUERY_QUEUE_LOCKS[i]:
                        for qid, _, _, _, _, _, _ in QUERY_QUEUES[i]:
                            queueing_time[i] += time.time() - self.alloc_time[qid]
                send_to(queueing_time.index(min(queueing_time)), query)
            self.alloc_time[query.qid] = time.time()
    

class CASAllocator:
    def __init__(self, n_workers=N_WORKERS):
        self.n_workers = n_workers
        self.len_sig = 128  # signature length
        self.len_hist = 8   # history length
        self.histories = [
            deque(maxlen=self.len_hist) for _ in range(self.n_workers)
        ]
        self.max_values, self.min_values = dict(), dict()
    
    def is_number_expr(self, expr):
        try:
            return type(eval(expr)) in [int, float]
        except:
            return False
    
    def convert_date_to_number(self, date_str):
        date = re.findall(r"(\d+-\d+-\d+)", date_str)[0]
        date = datetime.strptime(date, "%Y-%m-%d")
        if "INTERVAL" in date_str:
            interval = date_str.split(" INTERVAL ")[-1].strip("()")
            num, unit = interval.split()
            if unit == "day":
                delta = relativedelta(days=int(num.strip("'")))
            elif unit == "month":
                delta = relativedelta(months=int(num.strip("'")))
            elif unit == "year":
                delta = relativedelta(years=int(num.strip("'")))
            # apply interval
            if " + INTERVAL " in date_str:
                date += delta
            else:
                date -= delta
        return (date - datetime.strptime("1970-01-01", "%Y-%m-%d")).days

    def merge_signature(self, sig_dict, table, col, signature):
        if (table, col) not in sig_dict:
            sig_dict[(table, col)] = signature
        else:
            sig_dict[(table, col)] = [min(a, b) for a, b in zip(sig_dict[(table, col)], signature)]
    
    def get_table_by_col(self, identifier, alias_dict):
        if "." in identifier:
            table, col = identifier.split(".")
        else:
            table, col = None, identifier
        # replace with orignal name
        if col in alias_dict:
            col = alias_dict[col]
        if table in alias_dict:
            table = alias_dict[table]
        # get all possible tables
        tables = DB_MONITOR.get_tables_by_col(col)
        if not tables:
            table = None
        elif len(tables) == 1:
            table = tables[0]
        elif len(tables) > 1:
            assert table in tables
        return table, col

    def get_query_signature(self, query):
        parsed = sqlglot.parse_one(query.query_str)
        alias_dict = dict()
        for expr in parsed.find_all(exp.Alias, exp.Table):
            if str(expr).count(" AS ") == 1:
                origin, alias = str(expr).split(" AS ")
                alias_dict[alias] = origin
        all_predicates = parsed.find_all(exp.Predicate)
        join_predicates = [] # to be processed later
        query_signature = dict()
        for predicate in all_predicates:
            words = str(predicate).split()
            left, op, right = words[0], words[1], " ".join(words[2:])
            # single table query
            if len(list(parsed.find_all(exp.Table))) == 1:
                table = list(parsed.find_all(exp.Table))[0]
                left = str(table) + "." + left
            # skip non-column predicates
            table, col = self.get_table_by_col(left, alias_dict)
            if table is None:
                continue
            # multi-column predicates
            ncols = len(list(predicate.find_all(exp.Column)))
            if ncols == 2:
                join_predicates.append(predicate)
                continue
            elif ncols > 2:
                self.merge_signature(query_signature, table, col, [1 for _ in range(self.len_sig)])
                continue
            # normal predicates
            signature = [0 for _ in range(self.len_sig)]
            if self.is_number_expr(right) or (op.upper() == "BETWEEN" and right[-1].isdigit()):
                if not (table, col) in self.max_values:
                    self.max_values[(table, col)] = DB_MONITOR.get_max_value(table, col)
                if not (table, col) in self.min_values:
                    self.min_values[(table, col)] = DB_MONITOR.get_min_value(table, col)
                maxv, minv = float(self.max_values[(table, col)]), float(self.min_values[(table, col)])
                if op.upper() == "BETWEEN":
                    lb, ub = right.split(" AND ")
                    lb, ub = float(eval(lb)), float(eval(ub))
                    bin_idx_lb = int((lb - minv) / (maxv - minv + 1e-5) * self.len_sig)
                    bin_idx_ub = int((ub - minv) / (maxv - minv + 1e-5) * self.len_sig)
                    for i in range(bin_idx_lb, min(bin_idx_ub + 1, self.len_sig)):
                        signature[i] = 1
                else:
                    right = float(eval(right))
                    bin_idx = int((right - minv) / (maxv - minv + 1e-5) * self.len_sig)
                    # get signature for this predicate
                    if op == '=':
                        signature[bin_idx] = 1
                    elif op in ["<", "<="]:
                        for i in range(min(bin_idx + 1, self.len_sig)):
                            signature[i] = 1
                    elif op in [">", ">="]:
                        for i in range(bin_idx, self.len_sig):
                            signature[i] = 1
                    else:
                        signature = [1 for _ in range(self.len_sig)]
            elif "DATE" in right:
                if not (table, col) in self.max_values:
                    self.max_values[(table, col)] = DB_MONITOR.get_max_value(table, col)
                if not (table, col) in self.min_values:
                    self.min_values[(table, col)] = DB_MONITOR.get_min_value(table, col)
                maxv, minv = self.max_values[(table, col)], self.min_values[(table, col)]
                maxv_num = self.convert_date_to_number(str(maxv))
                minv_num = self.convert_date_to_number(str(minv))
                if op.upper() == "BETWEEN":
                    date_lb, date_ub = right.split(" AND ")
                    date_lb_num = self.convert_date_to_number(date_lb)
                    date_ub_num = self.convert_date_to_number(date_ub)
                    date_lb_idx = int((date_lb_num - minv_num) / (maxv_num - minv_num + 1e-5) * self.len_sig)
                    date_ub_idx = int((date_ub_num - minv_num) / (maxv_num - minv_num + 1e-5) * self.len_sig)
                    for i in range(date_lb_idx, min(date_ub_idx + 1, self.len_sig)):
                        signature[i] = 1
                else:
                    date_num = self.convert_date_to_number(right)
                    date_idx = int((date_num - minv_num) / (maxv_num - minv_num + 1e-5) * self.len_sig)
                    if op == '=':
                        signature[date_idx] = 1
                    elif op in ["<", "<="]:
                        for i in range(min(date_idx + 1, self.len_sig)):
                            signature[i] = 1
                    elif op in [">", ">="]:
                        for i in range(date_idx, self.len_sig):
                            signature[i] = 1
                    else:
                        signature = [1 for _ in range(self.len_sig)]
            else:
                signature = [1 for _ in range(self.len_sig)]
            # merge the signatures
            self.merge_signature(query_signature, table, col, signature)
        # process join predicates
        for predicate in join_predicates:
            left, right = map(str, predicate.find_all(exp.Column))
            # single table query
            if len(list(parsed.find_all(exp.Table))) == 1:
                table = list(parsed.find_all(exp.Table))[0]
                left = str(table) + "." + left
                right = str(table) + "." + right
            table1, col1 = self.get_table_by_col(left, alias_dict)
            table2, col2 = self.get_table_by_col(right, alias_dict)
            if table1 is None or table2 is None:
                continue
            signature = [1 for _ in range(self.len_sig)]
            for table, col in query_signature.keys():
                if table == table1:
                    signature = [min(a, b) for a, b in zip(signature, query_signature[(table, col)])]
            self.merge_signature(query_signature, table1, col1, signature)
            self.merge_signature(query_signature, table2, col2, signature)
        return query_signature

    def allocate_batch(self, queries, signatures):
        # allocate by batches for acceleration
        remaining_query_indexes = set(range(len(queries)))
        while remaining_query_indexes:
            max_score, best_query_idx, target = -1, -1, -1
            for qidx in remaining_query_indexes:
                for widx in range(self.n_workers):
                    if queries[qidx].is_write and widx != 0:
                        continue
                    score = 0
                    for table, col in signatures[qidx].keys():
                        for history in self.histories[widx]:
                            if (table, col) in history:
                                score += sum([a * b for a, b in zip(signatures[qidx][(table, col)], history[(table, col)])])
                    # prevent starvation of workers with fewer histories
                    if len(self.histories[widx]) < self.len_hist:
                        score = 1e8
                    if score > max_score:
                        max_score, best_query_idx, target = score, qidx, widx
            send_to(target, queries[best_query_idx])
            self.histories[target].append(signatures[best_query_idx])
            remaining_query_indexes.remove(best_query_idx)
    
    def allocate(self, queries):
        signatures = [self.get_query_signature(query) for query in queries]
        for i in range(0, len(queries), 50):
            self.allocate_batch(queries[i : i + 50], signatures[i : i + 50]) # batch size 50


class CacheBasedAllocator:
    def __init__(self, n_workers=N_WORKERS, device=DEVICE):
        self.n_workers = n_workers
        self.device = device
        self.query_model = None

        self.w_a = W_A
        self.w_a_step = W_A_STEP

        self.centers = []
        self.query_counts = [0 for _ in range(n_workers)]
        self.accumulated_costs = [1e-8 for _ in range(n_workers)]
        for _ in range(n_workers):
            self.centers.append(torch.zeros((N_TABLES + N_INDEXES) * N_BUCKETS, device=self.device))
        
        self.allocation_lock = Lock()

    def update_models(self, trainer):
        self.query_model = trainer.get_checkpoint()
        self.query_model.eval().to(self.device)
    
    def update_centers(self, queries):
        self.query_counts = [0 for _ in range(self.n_workers)]
        self.accumulated_costs = [1e-8 for _ in range(self.n_workers)]
        for i, query in enumerate(queries):
            self.centers[i] = self.query_model.encode_and_predict([query]).view(-1)
    
    def update_alloc_weight(self, is_increase):
        if is_increase:
            self.w_a += self.w_a_step
        else:
            self.w_a -= self.w_a_step
            self.w_a = max(self.w_a, 0)
    
    def cluster_and_sort(self, queries, predicted_caches, predicted_costs, write_query_idxs):
        ''' *** This function is deprecated *** '''
        # 1. kmeans clustering with balancing requirements
        cost_upperbound = sum(predicted_costs) * (1.5 / self.n_workers) # allow 50% of over-allocated queries
        # write queries can only be executed at master
        allocated_indexes = [write_query_idxs] + [[] for _ in range(self.n_workers - 1)]
        allocated_costs = [sum([predicted_costs[i] for i in idxs]) for idxs in allocated_indexes]
        # perform clustering
        cur_centers, max_iter, eps = self.centers, 1000, 1e-5
        indexes_to_cluster = [i for i, q in enumerate(queries) if not q.is_write]
        for _ in range(max_iter):
            cur_idxs, cur_costs = [[] for _ in range(self.n_workers)], [0 for _ in range(self.n_workers)]
            dists = [torch.norm(predicted_caches - center, p=1, dim=1) for center in cur_centers]   # (n_workers, n_queries)
            dists = torch.stack(dists)                                                              # (n_workers, n_queries)
            min_dist, min_wid = torch.min(dists, dim=0)                                             # (n_queries)
            # modified kmeans algorithm
            dist_idx_pairs = [(min_dist[i], min_wid[i], i) for i in indexes_to_cluster]   
            heapq.heapify(dist_idx_pairs)
            while dist_idx_pairs:
                dist, wid, idx = heapq.heappop(dist_idx_pairs)
                if dist.isinf():
                    # the over-allocate threshold requirement cannot be satisfied
                    # in this case, allocate the query to the worker with minimum total costs
                    wid = min(range(self.n_workers), key=lambda i: allocated_costs[i] + cur_costs[i])
                    cur_idxs[wid].append(idx)
                    cur_costs[wid] += predicted_costs[idx]
                elif allocated_costs[wid] + cur_costs[wid] + predicted_costs[idx] <= cost_upperbound:
                    cur_idxs[wid].append(idx)
                    cur_costs[wid] += predicted_costs[idx]
                else:
                    dists[wid][idx] = float('inf')
                    dist, wid = torch.min(dists[:, idx], dim=0)
                    heapq.heappush(dist_idx_pairs, (dist, wid, idx))
            # calculate new centers
            new_centers = []
            for wid in range(self.n_workers):
                points = [predicted_caches[i] for i in cur_idxs[wid]]
                if not points:
                    points.append(torch.zeros((N_TABLES + N_INDEXES) * N_BUCKETS, device=self.device))
                new_centers.append(torch.mean(torch.stack(points), dim=0))
            diff = sum([torch.norm(cur_centers[wid] - new_centers[wid], p=1) for wid in range(self.n_workers)])
            cur_centers = new_centers
            if diff < eps:
                break
        # 2. sort the queries based on distance to each center, and send them
        for wid in range(self.n_workers):
            allocated_indexes[wid] += cur_idxs[wid]
            sort_key = lambda i: torch.norm(predicted_caches[i] - cur_centers[wid], p=1)
            allocated_indexes[wid] = sorted(allocated_indexes[wid], key=sort_key)
            for idx in allocated_indexes[wid]:
                send_to(wid, queries[idx], predicted_caches[idx])
    
    def greedy_assignment(self, queries, predicted_caches, predicted_costs, write_query_idxs):
        ''' *** This function is deprecated *** '''
        remaining_queries = set(range(len(queries)))
        centers, acc_costs = self.centers, [1e-8 for _ in range(self.n_workers)]
        dists = [torch.norm(predicted_caches - center, p=1, dim=1) for center in centers]       # (n_workers, n_queries)
        dists = [dist / N_BUCKETS / (N_TABLES + N_INDEXES) for dist in dists] # normalize       # (n_workers, n_queries)
        costs = [predicted_cost / max(predicted_costs) for predicted_cost in predicted_costs]   # (n_queries)
        weighted_costs = torch.Tensor(costs).to(self.device) * COST_WEIGHT                      # (n_queries)
        while remaining_queries:
            penalties = [acc_cost / sum(acc_costs) for acc_cost in acc_costs]                   # (n_workers)
            scores_list = [
                dist + weighted_costs + penalty * PENALTY_WEIGHT
                for dist, penalty in zip(dists, penalties)
            ]                                                                                   # (n_workers, n_queries)
            # revise scores for write queries, as they can only be sent to master
            for idx in write_query_idxs:
                # by default, master is the first worker
                for wid in range(1, self.n_workers):
                    scores_list[wid][idx] = 1e8
            # greedily choose the best query to allocate
            min_score, min_wid = torch.min(torch.stack(scores_list), dim=0)                     # (n_queries)
            best_query_idx = min(remaining_queries, key=lambda x: min_score[x])
            # allocate {best_query_idx} to the corresponding worker
            send_to(min_wid[best_query_idx].item(), queries[best_query_idx], predicted_caches[best_query_idx])
            remaining_queries.remove(best_query_idx)
            # update centers, accumulated costs, and distances
            widx = min_wid[best_query_idx].item()
            centers[widx] = predicted_caches[best_query_idx]
            acc_costs[widx] += predicted_costs[best_query_idx]
            dists[widx] = torch.norm(predicted_caches - centers[widx], p=1, dim=1)
            dists[widx] = dists[widx] / N_BUCKETS / (N_TABLES + N_INDEXES)
    
    def adaptive_greedy_allocation(self, queries, predicted_caches, predicted_costs, write_query_idxs):
        for idx in range(len(queries)):
            # write queries can only be executed at master
            if idx in write_query_idxs:
                send_to(0, queries[idx], predicted_caches[idx]) # master is worker 0 by default
                self.query_counts[0] += 1
                self.accumulated_costs[0] += predicted_costs[idx]
                self.centers[0] = (self.centers[0] * (self.query_counts[0] - 1) + predicted_caches[idx]) / self.query_counts[0]
            else:
                distances = [torch.norm(predicted_caches[idx] - center, p=1) / N_BUCKETS / (N_TABLES + N_INDEXES) for center in self.centers]
                load_factors = [self.accumulated_costs[i] / sum(self.accumulated_costs) for i in range(self.n_workers)]
                wid = min(range(self.n_workers), key=lambda i: distances[i] + self.w_a * load_factors[i])
                send_to(wid, queries[idx], predicted_caches[idx])
                self.query_counts[wid] += 1
                self.accumulated_costs[wid] += predicted_costs[idx]
                self.centers[wid] = (self.centers[wid] * (self.query_counts[wid] - 1) + predicted_caches[idx]) / self.query_counts[wid]
    
    def allocate(self, queries):
        if not queries:
            return
        with self.allocation_lock:
            predicted_caches = self.query_model.encode_and_predict(queries)
            predicted_caches = predicted_caches.view(len(queries), -1)
            predicted_costs = [q.cost for q in queries]
            write_query_idxs = [i for i, q in enumerate(queries) if q.is_write]
            # Allocate the queries based on model prediction
            self.adaptive_greedy_allocation(queries, predicted_caches, predicted_costs, write_query_idxs)
            # Below are deprecated allocation algorithms
            # self.greedy_assignment(queries, predicted_caches, predicted_costs, write_query_idxs)
            # self.cluster_and_sort(queries, predicted_caches, predicted_costs, write_query_idxs)
    
    def re_allocate(self, queries, center_queries):
        if not queries:
            return
        with self.allocation_lock:
            self.update_centers(center_queries)
            predicted_caches = self.query_model.encode_and_predict(queries)
            predicted_caches = predicted_caches.view(len(queries), -1)
            predicted_costs = [q.cost for q in queries]
            write_query_idxs = [i for i, q in enumerate(queries) if q.is_write]
            ''' cost-bounded kmeans clustering '''
            cost_upperbound = sum(predicted_costs) * (1.5 / self.n_workers) # roughly allow 50% of over-allocated queries
            # write queries can only be executed at master, so mark them as "allocated"
            allocated_indexes = [write_query_idxs] + [[] for _ in range(self.n_workers - 1)]
            allocated_costs = [sum([predicted_costs[i] for i in idxs]) for idxs in allocated_indexes]
            # perform clustering
            cur_centers, max_iter, eps = self.centers, 200, 1e-5
            indexes_to_cluster = [i for i, q in enumerate(queries) if not q.is_write]
            for _ in range(max_iter):
                cur_idxs, cur_costs = [[] for _ in range(self.n_workers)], [0 for _ in range(self.n_workers)] # current cluster
                dists = [torch.norm(predicted_caches - center, p=1, dim=1) for center in cur_centers]   # (n_workers, n_queries)
                dists = torch.stack(dists)                                                              # (n_workers, n_queries)
                min_dist, min_wid = torch.min(dists, dim=0)                                             # (n_queries)
                # modified kmeans algorithm
                dist_idx_pairs = [(min_dist[i], min_wid[i], i) for i in indexes_to_cluster]   
                heapq.heapify(dist_idx_pairs)
                while dist_idx_pairs:
                    dist, wid, idx = heapq.heappop(dist_idx_pairs)
                    if dist.isinf():
                        # the over-allocate threshold requirement cannot be satisfied
                        # in this case, allocate the query to the worker with minimum total costs
                        wid = min(range(self.n_workers), key=lambda i: allocated_costs[i] + cur_costs[i])
                        cur_idxs[wid].append(idx)
                        cur_costs[wid] += predicted_costs[idx]
                    elif allocated_costs[wid] + cur_costs[wid] + predicted_costs[idx] <= cost_upperbound:
                        cur_idxs[wid].append(idx)
                        cur_costs[wid] += predicted_costs[idx]
                    else:
                        dists[wid][idx] = float('inf')
                        dist, wid = torch.min(dists[:, idx], dim=0)
                        heapq.heappush(dist_idx_pairs, (dist, wid, idx))
                # calculate the new centers
                new_centers = []
                for wid in range(self.n_workers):
                    points = [predicted_caches[i] for i in allocated_indexes[wid] + cur_idxs[wid]]
                    if not points:
                        points.append(torch.zeros((N_TABLES + N_INDEXES) * N_BUCKETS, device=self.device))
                    new_centers.append(torch.mean(torch.stack(points), dim=0))
                diff = sum([torch.norm(cur_centers[wid] - new_centers[wid], p=1) for wid in range(self.n_workers)])
                cur_centers = new_centers
                if diff < eps:
                    break
            # send the queries and update information
            self.centers = cur_centers
            self.accumulated_costs = [allocated_costs[i] + cur_costs[i] for i in range(self.n_workers)]
            for wid in range(self.n_workers):
                allocated_indexes[wid] += cur_idxs[wid]
                self.query_counts[wid] = len(allocated_indexes[wid])
                for idx in allocated_indexes[wid]:
                    send_to(wid, queries[idx], predicted_caches[idx])
