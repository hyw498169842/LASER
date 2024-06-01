import time
import torch
import heapq
from threading import Lock

from config import *
from shared import *
from utils import extract_tables


def send_to(wid, query, cache_state=None):
    with QUERY_QUEUE_LOCKS[wid]:
        if cache_state is not None:
            QUERY_QUEUES[wid].append((query.qid, query.query_str, query.cost, cache_state.detach().numpy()))
        else:
            QUERY_QUEUES[wid].append((query.qid, query.query_str, None, None))


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
                        for qid, _, _, _ in QUERY_QUEUES[i]:
                            queueing_time[i] += time.time() - self.alloc_time[qid]
                send_to(queueing_time.index(min(queueing_time)), query)
            self.alloc_time[query.qid] = time.time()
    

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
                distances = [torch.norm(predicted_caches[idx] - center, p=1) for center in self.centers]
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
