import re
import torch
import torch.nn as nn
import torch.nn.functional as F

from utils import one_hot_encoding
from config import *

class ScanEncoder(nn.Module):
    def __init__(self, n_scan_types=N_SCAN_TYPES, n_index_conds=N_INDEX_CONDS, range_encoding_size=N_INDEXES+N_OPERATORS+1, feature_dim=FEATURE_DIM):
        super(ScanEncoder, self).__init__()
        self.linear1 = nn.Linear(n_scan_types + n_index_conds + range_encoding_size, feature_dim)
        self.linear2 = nn.Linear(feature_dim, feature_dim)
    
    def forward(self, scan_type, index_cond, range_encoding):
        # scan_type: one-hot encoding of scan type          (batch_size, n_scan_types)
        # index_cond: one-hot encoding of index condition   (batch_size, n_index_conds)
        # range_encoding: encoding of range predicates      (batch_size, n_range_encodings, range_encoding_size)
        range_encoding = torch.mean(range_encoding, dim=1)              # (batch_size, range_encoding_size)
        x = torch.cat((scan_type, index_cond, range_encoding), dim=1)   # (batch_size, n_scan_types + n_index_conds + range_encoding_size)
        x = F.relu(self.linear1(x))                                     # (batch_size, feature_dim)
        return F.relu(self.linear2(x))                                  # (batch_size, feature_dim)


class CachePredictor(nn.Module):
    def __init__(self, feature_dim=FEATURE_DIM, n_tables=N_TABLES, n_indexes=N_INDEXES, n_buckets=N_BUCKETS):
        super(CachePredictor, self).__init__()
        self.up_linear = nn.Linear(feature_dim, feature_dim * 2)
        # one predictor for each table and each index
        self.table_predictors = nn.ModuleList([nn.Linear(feature_dim * 2, n_buckets) for _ in range(n_tables)])
        self.index_predictors = nn.ModuleList([nn.Linear(feature_dim * 2, n_buckets) for _ in range(n_indexes)])
    
    def forward(self, feature, tables, indexes):
        # feature: encoded scan feature                 (batch_size, feature_dim)
        # tables: list of tables involved in the scan   (batch_size)
        # indexes: list of indexes involved in the scan (batch_size)
        x = F.relu(self.up_linear(feature))                                                     # (batch_size, feature_dim * 2)
        result = [[torch.zeros(N_BUCKETS, device=DEVICE) for _ in range(2)] for f in feature]   # (batch_size, 2, N_BUCKETS)
        for i, (table, index) in enumerate(zip(tables, indexes)):
            if table is not None:
                table_id = ALL_TABLES.index(table)
                result[i][0] = F.sigmoid(self.table_predictors[table_id](x[i]))
            if index is not None:
                index_id = ALL_INDEXES.index(index)
                result[i][1] = F.sigmoid(self.index_predictors[index_id](x[i]))
        return result                                                                           # (batch_size, 2, N_BUCKETS)


class QueryModel(nn.Module):
    def __init__(self, n_scan_types=N_SCAN_TYPES, n_index_conds=N_INDEX_CONDS, feature_dim=FEATURE_DIM, n_tables=N_TABLES, n_indexes=N_INDEXES, n_operators=N_OPERATORS, n_buckets=N_BUCKETS):
        super(QueryModel, self).__init__()
        self.n_tables = n_tables
        self.n_indexes = n_indexes
        self.n_buckets = n_buckets
        self.scan_encoder = ScanEncoder(n_scan_types, n_index_conds, n_indexes + n_operators + 1, feature_dim)
        self.cache_predictor = CachePredictor(feature_dim, n_tables, n_indexes, n_buckets)
    
    def encode_and_predict(self, queries, device=DEVICE):
        table_result = [[torch.zeros(self.n_buckets, device=device) for _ in range(self.n_tables)] for q in queries]
        index_result = [[torch.zeros(self.n_buckets, device=device) for _ in range(self.n_indexes)] for q in queries]
        final_result = []
        for i, q in enumerate(queries):
            # perform batched encoding and prediction for each query
            scan_types, index_conds, range_encodings, indexes, tables = [], [], [], [], []
            for scan_type, index_cond, index, table in q.scans:
                if index_cond is None:
                    index_conds.append(one_hot_encoding([set()], ALL_INDEX_CONDS))
                    range_encodings.append([[0] * (N_INDEXES + N_OPERATORS + 1)])
                else:
                    index_cond_list, range_encoding_list = [], []
                    for expr in re.findall(r"[a-zA-Z0-9_\.\ ]*[><=].[a-zA-Z0-9_\.\ ]*", index_cond):
                        if os.environ["DATASET"] == "job":
                            # job dataset uses table.column instead of column
                            expr = "".join(c for c in expr if not c.isdigit())  # remove numbers in table name
                            index_cond_list.append(set(expr.split(" = ")))      # but do not remove table name
                        elif os.environ["DATASET"] == "sysbench":
                            # this implementation is ugly, may be improved later
                            left, op, right = expr.split(" ")
                            if right.isdigit():
                                range_encoding = one_hot_encoding([index], ALL_INDEXES) + \
                                                 one_hot_encoding([op], ALL_OPERATORS) + \
                                                 [float(right) / 5000000]
                                range_encoding_list.append(range_encoding)
                            else:
                                # add table name for sysbench join conditions
                                index_cond_list.append(set([table + "." + left, right]))
                        else:
                            index_cond_list.append(set(map(lambda s: s.split(".")[-1], expr.split(" = "))))
                    index_conds.append(one_hot_encoding(index_cond_list, ALL_INDEX_CONDS))
                    if not range_encoding_list:
                        range_encodings.append([[0] * (N_INDEXES + N_OPERATORS + 1)])
                    else:
                        range_encodings.append(range_encoding_list)
                scan_types.append(one_hot_encoding([scan_type], ALL_SCAN_TYPES))
                indexes.append(index)
                tables.append(table)
            scan_types = torch.Tensor(scan_types, device=device)
            index_conds = torch.Tensor(index_conds, device=device)
            range_encodings = torch.Tensor(range_encodings, device=device)
            scan_encodings = self.scan_encoder(scan_types, index_conds, range_encodings)
            cache_predictions = self.cache_predictor(scan_encodings, tables, indexes)
            # revise predictions for seq scan and index-only scan
            for (scan_type, _, _, _), cache_prediction in zip(q.scans, cache_predictions):
                # sequential scans only access the full table
                if scan_type == "Seq Scan":
                    cache_prediction[0] = torch.ones(self.n_buckets, requires_grad=True, device=device)
                    cache_prediction[1] = torch.zeros(self.n_buckets, requires_grad=True, device=device)
                # index-only scans never access the table blocks
                elif scan_type == "Index Only Scan":
                    cache_prediction[0] = torch.zeros(self.n_buckets, requires_grad=True, device=device)
            # merge predictions of different scans in the same query plan
            for cache_prediction, table, index in zip(cache_predictions, tables, indexes):
                # use max to merge cache access from different scans (maybe better ways?)
                if table is not None:
                    table_id = ALL_TABLES.index(table)
                    table_result[i][table_id] = torch.maximum(table_result[i][table_id], cache_prediction[0])
                if index is not None:
                    index_id = ALL_INDEXES.index(index)
                    index_result[i][index_id] = torch.maximum(index_result[i][index_id], cache_prediction[1])
            final_result.append(torch.stack(table_result[i] + index_result[i]))
        # (n_queries, n_tables + n_indexes, n_buckets)
        return torch.stack(final_result)
