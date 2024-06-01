from config import ALL_TABLES, ALL_INDEXES, N_BUCKETS
from utils import *

class TrainData:
    def __init__(self, query, new_pages_dict, finish_time):
        self.query = query
        self.finish_time = finish_time
        # compute cache state changes
        self.cache_states = []
        for relation in ALL_TABLES + ALL_INDEXES:
            if relation not in new_pages_dict.keys():
                self.cache_states.append([0] * N_BUCKETS)
            else:
                total_pages = get_total_pages(relation)
                buckets = divide_pages_into_buckets(total_pages, new_pages_dict[relation])
                self.cache_states.append(buckets)
