DATABASE = "job"

# Schema information
ALL_TABLES = [
    "aka_name", "aka_title", "cast_info", "char_name", "comp_cast_type", "company_name", "company_type",
    "complete_cast", "info_type", "keyword", "kind_type", "link_type", "movie_companies", "movie_info",
    "movie_info_idx", "movie_keyword", "movie_link", "name", "person_info", "role_type", "title"
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
    # no self join conditions in the workload
    # below are two-table join conditions
    ["id", "mi_idx.movie_id"], ["id", "mk.keyword_id"], ["id", "mi.movie_id"], ["id", "t.kind_id"],
    ["id", "mc.company_id"], ["id", "mc.company_type_id"], ["id", "cc.status_id"], ["id", "mc.movie_id"],
    ["id", "ci.person_id"], ["id", "mk.movie_id"], ["id", "cc.subject_id"], ["id", "ml.link_type_id"],
    ["id", "ci.person_role_id"], ["id", "ci.movie_id"], ["id", "ci.role_id"], ["id", "pi.person_id"],
    ["id", "cc.movie_id"], ["id", "an.person_id"], ["id", "pi.info_type_id"], ["id", "mi.info_type_id"],
    ["id", "at.movie_id"], ["id", "ml.linked_movie_id"], ["id", "ml.movie_id"], ["id", "mi_idx.info_type_id"]
]
ALL_INDEX_CONDS = [set(cond) for cond in ALL_INDEX_CONDS]
N_INDEX_CONDS = len(ALL_INDEX_CONDS)

# warm up table
WARM_UP_TABLE = "cast_info"

# Input queries
QUERY_DIR = "dataset/job/job-queries"
N_QUERIES = 1000
