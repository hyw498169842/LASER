import os
import random
from inspect import cleandoc

if not os.getcwd().endswith("dataset/sysbench"):
    print("Please run this script under dataset/sysbench/ folder")
    exit(1)

if not os.path.exists("sysbench-queries"):
    os.mkdir("sysbench-queries")

N_UPDATES = 200
N_SELECT_SINGLE = 400
N_SELECT_MULTI = 400

random.seed(0)

queries = []

# generate update queries
for _ in range(N_UPDATES):
    table = f"sbtest{random.randint(1, 10)}"
    range_size = random.randint(100, 10000)
    range_start = random.randint(1, 5000000 - range_size)
    queries.append(cleandoc(f"""
        UPDATE {table}
        SET k = k + 1
        WHERE id BETWEEN {range_start} AND {range_start + range_size};
    """))

# generate select queries on single table
for _ in range(N_SELECT_SINGLE):
    table = f"sbtest{random.randint(1, 10)}"
    range_size = random.randint(10000, 1000000)
    range_start = random.randint(1, 5000000 - range_size)
    queries.append(cleandoc(f"""
        SELECT *
        FROM {table}
        WHERE id BETWEEN {range_start} AND {range_start + range_size}
        ORDER BY c
        LIMIT 100;
    """))

# generate select queries on multiple tables
for _ in range(N_SELECT_MULTI):
    table1 = f"sbtest{random.randint(1, 10)}"
    table2 = f"sbtest{random.randint(1, 10)}"
    while table1 == table2:
        table2 = f"sbtest{random.randint(1, 10)}"
    queries.append(cleandoc(f"""
        SELECT *
        FROM {table1}, {table2}
        WHERE {table1}.id = {table2}.id
        ORDER BY {table1}.c
        LIMIT 100;
    """))

# write queries to file
random.shuffle(queries)

for i in range(N_UPDATES + N_SELECT_SINGLE + N_SELECT_MULTI):
    with open(f"sysbench-queries/{i + 1}.sql", "w") as f:
        f.write(queries[i])