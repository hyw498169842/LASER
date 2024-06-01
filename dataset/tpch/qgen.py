import os
import random
import subprocess

def run_bash(cmd):
    return subprocess.check_output(cmd, shell=True).decode("utf-8")

if not os.getcwd().endswith("dataset/tpch"):
    print("Please run this script under dataset/tpch/ folder")
    exit(1)

if not os.path.exists("tpch-queries"):
    os.mkdir("tpch-queries")

# exclude q17, q20 as they are too slow to run
qids = [
     1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
    11, 12, 13, 14, 15, 16, 18, 19, 21, 22
]

seed = 0
queries = []
for qid in qids:
    # generate 50 queries for each qid
    for _ in range(50):
        seed += 1
        queries.append(run_bash(f"export DSS_QUERY=queries && \
                                  cd tpch-kit/dbgen/ && \
                                  ./qgen -v -s 10 -r {seed} {qid}"))

random.seed(0)
random.shuffle(queries)

for i in range(1000):
    with open(f"tpch-queries/{i + 1}.sql", "w") as f:
        f.write(queries[i])
