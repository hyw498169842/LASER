import os
import random
import subprocess

def run_bash(cmd):
    return subprocess.check_output(cmd, shell=True).decode("utf-8")

if not os.getcwd().endswith("dataset/tpcds"):
    print("Please run this script under dataset/tpcds/ folder")
    exit(1)

if not os.path.exists("tpcds-queries"):
    os.mkdir("tpcds-queries")

# ignore queries that take more than 30 min to execute
ignored_qids = {1, 4, 6, 11, 14, 17, 30, 56, 60, 63, 74, 78, 81, 95}
qids = list(set(range(1, 100)) - ignored_qids)

seed = 0
queries = []
for i in range(1000):
    cur_idx = seed % len(qids)
    # write template file
    with open("temp.lst", "w") as f:
        f.write(f"query{qids[cur_idx]}.tpl\n")
    queries.append(run_bash(f"cd tpcds-kit/tools && \
                                ./dsqgen -SCALE 10 \
                                        -INPUT ../../temp.lst \
                                        -DIRECTORY ../query_templates/ \
                                        -DIALECT netezza \
                                        -FILTER Y \
                                        -RNGSEED {seed} \
                                        -QUIET Y"))
    seed += 1

random.seed(0)
random.shuffle(queries)

for i in range(1000):
    with open(f"tpcds-queries/{i + 1}.sql", "w") as f:
        f.write(queries[i])

os.remove("temp.lst")