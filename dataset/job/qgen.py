import os
import random

templates = []
for filename in os.listdir("./jo-bench/queries"):
    with open(f"./jo-bench/queries/{filename}", "r") as f:
        templates.append(f.read())

queries = []
for i in range(1000):
    cur_idx = i % len(templates)
    queries.append(templates[cur_idx])

random.seed(0)
random.shuffle(queries)

for i in range(1000):
    with open(f"job-queries/{i + 1}.sql", "w") as f:
        f.write(queries[i])
