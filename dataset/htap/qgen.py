import os
import random

templates = []
for filename in os.listdir("./test-queries"):
    with open(f"./test-queries/{filename}", "r") as f:
        templates.append(f.read())

queries = []
for i in range(200):
    cur_idx = i % len(templates)
    queries.append(templates[cur_idx])

for filename in os.listdir("./transactions"):
    with open(f"./transactions/{filename}", "r") as f:
        queries.append(f.read())

random.seed(0)
random.shuffle(queries)

for i in range(1000):
    with open(f"htap-queries/{i + 1}.sql", "w") as f:
        f.write(queries[i])
