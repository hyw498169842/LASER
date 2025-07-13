### 1. [Set up PostgreSQL master-standby cluster.](https://www.postgresql.org/docs/current/warm-standby.html)

### 2. Load database

Modify the hyper-parameters at the beginning of `create_tables.py` under each dataset folder and then run the python script.

> For TPC-H and TPC-DS datasets, please refer to [tpch-kit](https://github.com/gregrahn/tpch-kit) and [tpcds-kit](https://github.com/gregrahn/tpcds-kit) for more usage guide of `dbgen` and `dsdgen`, which should be run first to generate the `.dat` files used by `create_tables.py`.

> For sysbench dataset, please install [sysbench](https://github.com/akopytov/sysbench) under `~/.local` before running the scripts.

> For HTAP dataset, please refer to [s64da-benchmark-toolkit](https://github.com/swarm64/s64da-benchmark-toolkit) for database creation guide.

### 3. Generate queries
Run `qgen.py` under each dataset folder to generate the queries.

### 4. Experiment
Modify the hyper-parameters regarding database connection in `config.py`, and then run `run.sh` to perform the experiments (usage guides are provided in the script).
