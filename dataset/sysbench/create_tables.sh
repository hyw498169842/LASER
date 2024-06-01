sysbench \
--db-driver=pgsql \
--oltp-table-size=5000000 \
--oltp-tables-count=10 \
--threads=24 \
--pgsql-host=127.0.0.1 \
--pgsql-port=13000 \
--pgsql-user=user \
--pgsql-password=password \
--pgsql-db=sbtest \
~/.local/share/sysbench/tests/include/oltp_legacy/parallel_prepare.lua \
run
