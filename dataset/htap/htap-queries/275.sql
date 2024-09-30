-- vim: set ft=sql:
-- EXPLAIN (FORMAT JSON)
select
    sum(ol_amount) as revenue
from
    order_line
where
    ol_delivery_d >= '1993-01-02 23:58:35.929386'
    and ol_delivery_d < '1994-01-02 23:58:35.929386'
    and ol_quantity between 1 and 100000;

