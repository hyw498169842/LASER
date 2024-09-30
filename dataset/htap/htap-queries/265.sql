-- vim: set ft=sql:
-- EXPLAIN (FORMAT JSON)
select
    ol_number,
    sum(ol_quantity) as sum_qty,
    sum(ol_amount) as sum_amount,
    avg(ol_quantity) as avg_qty,
    avg(ol_amount) as avg_amount,
    count(*) as count_order
from
    order_line
where
    ol_delivery_d > '1998-09-23 23:58:35.929386'
group by
    ol_number
order by
    ol_number;

