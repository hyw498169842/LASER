-- vim: set ft=sql:
-- EXPLAIN (FORMAT JSON)
select
    c_id,
    c_last,
    sum(ol_amount) as revenue,
    c_city,
    c_phone,
    n_name
from
    customer,
    orders,
    order_line,
    nation
where
    c_id = o_c_id
    and c_w_id = o_w_id
    and c_d_id = o_d_id
    and ol_w_id = o_w_id
    and ol_d_id = o_d_id
    and ol_o_id = o_id
    and o_entry_d >= '1993-07-02 23:58:35.929386'
    and o_entry_d < '1993-10-02 23:58:35.929386'
    and o_entry_d <= ol_delivery_d
    and n_nationkey = c_nationkey
    and ol_delivery_d >= '1992-01-02 23:58:35.929386'
group by
    c_id,
    c_last,
    c_city,
    c_phone,
    n_name
order by
    revenue desc
limit 1000;

