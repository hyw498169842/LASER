-- vim: set ft=sql:
-- EXPLAIN (FORMAT JSON)
select
    c_last,
    c_id o_id,
    o_entry_d,
    o_ol_cnt,
    sum(ol_amount)
from
    customer, orders, order_line
where
    c_id = o_c_id
    and c_w_id = o_w_id
    and c_d_id = o_d_id
    and ol_w_id = o_w_id
    and ol_d_id = o_d_id
    and ol_o_id = o_id
    and o_entry_d >= '1992-01-02 23:58:35.929386'
    and ol_delivery_d >= '1992-01-02 23:58:35.929386'
group by
    o_id,
    o_w_id,
    o_d_id,
    c_id,
    c_last,
    o_entry_d,
    o_ol_cnt
having
    sum(ol_amount) > 110000
order by
    sum(ol_amount) desc,
    o_entry_d;
