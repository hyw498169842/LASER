-- vim: set ft=sql:
-- EXPLAIN (FORMAT JSON)
select
    ol_o_id,
    ol_w_id,
    ol_d_id,
    sum(ol_amount) as revenue,
    o_entry_d
from
    customer,
    new_orders,
    orders,
    order_line
where c_state like 'A%'
    and c_id = o_c_id
    and c_w_id = o_w_id
    and c_d_id = o_d_id
    and no_w_id = o_w_id
    and no_d_id = o_d_id
    and no_o_id = o_id
    and ol_w_id = o_w_id
    and ol_d_id = o_d_id
    and ol_o_id = o_id
    and o_entry_d > '1995-03-08 23:58:35.929386'
-- do NOT filter on ol_delivery_d as we want the ones that have NOT been delivered yet :)
group by
    ol_o_id,
    ol_w_id,
    ol_d_id,
    o_entry_d
order by
    revenue desc,
    o_entry_d
limit 1000;

