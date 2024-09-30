-- vim: set ft=sql:
-- FIXME: This query produces an empty result as there are
-- no customers who never ordered.
-- EXPLAIN (FORMAT JSON)
select
    c_nationkey,
    count(*) as numcust,
    sum(c_balance) as totacctbal
from
    customer
where
    substring(c_phone from 1 for 2) in ('32', '27', '31', '25', '26', '24', '19')
    and c_balance > (

        select
            avg(c_balance)
        from
            customer
        where
            c_balance > 0.00
            and substring(c_phone from 1 for 2) in ('32', '27', '31', '25', '26', '24', '19')
    )
    and not exists (
        select
            *
        from
            orders
        where
            o_c_id = c_id
            and o_w_id = c_w_id
            and o_d_id = c_d_id
            and o_entry_d >= '1992-01-02 23:58:35.929386'
    )
group by
    c_nationkey
order by
    c_nationkey;
