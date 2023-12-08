{{ config(materialized="table") }}


with
    payments as (
        select order_id, amount from {{ ref("stg_payments") }}
    ),

    orders as (select order_id , customer_id from {{ ref("stg_orders") }}),

    fct_orders as (
        select payments.order_id, orders.customer_id, amount
        from payments
        inner join orders using (order_id)
    )
select *
from fct_orders
