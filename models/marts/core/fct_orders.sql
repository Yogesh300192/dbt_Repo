with payments as
(
  select orderid,amount from {{ ref('stg_payments') }}  
),

orders as
(
    select order_id as orderid,customer_id from {{ ref('stg_orders') }}
),
fct_orders as
(
    select payments.orderid,customer_id,amount from payments inner join orders using(orderid)
)
select * from fct_orders