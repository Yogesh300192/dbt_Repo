with payments as
(
    select * from {{ ref('stg_payments') }}
),
 customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

intermediate as (

    select
        customers.customer_id,
        orders.order_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders
     
    from customers

    left join customer_orders using (customer_id)
    left join orders using (customer_id)
       --on payments.orderid=orders.order_id

)

,
final as(
    select  intermediate.customer_id,
        intermediate.order_id,
        intermediate.first_name,
        intermediate.last_name,
        intermediate.first_order_date,
        intermediate.most_recent_order_date,
        intermediate.number_of_orders,
        sum(payments.amount) as total_amount
  from intermediate
  left join payments using(order_id)
    group by all
)

select * from final
