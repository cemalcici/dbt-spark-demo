-- Sipariş başına teslimat gecikmesi (gün) + sepet tutarı
with orders as (
    select * from {{ ref('stg_orders') }}
    where order_status = 'delivered'
      and delivered_at is not null
),

items as (
    select
        order_id,
        count(*)   as item_count,
        sum(price) as order_amount
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.purchased_at,
    o.estimated_delivery_at,
    o.delivered_at,
    -- pozitif = tahmini tarihten sonra geldi (gecikti)
    datediff(o.delivered_at, o.estimated_delivery_at) as delay_days,
    i.item_count,
    i.order_amount
from orders o
left join items i on o.order_id = i.order_id
