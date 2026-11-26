-- Tekil (singular) test: dönen her satır bir hatadır.
-- Müşteriye teslim tarihi, satın alma tarihinden önce olamaz.
select order_id, purchased_at, delivered_at
from {{ ref('stg_orders') }}
where delivered_at < purchased_at
