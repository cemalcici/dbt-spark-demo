-- İş sorusu: Gecikme memnuniyeti düşürüyor mu?
{#- Gruplar tek yerde: yeni eşik eklemek = listeye bir satır eklemek -#}
{%- set gecikme_gruplari = [
    (0, '1. erken/zamanında'),
    (3, '2. 1-3 gün'),
    (7, '3. 4-7 gün'),
] %}
with delays as (
    select * from {{ ref('int_order_delays') }}
),

-- Bazı siparişlerin birden fazla yorumu var: sipariş başına ortalama puan
reviews as (
    select order_id, avg(review_score) as review_score
    from {{ ref('stg_order_reviews') }}
    group by order_id
),

joined as (
    select
        case
        {%- for ust_sinir, grup in gecikme_gruplari %}
            when d.delay_days <= {{ ust_sinir }} then '{{ grup }}'
        {%- endfor %}
            else '4. 7+ gün'
        end as delay_bucket,
        r.review_score
    from delays d
    join reviews r on d.order_id = r.order_id
)

select
    delay_bucket,
    count(*)                                                   as order_count,
    round(avg(review_score), 2)                                as avg_review_score,
    round(avg(case when review_score <= 2 then 1.0 else 0.0 end) * 100, 1) as bad_review_pct
from joined
group by delay_bucket
order by delay_bucket
