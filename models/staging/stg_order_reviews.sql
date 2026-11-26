select
    review_id,
    order_id,
    cast(review_score as int)                   as review_score,
    review_comment_message                      as comment,
    cast(review_creation_date    as timestamp)  as reviewed_at,
    cast(review_answer_timestamp as timestamp)  as answered_at
from {{ source('olist', 'order_reviews') }}
