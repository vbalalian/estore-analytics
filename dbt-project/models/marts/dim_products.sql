

with product_attributes as (
    select
        product_id,
        brand,
        category_id,
        count(*) as frequency
    from {{ ref('stg_events') }}
    group by product_id, brand, category_id
),

ranked as (
    select *,
           row_number() over (partition by product_id order by frequency desc) as rank
    from product_attributes
),

final as (

select
    p.product_id,
    p.brand,
    c.category_code,
    c.category_lvl_1,
    c.category_lvl_2,
    c.category_lvl_3,
    c.category_lvl_4
from ranked p
left join {{ ref('dim_categories') }} c
  on p.category_id = c.raw_category_id
where rank = 1

)

select * from final
