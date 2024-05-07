with markup as (
    SELECT *,
        first_value(customer_id)
        over(partition by company_name, contact_name
        order by company_name
        rows between unbounded preceding and unbounded following) as result
    FROM {{source('sources','customers')}}
    -- SELECT * FROM {{source('sources','customers')}}
), removed as (
    SELECT DISTINCT result FROM markup
), final as (
    SELECT * FROM {{source('sources','customers')}} WHERE customer_id IN (select result from removed)
)

SELECT * FROM final