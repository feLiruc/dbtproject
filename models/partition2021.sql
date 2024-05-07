SELECT * FROM {{ref('joins')}}
WHERE date_part(year, order_date) = 2021