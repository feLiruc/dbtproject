WITH final AS (
    SELECT 
        od.order_id, od.product_id, od.unit_price, od.quantity, pr.product_name, pr.supplier_id, pr.category_id,
        od.unit_price * od.quantity                 AS total,
        (pr.unit_price * od.quantity) - total       AS discount
    FROM {{source('sources','order_details')}} od
    LEFT JOIN {{source('sources','products')}} pr ON
    (od.product_id = pr.product_id)
)

SELECT * FROM final