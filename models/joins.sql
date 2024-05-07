WITH prod AS (
    SELECT
        ct.category_name,
        sp.company_name as supplier,
        pd.product_name,
        pd.unit_price,
        pd.product_id
    FROM {{source('sources','products')}} pd
    LEFT JOIN {{source('sources','suppliers')}} sp ON
    sp.supplier_id = pd.supplier_id
    LEFT JOIN {{source('sources','categories')}} ct ON
    ct.category_id = pd.category_id
), order_details AS (
    SELECT pd.*, od.order_id, od.quantity, od.discount
    FROM {{ref('order_details')}} od 
    LEFT JOIN prod pd ON
    od.product_id = pd.product_id
), ordrs AS (
    SELECT ord.order_date, ord.order_id, cus.company_name customer, emp.full_name employee, emp.employee_age, emp.length_of_service
    FROM {{source('sources','orders')}} ord
    LEFT JOIN {{ref('customers')}} cus ON
    cus.customer_id = ord.customer_id
    LEFT JOIN {{ref('employees')}} emp ON
    emp.employee_id = ord.employee_id
    LEFT JOIN {{source('sources','shippers')}} ship ON
    ship.shipper_id = ord.ship_via
), final_join AS (
    SELECT od.*, ord.order_date, ord.customer, ord.employee, ord.length_of_service
    FROM order_details od
    INNER JOIN ordrs ord ON
    od.order_id = ord.order_id
)

SELECT * FROM final_join
LIMIT 3000