-- Mudando view de funcionário com alguns calculos

WITH employees AS (
    SELECT *,
        date_part(year, current_date) - date_part(year, birth_date) as employee_age,
        date_part(year, current_date) - date_part(year, hire_date)  as length_of_service,
        first_name || ' ' || last_name                              as full_name
    FROM {{source('sources','employees')}}
)

SELECT * FROM employees