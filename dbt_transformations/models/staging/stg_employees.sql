WITH src AS (
    SELECT *
    FROM {{ source("raw", "stroe_tracking_export_v2") }}
),

employees AS (
    SELECT
        e.HiredOn AS hired_on,
        e.JobPosition AS job_position,
        e.EmployeeID AS employee_id,
        e.FirstName AS first_name,
        e.PhoneNumber AS phone_number,
        e.Address AS employee_address,
        e.LastName AS last_name,
        CountryCode AS country_code,
        StoreName AS store_name,
        PostCode AS post_code,
        src.Address AS store_address,
        StoreID AS store_id
    FROM src
    CROSS JOIN UNNEST(src.Employees) AS e
)

SELECT *
FROM employees
