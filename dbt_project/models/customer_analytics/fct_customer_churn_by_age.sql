{{
    config(
        schema='customer_analytics',
        materialized='view'
    )
}}

select
    age,
    round(avg(tenure)::numeric,0)          as average_tenure,
    round(avg(monthlycharges)::numeric,2)  as average_monthlycharges,
    round(avg(totalcharges)::numeric,2)    as average_totalcharges
from {{ ref('fct_customer_churn') }}
group by age
