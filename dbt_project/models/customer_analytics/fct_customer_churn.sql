{{
    config(
        schema='customer_analytics',
        materialized='incremental',
        unique_key='customerid'
    )
}}

select
    coalesce(customerid, 0)                                     as customerid,
    coalesce(age, 0)                                            as age,
    scrub_pii(coalesce(gender, 'Other'))                        as gender,
    coalesce(tenure, 0)                                         as tenure,
    coalesce(monthlycharges, 0)                                 as monthlycharges,
    scrub_pii(coalesce(contracttype, 'None'))                   as contracttype,
    scrub_pii(coalesce(internetservice, 'No Service'))          as internetservice,
    coalesce(totalcharges, 0)                                   as totalcharges,
    scrub_pii(coalesce(techsupport, 'Unknown'))                 as techsupport,
    scrub_pii(coalesce(churn, 'Unknown'))                       as churn
from {{ source('staging', 'customer_churn_data') }}

{% if is_incremental() %}
where customerid not in (select customerid from {{ this }})
{% endif %}
