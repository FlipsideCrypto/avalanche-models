{{ config(
    materialized = 'incremental',
    unique_key = "address",
) }}

SELECT
  blockchain,
  creator,
  address,
  address_name,
  project_name
FROM {{ ref('core__dim_labels') }}
WHERE project_name = 'trader joe' 
  AND label_subtype = 'pool' 