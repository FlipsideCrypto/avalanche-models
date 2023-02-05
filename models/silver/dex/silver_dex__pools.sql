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
WHERE label_subtype = 'pool' 