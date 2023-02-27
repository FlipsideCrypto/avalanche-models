{{ config(
    materialized = 'incremental',
    unique_key = "pool",
) }}

SELECT 
  event_inputs:pair ::STRING AS pool,
  event_inputs:token0 ::STRING AS token0,
  event_inputs:token1 ::STRING AS token1
FROM {{ ref('silver__logs') }}
WHERE event_name = 'PairCreated'