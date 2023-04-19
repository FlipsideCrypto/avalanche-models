{{ config(
    materialized = 'incremental',
    unique_key = "pool_address"
) }}

    SELECT
        lower(pool_address) as pool_address,
        lower(token0_address) as token0_address,
        pool_name,
        token0_symbol,
        lower(token1_address) as token1_address,
        token1_symbol,
        token0_decimals,
        token1_decimals
    FROM
         {{ source(
            'avalanche_pools',
            'SUSHI_DIM_DEX_POOLS'
        ) }} 