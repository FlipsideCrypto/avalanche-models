{{ config(
    materialized = 'table',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'SUSHI',
                'PURPOSE': 'DEFI, DEX'
            }
        }
    }
) }}

    SELECT
        lower(kashi_pair_address) as pair_address, 
        pair_name,
        asset_token_symbol as asset_symbol,
        lower(asset_token_address) as asset_address,
        collateral_token_symbol as collateral_symbol,
        lower(collateral_token_address) as collateral_address,
        asset_token_decimals as asset_decimals,
        collateral_token_decimals as collateral_decimals  
    FROM
         {{ source(
            'avalanche_pools',
            'SUSHI_DIM_KASHI_PAIRS'
        ) }} 