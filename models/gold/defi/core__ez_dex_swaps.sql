{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'TRADER JOE, WOOFI, GMX, KYBERSWAP, FRAX, HASHFLOW, SUSHI, CURVE',
                'PURPOSE': 'DEX, SWAPS'
            }
        }
    }
) }}

SELECT *
FROM {{ ref('defi__ez_dex_swaps') }}