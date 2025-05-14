{{ config (
    materialized = 'view',
    tags = ['dexalot','recent_test']
) }}

SELECT
    *
FROM
    {{ ref('silver_dexalot__receipts') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref("_dexalot_block_lookback") }}
    )
