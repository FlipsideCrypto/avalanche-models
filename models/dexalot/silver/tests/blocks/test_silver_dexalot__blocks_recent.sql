{{ config (
    materialized = 'view',
    tags = ['recent_test']
) }}

SELECT
    *
FROM
    {{ ref('silver_dexalot__blocks') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref("_dexalot_block_lookback") }}
    )
