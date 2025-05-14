{{ config (
    materialized = 'view',
    tags = ['dexalot','recent_test']
) }}

SELECT
    *
FROM
    {{ ref('dexalot__fact_blocks') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref("_dexalot_block_lookback") }}
    )
