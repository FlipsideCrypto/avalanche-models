{{ config (
    materialized = 'view',
    tags = ['dexalot','full_test']
) }}

SELECT
    *
FROM
    {{ ref('dexalot__fact_blocks') }}
