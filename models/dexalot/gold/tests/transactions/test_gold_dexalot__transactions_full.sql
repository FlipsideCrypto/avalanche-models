{{ config (
    materialized = 'view',
    tags = ['full_dexalot_test']
) }}

SELECT
    *
FROM
    {{ ref('dexalot__fact_transactions') }}
