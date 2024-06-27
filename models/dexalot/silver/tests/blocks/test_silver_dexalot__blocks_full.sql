{{ config (
    materialized = 'view',
    tags = ['full_dexalot_test']
) }}

SELECT
    *
FROM
    {{ ref('silver_dexalot__blocks') }}
