{{ config (
    materialized = 'view',
    tags = ['dexalot','full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver_dexalot__transactions') }}
