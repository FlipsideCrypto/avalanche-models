{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    tx_hash,
    block_number
FROM
    {{ ref('silver__transactions') }}
