{{ config (
    materialized = "view",
    tags = ['streamline_dexalot_complete']
) }}

SELECT
    block_number,
    VALUE :: STRING AS tx_hash
FROM
    {{ ref("silver_dexalot__blocks") }},
    LATERAL FLATTEN (
        input => transactions
    )
