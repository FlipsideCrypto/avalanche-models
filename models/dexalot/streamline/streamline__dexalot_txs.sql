{{ config (
    materialized = "view",
    tags = ['streamline_dexalot_complete','streamline_dexalot_blocks']
) }}

SELECT
    block_number,
    VALUE :: STRING AS tx_hash
FROM
    {{ ref("silver_dexalot__blocks") }},
    LATERAL FLATTEN (
        input => transactions
    )
