{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    DATA ['result'] ['hash'] :: STRING AS tx_hash,
    block_number
FROM
    {{ source(
        "bronze_streamline",
        "transactions"
    ) }}
    --Point this to Silver Transactions table when it is created
