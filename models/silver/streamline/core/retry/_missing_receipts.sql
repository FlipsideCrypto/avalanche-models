{{ config (
    materialized = "ephemeral"
) }}

SELECT
    DISTINCT COALESCE(
        tx.block_number,
        r.block_number
    ) AS block_number
FROM
    {{ ref("silver__transactions") }}
    tx full
    OUTER JOIN {{ ref("silver__receipts") }}
    r
    ON tx.block_number = r.block_number
    AND tx.tx_hash = r.tx_hash
    AND tr.block_timestamp >= DATEADD(
        'day',
        -2,
        CURRENT_DATE
    )
WHERE
    tx.block_timestamp >= DATEADD(
        'day',
        -2,
        CURRENT_DATE
    )
    AND (
        r.tx_hash IS NULL
        OR tx.tx_hash IS NULL
    )
