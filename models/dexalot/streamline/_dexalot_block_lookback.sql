{{ config (
    materialized = "ephemeral"
) }}

SELECT
    COALESCE(MIN(block_number), 21248026) AS block_number
FROM
    {{ ref("silver_dexalot__blocks") }}
WHERE
    block_number >= 21248026 -- min block_number for historical data that exists within the internal node for Traces
    AND block_timestamp >= DATEADD('hour', -72, TRUNCATE(SYSDATE(), 'HOUR'))
    AND block_timestamp < DATEADD('hour', -71, TRUNCATE(SYSDATE(), 'HOUR'))
