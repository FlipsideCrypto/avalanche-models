{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH avax_base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        avax_value,
        identifier,
        _call_id,
        input
    FROM
        {{ ref('silver__traces') }}
    WHERE
        avax_value > 0
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'
),
avax_prices AS (
    SELECT
        HOUR,
        price AS avax_price
    FROM
        {{ ref('silver__prices') }}
    WHERE
        token_address = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
)
SELECT
    A.tx_hash AS tx_hash,
    A.block_number AS block_number,
    A.block_timestamp AS block_timestamp,
    A.identifier AS identifier,
    tx.from_address AS origin_from_address,
    tx.to_address AS origin_to_address,
    tx.origin_function_signature AS origin_function_signature,
    A.from_address AS avax_from_address,
    A.to_address AS avax_to_address,
    A.avax_value AS amount,
    ROUND(
        A.avax_value * avax_price,
        2
    ) AS amount_usd
FROM
    avax_base A
    LEFT JOIN avax_prices
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = HOUR
    JOIN {{ ref('silver__transactions') }}
    tx
    ON A.tx_hash = tx.tx_hash
