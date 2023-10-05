{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['core','non_realtime','reorg'],
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
        input,
        _INSERTED_TIMESTAMP,
        to_varchar(
            TO_NUMBER(REPLACE(DATA :value :: STRING, '0x'), REPEAT('X', LENGTH(REPLACE(DATA :value :: STRING, '0x'))))
        ) AS avax_value_precise_raw,
        IFF(LENGTH(avax_value_precise_raw) > 18, LEFT(avax_value_precise_raw, LENGTH(avax_value_precise_raw) - 18) || '.' || RIGHT(avax_value_precise_raw, 18), '0.' || LPAD(avax_value_precise_raw, 18, '0')) AS rough_conversion,
        IFF(
            POSITION(
                '.000000000000000000' IN rough_conversion
            ) > 0,
            LEFT(rough_conversion, LENGTH(rough_conversion) - 19),
            REGEXP_REPLACE(
                rough_conversion,
                '0*$',
                ''
            )
        ) AS avax_value_precise,
        tx_position,
        trace_index
    FROM
        {{ ref('silver__traces') }}
    WHERE
        avax_value > 0
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '72 hours'
    FROM
        {{ this }}
)
{% endif %}
),
tx_table AS (
    SELECT
        block_number,
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                avax_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '72 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    tx_hash AS tx_hash,
    block_number AS block_number,
    block_timestamp AS block_timestamp,
    identifier AS identifier,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    from_address AS avax_from_address,
    to_address AS avax_to_address,
    avax_value AS amount,
    avax_value_precise_raw AS amount_precise_raw,
    avax_value_precise AS amount_precise,
    ROUND(
        avax_value * price,
        2
    ) AS amount_usd,
    _call_id,
    _inserted_timestamp,
    tx_position,
    trace_index
FROM
    avax_base A
    LEFT JOIN {{ ref('silver__hourly_prices_priority') }}
    ON DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = HOUR
    AND token_address = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
    JOIN tx_table USING (
        tx_hash,
        block_number
    )
