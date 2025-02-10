{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['core','non_realtime','reorg']
) }}

WITH avax_base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        VALUE AS avax_value,
        identifier,
        concat_ws(
            '-',
            block_number,
            tx_position,
            CONCAT(
                TYPE,
                '_',
                trace_address
            )
        ) AS _call_id,
        input,
        modified_timestamp AS _INSERTED_TIMESTAMP,
        value_precise_raw AS avax_value_precise_raw,
        value_precise AS avax_value_precise,
        tx_position,
        trace_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        avax_value > 0
        AND tx_succeeded
        AND trace_succeeded
        AND TYPE NOT IN (
            'DELEGATECALL',
            'STATICCALL'
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
    tx_hash,
    block_number,
    block_timestamp,
    identifier,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    from_address,
    to_address,
    avax_value AS amount,
    avax_value_precise_raw AS amount_precise_raw,
    avax_value_precise AS amount_precise,
    ROUND(
        avax_value * price,
        2
    ) AS amount_usd,
    _call_id,
    a._inserted_timestamp,
    tx_position,
    trace_index,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'trace_index']
    ) }} AS native_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    avax_base A
    LEFT JOIN {{ ref('silver__complete_token_prices') }}
    ON DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = HOUR
    AND token_address = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'