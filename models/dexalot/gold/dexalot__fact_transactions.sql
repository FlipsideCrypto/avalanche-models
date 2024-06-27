{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    incremental_strategy = 'delete+insert',
    cluster_by = "block_timestamp::date",
    tags = ['dexalot_incremental']
) }}

SELECT
    block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    VALUE,
    value_precise_raw,
    value_precise,
    tx_fee,
    tx_fee_precise,
    gas_price,
    effective_gas_price,
    gas AS gas_limit,
    gas_used,
    cumulative_gas_used,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    input_data,
    tx_status AS tx_succeeded,
    r,
    s,
    v,
    transactions_id AS fact_transactions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_dexalot__transactions') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
