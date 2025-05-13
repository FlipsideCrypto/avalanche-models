{{ config(
    materialized = 'incremental',
    unique_key = "fact_transactions_id",
    incremental_strategy = 'delete+insert',
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['dexalot_main']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    origin_function_signature,
    value,
    value_precise_raw,
    value_precise,
    tx_fee,
    tx_fee_precise,
    tx_status AS tx_succeeded,
    tx_type,
    nonce,
    position as tx_position,
    input_data,
    gas_price,
    effective_gas_price,
    gas AS gas_limit,
    gas_used,
    cumulative_gas_used,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    r,
    s,
    v,
    transactions_id AS fact_transactions_id,
    inserted_timestamp,
    modified_timestamp,
    block_hash, -- deprecate
    position -- deprecate
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
