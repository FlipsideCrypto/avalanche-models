{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['non_realtime']
) }}
--     full_refresh = false

SELECT
    block_number,
    tx_hash,
    block_timestamp,
    tx_status AS tx_succeeded,
    tx_position,
    trace_index,
    from_address,
    to_address,
    value_precise_raw,
    value_precise,
    VALUE,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    identifier,
    sub_traces,
    error_reason,
    trace_status AS trace_succeeded,
    DATA,
    traces_id AS fact_traces_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_dexalot__traces') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
