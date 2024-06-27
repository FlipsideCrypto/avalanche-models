{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "fact_event_logs_id",
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['dexalot_non_realtime']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    topics,
    DATA,
    event_removed,
    tx_status AS tx_succeeded,
    _log_id,
    logs_id AS fact_event_logs_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_dexalot__logs') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp) _INSERTED_TIMESTAMP
        FROM
            {{ this }}
    )
{% endif %}
