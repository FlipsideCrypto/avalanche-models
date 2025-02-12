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
    {# tx_position, -- new column #}
    event_index,
    contract_address,
    topics,
    topics[0] AS topic_0,
    topics[1] AS topic_1,
    topics[2] AS topic_2,
    topics[3] AS topic_3,
    DATA,
    event_removed,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CASE
        WHEN tx_status = 'SUCCESS' THEN TRUE
        ELSE FALSE
    END AS tx_succeeded,
    logs_id AS fact_event_logs_id,
    inserted_timestamp,
    modified_timestamp,
    tx_status,
    -- deprecate
    _log_id -- deprecate
FROM
    {{ ref('silver_dexalot__logs') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
