{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH unwrap AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        contract_address,
        topic_0,
        '0xf5163f69f97b221d50347dd79382f11c6401f1a1' AS bridge_address,
        'core-bitcoin' AS platform,
        event_name,
        event_index,
        origin_from_address AS sender,
        origin_from_address AS receiver,
        origin_from_address AS destination_chain_receiver,
        contract_address AS token_address,
        decoded_log :amount :: INTEGER AS amount,
        decoded_log :chainId :: STRING AS destination_chain_id,
        'bitcoin' AS destination_chain,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address IN ('0x152b9d0fdc40c096757f570a51e494bd4b943e50')
        AND topic_0 = '0x37a06799a3500428a773d00284aa706101f5ad94dae9ec37e1c3773aa54c3304'
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    contract_address,
    token_address,
    topic_0,
    event_name,
    bridge_address,
    platform,
    sender,
    receiver,
    destination_chain_receiver,
    amount AS amount_unadj,
    destination_chain_id,
    destination_chain,
    _log_id,
    modified_timestamp
FROM
    unwrap
