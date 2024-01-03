{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'celer_cbridge' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_flat :"dstChainId" :: STRING
        ) AS dstChainId,
        TRY_TO_NUMBER(
            decoded_flat :"maxSlippage" :: STRING
        ) AS maxSlippage,
        TRY_TO_NUMBER(
            decoded_flat :"nonce" :: STRING
        ) AS nonce,
        decoded_flat :"receiver" :: STRING AS receiver,
        decoded_flat :"sender" :: STRING AS sender,
        decoded_flat :"token" :: STRING AS token,
        decoded_flat :"transferId" :: STRING AS transferId,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0x89d8051e597ab4178a863a5190407b98abfeff406aa8db90c59af76612e58f01'
        AND contract_address IN (
            '0xbb7684cc5408f4dd0921e5c2cadd547b8f1ad573',
            '0x9b36f165bab9ebe611d491180418d8de4b8f3a1f',
            '0xef3c714c9425a8f3697a9c969dc1af30ba82e5d4'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
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
    topic_0,
    event_name,
    event_removed,
    tx_status,
    contract_address AS bridge_address,
    NAME AS platform,
    sender,
    receiver,
    receiver AS destination_chain_receiver,
    amount,
    dstChainId AS destination_chain_id,
    maxSlippage AS max_slippage,
    nonce,
    token AS token_address,
    transferId AS transfer_id,
    _log_id,
    _inserted_timestamp
FROM
    base_evt
