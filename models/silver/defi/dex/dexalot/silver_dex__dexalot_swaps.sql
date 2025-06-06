{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH base_swaps AS (

    SELECT
        block_number,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        topic_1 AS nonceAndMeta,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS taker,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS destTrader,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [2] :: STRING)) AS destChainId,
        CASE
            WHEN CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) = '0x0000000000000000000000000000000000000000' THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
            ELSE CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40))
        END AS srcAsset,
        CASE
            WHEN CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) = '0x0000000000000000000000000000000000000000' THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
            ELSE CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40))
        END AS destAsset,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [5] :: STRING)) AS srcAmount,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [6] :: STRING)) AS destAmount,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0xeed3c159f3a96ab8d41c8b9ca49ee1e5071a7cdd'
        AND topic_0 IN (
            '0x68eb6d948c037c94e470f9a5b288dd93debbcd9342635408e66cb0211686f7f7',
            '0xfeb087be954e9eb692f863466081925668f8f5214f5c1d1a28438df811cbf042'
        )
        AND destChainId = 43114
        AND tx_succeeded
        AND event_removed = FALSE

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    'SwapExecuted' AS event_name,
    srcAmount AS amount_in_unadj,
    destAmount AS amount_out_unadj,
    srcAsset AS token_in,
    destAsset AS token_out,
    origin_from_address AS sender,
    taker AS recipient,
    destTrader AS tx_to,
    event_index,
    'dexalot' AS platform,
    _log_id,
    modified_timestamp
FROM
    base_swaps
