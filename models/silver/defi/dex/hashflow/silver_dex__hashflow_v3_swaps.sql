{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH pools AS (

    SELECT
        pool_address
    FROM
        {{ ref('silver_dex__hashflow_v3_pools') }}
),
swaps AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        l.event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS trader_address,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS effective_trader_address,
        CONCAT(
            '0x',
            segmented_data [2] :: STRING
        ) AS txid,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS tokenIn,
        CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) AS tokenOut,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [5] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [6] :: STRING
            )
        ) AS amountOut,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN pools p
        ON l.contract_address = p.pool_address
    WHERE
        l.topics [0] :: STRING = '0x34f57786fb01682fb4eec88d340387ef01a168fe345ea5b76f709d4e560c10eb' --Trade
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    effective_trader_address AS sender,
    trader_address AS tx_to,
    txid,
    CASE
        WHEN tokenIn = '0x0000000000000000000000000000000000000000' THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
        ELSE tokenIn
    END AS token_in,
    CASE
        WHEN tokenOut = '0x0000000000000000000000000000000000000000' THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
        ELSE tokenOut
    END AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    'Trade' AS event_name,
    'hashflow-v3' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps
