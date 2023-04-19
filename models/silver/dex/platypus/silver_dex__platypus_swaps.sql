{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH swaps_base AS (

    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS sender_address,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS to_address,
        CONCAT('0x', SUBSTR(l_segmented_data [0] :: STRING, 25, 40)) AS fromToken,
        CONCAT('0x', SUBSTR(l_segmented_data [1] :: STRING, 25, 40)) AS toToken,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [2] :: STRING
            )
        ) AS fromAmount,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [3] :: STRING
            )
        ) AS toAmount,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        contract_address = '0x66357dcace80431aee0a7507e2e361b7e2402370'
        AND topics [0] :: STRING = '0x54787c404bb33c88e86f4baf88183a3b0141d0a848e6a9f7a13b66ae3a9b73d1'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 1
    FROM
        {{ this }}
)
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
    sender_address AS sender,
    to_address AS tx_to,
    fromToken AS token_in,
    toToken AS token_out,
    fromAmount AS amount_in,
    toAmount AS amount_out,
    'Swap' AS event_name,
    'platypus' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
