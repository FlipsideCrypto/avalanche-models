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
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS from_token,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS to_token,
        CONCAT('0x', SUBSTR(l.topics [3] :: STRING, 27, 40)) AS to_address,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS swapType,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS fromAmount,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [2] :: STRING
            )
        ) AS toAmount,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [3] :: STRING,
                25,
                40
            )
        ) AS from_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [4] :: STRING,
                25,
                40
            )
        ) AS rebateTo,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        contract_address IN (
            '0x5aa6a4e96a9129562e2fc06660d07feddaaf7854', --v1
            '0xc22fbb3133df781e6c25ea6acebe2d2bb8cea2f9' --v2
        ) -- avax
        AND topics [0] :: STRING = '0x27c98e911efdd224f4002f6cd831c3ad0d2759ee176f9ee8466d95826af22a1c'

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
    from_token AS token_in,
    to_token AS token_out,
    to_address AS tx_to,
    swapType AS swap_type,
    fromAmount AS amount_in_unadj,
    toAmount AS amount_out_unadj,
    from_address AS sender,
    rebateTo AS rebate_to,
    'Swap' AS event_name,
    'woofi' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
