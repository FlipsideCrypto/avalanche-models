{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH router_swaps_base AS (

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
            utils.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS swapType,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS fromAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
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
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    WHERE
        contract_address IN (
            '0x5aa6a4e96a9129562e2fc06660d07feddaaf7854',
            --v1
            '0xc22fbb3133df781e6c25ea6acebe2d2bb8cea2f9',
            '0x4c4af8dbc524681930a27b2f1af5bcc8062e6fb7' --v2
        )
        AND topics [0] :: STRING = '0x27c98e911efdd224f4002f6cd831c3ad0d2759ee176f9ee8466d95826af22a1c' --WooRouterSwap
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'

{% endif %}
),
swaps_base AS (
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
            utils.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS fromAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS toAmount,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [2] :: STRING,
                25,
                40
            )
        ) AS from_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [3] :: STRING,
                25,
                40
            )
        ) AS rebateTo,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    WHERE
        contract_address IN (
            '0xf8ce0d043891b62c55380fb1efbfb4f186153d96',
            '0x1df3009c57a8b143c6246149f00b090bce3b8f88',
            '0x3b3e4b4741e91af52d0e9ad8660573e951c88524',
            '0xed9e3f98bbed560e66b89aac922e29d4596a9642'
        )
        AND topics [0] :: STRING IN (
            '0x74ef34e2ea7c5d9f7b7ed44e97ad44b4303416c3a660c3fb5b3bdb95a1d6abd3',
            '0x0e8e403c2d36126272b08c75823e988381d9dc47f2f0a9a080d95f891d95c469'
        ) --WooSwap
        AND tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                router_swaps_base
        )
        AND tx_succeeded

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
    CASE
        WHEN from_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
        ELSE from_token
    END AS token_in,
    CASE
        WHEN to_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
        ELSE to_token
    END AS token_out,
    to_address AS tx_to,
    swapType AS swap_type,
    fromAmount AS amount_in_unadj,
    toAmount AS amount_out_unadj,
    from_address AS sender,
    rebateTo AS rebate_to,
    'WooRouterSwap' AS event_name,
    'woofi' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    router_swaps_base
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    CASE
        WHEN from_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
        ELSE from_token
    END AS token_in,
    CASE
        WHEN to_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
        ELSE to_token
    END AS token_out,
    to_address AS tx_to,
    NULL AS swap_type,
    fromAmount AS amount_in_unadj,
    toAmount AS amount_out_unadj,
    from_address AS sender,
    rebateTo AS rebate_to,
    'WooSwap' AS event_name,
    'woofi' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
