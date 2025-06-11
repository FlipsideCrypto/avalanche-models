{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH base_contracts AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        to_address AS contract_address,
        POSITION(
            '00000000000000000000000000000000000000000000000000000000000000e0',
            input,
            LENGTH(input) - 703
        ) AS argument_start,
        -- starting position of arguments
        SUBSTR(input, argument_start, LENGTH(input) - argument_start + 1) AS arguments,
        regexp_SUBSTR_all(SUBSTR(arguments, 0, len(arguments)), '.{64}') AS segmented_arguments,
        ARRAY_SIZE(segmented_arguments) AS data_size,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_arguments [2] :: STRING,
                25,
                40
            )
        ) AS token_address,
        utils.udf_hex_to_int(
            segmented_arguments [data_size-8] :: STRING
        ) AS decimals,
        utils.udf_hex_to_int(
            segmented_arguments [data_size-7] :: STRING
        ) AS shared_decimals,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_arguments [data_size-6] :: STRING,
                25,
                40
            )
        ) AS endpoint,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_arguments [data_size-5] :: STRING,
                25,
                40
            )
        ) AS owner,
        utils.udf_hex_to_string(
            segmented_arguments [data_size-3] :: STRING
        ) AS token_name,
        utils.udf_hex_to_string(
            segmented_arguments [data_size-1] :: STRING
        ) AS token_symbol,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        origin_function_signature = '0x61014060'
        AND from_address = '0x4a79adc4539905376d339c69b6a7092d0598cc24'
        AND trace_succeeded

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
    contract_address as pool_address,
    token_address,
    decimals,
    shared_decimals,
    endpoint,
    owner,
    token_name,
    token_symbol,
    _inserted_timestamp
FROM
    base_contracts
