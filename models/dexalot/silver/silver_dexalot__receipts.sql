-- depends_on: {{ ref('bronze_dexalot__streamline_receipts') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_hash, tx_hash, from_address, to_address)",
    tags = ['dexalot','dexalot_main']
) }}

WITH base AS (

    SELECT
        COALESCE(
            VALUE :BLOCK_NUMBER :: INT,
            metadata :request :"data" :id :: INT,
            PARSE_JSON(
                metadata :request :"data"
            ) :id :: INT
        ) AS block_number,
        DATA,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze_dexalot__streamline_receipts') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND IS_OBJECT(DATA)
{% else %}
    {{ ref('bronze_dexalot__streamline_fr_receipts') }}
WHERE
    IS_OBJECT(DATA)
{% endif %}
),
FINAL AS (
    SELECT
        DATA,
        block_number,
        DATA :blockHash :: STRING AS block_hash,
        utils.udf_hex_to_int(
            DATA :blockNumber :: STRING
        ) :: INT AS blockNumber,
        DATA :contractAddress :: STRING AS contractAddress,
        utils.udf_hex_to_int(
            DATA :cumulativeGasUsed :: STRING
        ) :: INT AS cumulative_gas_used,
        utils.udf_hex_to_int(
            DATA :effectiveGasPrice :: STRING
        ) :: INT / pow(
            10,
            9
        ) AS effective_gas_price,
        DATA :from :: STRING AS from_address,
        COALESCE(
            utils.udf_hex_to_int(
                DATA :gasUsed :: STRING
            ) :: INT,
            0
        ) AS gas_used,
        DATA :logs AS logs,
        DATA :logsBloom :: STRING AS logs_bloom,
        utils.udf_hex_to_int(
            DATA :status :: STRING
        ) :: INT AS status,
        CASE
            WHEN status = 1 THEN TRUE
            ELSE FALSE
        END AS tx_status,
        DATA :to :: STRING AS to_address1,
        CASE
            WHEN to_address1 = '' THEN NULL
            ELSE to_address1
        END AS to_address,
        DATA :transactionHash :: STRING AS tx_hash,
        CASE
            WHEN block_number <> blockNumber THEN NULL
            ELSE utils.udf_hex_to_int(
                DATA :transactionIndex :: STRING
            ) :: INT
        END AS POSITION,
        utils.udf_hex_to_int(
            DATA :type :: STRING
        ) :: INT AS TYPE,
        _inserted_timestamp
    FROM
        base
)
SELECT
    *
FROM
    FINAL
WHERE
    tx_hash IS NOT NULL
    AND POSITION IS NOT NULL qualify(ROW_NUMBER() over (PARTITION BY block_number, POSITION
ORDER BY
    _inserted_timestamp DESC)) = 1
