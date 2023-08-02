{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    avax_value,
    IFNULL(
        utils.udf_hex_to_int(
            DATA :value :: STRING
        ),
        '0'
    ) AS avax_value_precise_raw,
    utils.udf_decimal_adjust(
        avax_value_precise_raw,
        18
    ) AS avax_value_precise,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    identifier,
    DATA,
    tx_status,
    sub_traces,
    trace_status,
    error_reason,
    trace_index
FROM
    {{ ref('silver__traces') }}
