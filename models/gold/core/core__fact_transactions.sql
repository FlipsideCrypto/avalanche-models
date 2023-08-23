{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    avax_value,
    avax_value_precise_raw,
    avax_value_precise,
    tx_fee,
    tx_fee_precise,
    gas_price,
    gas_limit,
    gas_used,
    cumulative_gas_used,
    input_data,
    status,
    effective_gas_price,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    r,
    s,
    v,
    tx_type,
    chain_id
FROM
    (
        SELECT
            block_number,
            block_timestamp,
            block_hash,
            tx_hash,
            nonce,
            POSITION,
            origin_function_signature,
            from_address,
            to_address,
            VALUE AS avax_value,
            tx_fee,
            tx_fee_precise,
            gas_price,
            gas AS gas_limit,
            gas_used,
            cumulative_gas_used,
            input_data,
            tx_status AS status,
            effective_gas_price,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            r,
            s,
            v,
            tx_type,
            chain_id,
            to_varchar(
                TO_NUMBER(REPLACE(DATA :value :: STRING, '0x'), REPEAT('X', LENGTH(REPLACE(DATA :value :: STRING, '0x'))))
            ) AS avax_value_precise_raw,
            IFF(LENGTH(avax_value_precise_raw) > 18, LEFT(avax_value_precise_raw, LENGTH(avax_value_precise_raw) - 18) || '.' || RIGHT(avax_value_precise_raw, 18), '0.' || LPAD(avax_value_precise_raw, 18, '0')) AS rough_conversion,
            IFF(
                POSITION(
                    '.000000000000000000' IN rough_conversion
                ) > 0,
                LEFT(rough_conversion, LENGTH(rough_conversion) - 19),
                REGEXP_REPLACE(
                    rough_conversion,
                    '0*$',
                    ''
                )
            ) AS avax_value_precise
        FROM
            {{ ref('silver__transactions') }}
    )
