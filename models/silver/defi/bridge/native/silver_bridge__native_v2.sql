{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH unwrap AS (

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        contract_address,
        contract_address AS bridge_address,
        contract_address AS token_address,
        'Unwrap' AS event_name,
        'avalanche_native_bridge' AS platform,
        'v2' AS version,
        regexp_SUBSTR_all(SUBSTR(DATA, 3), '.{64}') AS part,
        utils.udf_hex_to_int(
            part [0] :: STRING
        ) :: INT AS amount_unadj,
        utils.udf_hex_to_int(
            part [1] :: STRING
        ) :: STRING AS destination_chain_id,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2022-01-01'
        AND topic_0 = '0x37a06799a3500428a773d00284aa706101f5ad94dae9ec37e1c3773aa54c3304'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
transfers AS (
    SELECT
        contract_address,
        tx_hash,
        from_address
    FROM
        {{ ref('core__ez_token_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2022-01-01'
        AND to_address = '0x0000000000000000000000000000000000000000'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY tx_hash,
    contract_address,
    from_address
    ORDER BY
        event_index ASC
) = 1
)
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    bridge_address,
    event_name,
    platform,
    version,
    from_address AS sender,
    from_address AS receiver,
    from_address AS destination_chain_receiver,
    destination_chain_id :: STRING AS destination_chain_id,
    IFF(
        token_address = '0x152b9d0fdc40c096757f570a51e494bd4b943e50',
        -- btc.b
        'bitcoin',
        'ethereum'
    ) AS destination_chain,
    token_address,
    amount_unadj,
    _log_id,
    modified_timestamp,
    inserted_timestamp
FROM
    unwrap
    INNER JOIN transfers USING (
        contract_address,
        tx_hash
    )
