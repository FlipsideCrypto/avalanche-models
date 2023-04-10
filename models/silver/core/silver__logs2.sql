{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = "block_timestamp::date, _inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH base AS (

    SELECT
        block_number,
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        status AS tx_status,
        logs,
        _inserted_timestamp
    FROM
        {{ ref('silver__receipts') }}
    WHERE
        ARRAY_SIZE(logs) > 0

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(_INSERTED_TIMESTAMP) _INSERTED_TIMESTAMP
    FROM
        {{ this }}
)
{% endif %}
),
flat_logs AS (
    SELECT
        block_number,
        tx_hash,
        origin_from_address,
        origin_to_address,
        tx_status,
        VALUE :address :: STRING AS address,
        VALUE :blockHash :: STRING AS block_hash,
        VALUE :data :: STRING AS DATA,
        PUBLIC.udf_hex_to_int(
            VALUE :logIndex :: STRING
        ) :: INT AS event_index,
        VALUE :removed :: BOOLEAN AS event_removed,
        VALUE :topics AS topics,
        _inserted_timestamp
    FROM
        base,
        LATERAL FLATTEN(
            input => logs
        )
),
new_records AS (
    SELECT
        l.block_number,
        txs.block_timestamp,
        l.tx_hash,
        l.origin_from_address,
        l.origin_to_address,
        txs.origin_function_signature,
        l.tx_status,
        l.address,
        l.block_hash,
        l.data,
        l.event_index,
        l.event_removed,
        l.topics,
        l._inserted_timestamp,
        CASE
            WHEN txs.block_timestamp IS NULL
            OR txs.origin_function_signature IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id
    FROM
        flat_logs l
        LEFT OUTER JOIN {{ ref('silver__transactions2') }}
        txs USING (
            block_number,
            tx_hash
        )
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        txs.block_timestamp,
        t.tx_hash,
        t.origin_from_address,
        t.origin_to_address,
        txs.origin_function_signature,
        t.tx_status,
        t.address,
        t.block_hash,
        t.data,
        t.event_index,
        t.event_removed,
        t.topics,
        GREATEST(
            t._inserted_timestamp,
            txs._inserted_timestamp
        ) AS _inserted_timestamp,
        _log_id,
        FALSE AS is_pending
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__transactions2') }}
        txs USING (
            block_number,
            tx_hash
        )
    WHERE
        t.is_pending
)
{% endif %}
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_status,
    address,
    block_hash,
    DATA,
    event_index,
    event_removed,
    topics,
    _inserted_timestamp,
    _log_id,
    is_pending
FROM
    new_records qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1

{% if is_incremental() %}
UNION
SELECT
    *
FROM
    missing_data
{% endif %}
