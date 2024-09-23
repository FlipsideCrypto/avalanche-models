-- depends_on: {{ ref('bronze_dexalot__streamline_receipts_by_hash') }}
{{ config (
    materialized = "incremental",
    unique_key = "complete_receipts_by_hash_id",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(complete_receipts_by_hash_id)",
    tags = ['stale']
) }}

SELECT
    VALUE :BLOCK_NUMBER :: INT AS block_number,
    VALUE: "TX_HASH" :: STRING AS tx_hash,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'tx_hash']
    ) }} AS complete_receipts_by_hash_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze_dexalot__streamline_receipts_by_hash') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) _inserted_timestamp
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze_dexalot__streamline_fr_receipts_by_hash') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY complete_receipts_by_hash_id
        ORDER BY
            _inserted_timestamp DESC)) = 1
