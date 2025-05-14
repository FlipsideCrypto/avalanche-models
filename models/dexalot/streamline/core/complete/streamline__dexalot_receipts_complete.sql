-- depends_on: {{ ref('bronze_dexalot__streamline_receipts') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    tags = ['dexalot','streamline_dexalot_complete']
) }}

SELECT
    block_number,
    complete_receipts_by_hash_id AS complete_receipts_id,
    inserted_timestamp,
    modified_timestamp,
    _inserted_timestamp,
    _invocation_id
FROM
    {{ ref('streamline__dexalot_receipts_by_hash_complete') }}

{% if is_incremental() %}
WHERE
    1 = 2
{% endif %}
UNION ALL
SELECT
    VALUE :BLOCK_NUMBER :: INT AS block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS complete_receipts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze_dexalot__streamline_receipts') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) _inserted_timestamp
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze_dexalot__streamline_fr_receipts') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY block_number
        ORDER BY
            _inserted_timestamp DESC)) = 1
