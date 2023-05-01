{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

WITH meta AS (

    SELECT
        registered_on,
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "dexalot_eth_getBlockByNumber") }}'
            )
        ) A

{% if is_incremental() %}
WHERE
    LEAST(
        registered_on,
        last_modified
    ) >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    ),
    partitions AS (
        SELECT
            CAST(
                SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER
            ) AS _partition_by_block_number
        FROM
            meta
    )
{% else %}
)
{% endif %}
SELECT
    MD5(
        CAST(COALESCE(CAST(block_number AS text), '') AS text)
    ) AS id,
    block_number,
    registered_on AS _inserted_timestamp
FROM
    {{ source(
        "bronze_streamline",
        "dexalot_eth_getBlockByNumber"
    ) }}
    t
    JOIN meta b
    ON b.file_name = metadata$filename

{% if is_incremental() %}
JOIN partitions p
ON p._partition_by_block_number = t._partition_by_block_id
WHERE
    p._partition_by_block_number = t._partition_by_block_id
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
