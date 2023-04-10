-- depends_on: {{ ref('bronze__streamline_traces') }}
{{ config(
    materialized = 'incremental',
    unique_key = "_call_id",
    cluster_by = "block_timestamp::date, _inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    full_refresh = False,
    enabled = False
) }}

WITH base AS (

    SELECT
        block_number,
        DATA,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{% if var('LIMIT_PARTITIONS') %}
    {{ ref('bronze__streamline_FR_traces') }}
    WHERE
        _partition_by_block_id BETWEEN (
            SELECT
                ROUND(MAX(block_number), -4) - 10000
            FROM
                {{ this }})
                AND (
                    SELECT
                        ROUND(MAX(block_number), -4) + 1000000
                    FROM
                        {{ this }})
                    {% else %}
                        {{ ref('bronze__streamline_traces') }}
                    WHERE
                        _inserted_timestamp >= (
                            SELECT
                                MAX(_inserted_timestamp) _inserted_timestamp
                            FROM
                                {{ this }}
                        )
                    {% endif %}
                    {% else %}
                        {{ ref('bronze__streamline_FR_traces') }}
                    WHERE
                        _partition_by_block_id <= 1000000
                    {% endif %}
                )
