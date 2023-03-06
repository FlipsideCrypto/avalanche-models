{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'blocks', 'sql_limit', {{var('sql_limit','20000')}}, 'producer_batch_size', {{var('producer_batch_size','20000')}}, 'worker_batch_size', {{var('worker_batch_size','10000')}}, 'batch_call_limit', {{var('batch_call_limit','1000')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH blocks AS (

    SELECT
        block_number :: STRING AS block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number > 26000000
    EXCEPT
    SELECT
        block_number :: STRING
    FROM
        {{ ref("streamline__complete_blocks") }}
    WHERE
        block_number > 26000000
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "eth_getBlockByNumber", "params":[',
            block_number :: INTEGER,
            ',',
            FALSE :: BOOLEAN,
            '],"id":',
            block_number :: STRING,
            '}'
        )
    ) AS request
FROM
    blocks
