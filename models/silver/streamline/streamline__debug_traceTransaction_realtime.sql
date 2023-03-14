{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'debug_traceTransaction', 'sql_limit', {{var('sql_limit','160000')}}, 'producer_batch_size', {{var('producer_batch_size','40000')}}, 'worker_batch_size', {{var('worker_batch_size','20000')}}, 'batch_call_limit', {{var('batch_call_limit','10')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
),
transactions AS (
    SELECT
        tx_hash :: STRING AS tx_hash,
        block_number :: STRING AS block_number
    FROM
        {{ ref("streamline__txs") }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND block_number IS NOT NULL
    EXCEPT
    SELECT
        tx_hash :: STRING,
        block_number :: STRING
    FROM
        {{ ref("streamline__complete_debug_traceTransaction") }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "debug_traceTransaction", "params":["',
            tx_hash :: STRING,
            '",{"tracer": "callTracer"}',
            '],"id":"',
            block_number :: STRING,
            '-',
            tx_hash :: STRING,
            '"}'
        )
    ) AS request
FROM
    transactions