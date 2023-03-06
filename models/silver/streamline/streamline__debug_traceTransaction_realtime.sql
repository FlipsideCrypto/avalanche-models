{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'debug_traceTransaction', 'sql_limit', {{var('sql_limit','400000')}}, 'producer_batch_size', {{var('producer_batch_size','40000')}}, 'worker_batch_size', {{var('worker_batch_size','20000')}}, 'batch_call_limit', {{var('batch_call_limit','10')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH transactions AS (

    SELECT
        tx_hash :: STRING as tx_hash,
        block_number :: STRING AS block_number
    FROM
        {{ ref("streamline__transactions") }}
    WHERE
        block_number > 26000000
    EXCEPT
    SELECT
        tx_hash :: STRING,
        block_number :: STRING
    FROM
        {{ ref("streamline__complete_debug_traceTransaction") }}
    WHERE
        block_number > 26000000
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
