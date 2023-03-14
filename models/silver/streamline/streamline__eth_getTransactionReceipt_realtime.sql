{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'eth_getTransactionReceipt', 'sql_limit', {{var('sql_limit','200000')}}, 'producer_batch_size', {{var('producer_batch_size','50000')}}, 'worker_batch_size', {{var('worker_batch_size','25000')}}, 'batch_call_limit', {{var('batch_call_limit','500')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH transactions AS (

    SELECT
        tx_hash :: STRING as tx_hash,
        block_number :: STRING AS block_number
    FROM
        {{ ref("streamline__txs") }}
    WHERE
        block_number > 27000000
    EXCEPT
    SELECT
        tx_hash :: STRING,
        block_number :: STRING
    FROM
        {{ ref("streamline__complete_eth_getTransactionReceipt") }}
    WHERE
        block_number > 27000000
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "eth_getTransactionReceipt", "params":["',
            tx_hash :: STRING,
            '"],"id":"',
            block_number :: STRING,
            '-',
            tx_hash :: STRING,
            '"}'
        )
    ) AS request
FROM
    transactions
