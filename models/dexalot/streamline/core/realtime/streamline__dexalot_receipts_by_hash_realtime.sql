{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"dexalot_receipts_by_hash",
        "sql_limit" :"25000",
        "producer_batch_size" :"10000",
        "worker_batch_size" :"2000",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['dexalot','stale']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_dexalot_block_lookback") }}
),
to_do AS (
    SELECT
        block_number,
        tx_hash
    FROM
        {{ ref("streamline__dexalot_txs") }}
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
        AND tx_hash IS NOT NULL
    EXCEPT
    SELECT
        block_number,
        tx_hash
    FROM
        {{ ref("streamline__dexalot_receipts_by_hash_complete") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
),
ready_blocks AS (
    SELECT
        block_number,
        tx_hash
    FROM
        to_do
)
SELECT
    block_number,
    tx_hash,
    ROUND(
        block_number,
        -3
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{URL}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_number :: STRING,
            'jsonrpc',
            '2.0',
            'method',
            'eth_getTransactionReceipt',
            'params',
            ARRAY_CONSTRUCT(tx_hash)
        ),
        'Vault/prod/evm/nirvana/dexalot/mainnet'
    ) AS request
FROM
    ready_blocks
ORDER BY
    block_number ASC
