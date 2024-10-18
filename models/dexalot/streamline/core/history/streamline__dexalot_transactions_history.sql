{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"dexalot_transactions",
        "sql_limit" :"10000",
        "producer_batch_size" :"5000",
        "worker_batch_size" :"1000",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["result.transactions"]) }
    ),
    tags = ['streamline_dexalot_history']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_dexalot_block_lookback") }}
),
to_do AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__dexalot_blocks") }}
    WHERE
        (
            block_number < (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__dexalot_transactions_complete") }}
    WHERE
        block_number < (
            SELECT
                block_number
            FROM
                last_3_days
        )
),
ready_blocks AS (
    SELECT
        block_number
    FROM
        to_do
)
SELECT
    block_number,
    ROUND(
        block_number,
        -3
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}',
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
            'eth_getBlockByNumber',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)),
            'Vault/prod/avalanche/dexalot/internal/mainnet'
        ) AS request
        FROM
            ready_blocks
        ORDER BY
            block_number ASC
