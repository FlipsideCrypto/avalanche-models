{{ config (
    materialized = 'table',
    tags = ['streamline_dexalot_complete']
) }}

SELECT
    {{ target.database }}.live.udf_api(
        'POST',
        'https://subnets.avax.network/dexalot/mainnet/rpc',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state',
            'livequery'
        ),
        OBJECT_CONSTRUCT(
            'id',
            0,
            'jsonrpc',
            '2.0',
            'method',
            'eth_blockNumber',
            'params',
            []
        ),
        ''
    ) AS resp,
    utils.udf_hex_to_int(
        resp :data :result :: STRING
    ) AS block_number
