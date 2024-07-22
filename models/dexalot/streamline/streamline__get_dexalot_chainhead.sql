{{ config (
    materialized = 'table',
    tags = ['streamline_dexalot_blocks']
) }}

SELECT
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}',
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
        'Vault/prod/avalanche/dexalot/internal/mainnet'
    ) AS resp,
    utils.udf_hex_to_int(
        resp :data :result :: STRING
    ) AS block_number