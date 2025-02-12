{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    hash as block_hash,
    block_timestamp,
    'mainnet' AS network,
    tx_count,
    size,
    miner,
    extra_data,
    parent_hash,
    gas_used,
    gas_limit,
    base_fee_per_gas,
    difficulty,
    total_difficulty,
    sha3_uncles,
    uncles AS uncle_blocks,
    nonce,
    receipts_root,
    state_root,
    transactions_root,
    logs_bloom,
    COALESCE (
        blocks_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number']
        ) }}
    ) AS fact_blocks_id,
    GREATEST(
        COALESCE(
            A.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            d.inserted_timestamp,
            '2000-01-01'
        )
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE(
            A.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            d.modified_timestamp,
            '2000-01-01'
        )
    ) AS modified_timestamp,
    'avalanche' AS blockchain,
    hash,
    OBJECT_CONSTRUCT(
        'baseFeePerGas',
        base_fee_per_gas,
        'difficulty',
        difficulty,
        'extraData',
        extra_data,
        'gasLimit',
        gas_limit,
        'gasUsed',
        gas_used,
        'hash',
        HASH,
        'logsBloom',
        logs_bloom,
        'miner',
        miner,
        'nonce',
        nonce,
        'number',
        NUMBER,
        'parentHash',
        parent_hash,
        'receiptsRoot',
        receipts_root,
        'sha3Uncles',
        sha3_uncles,
        'size',
        SIZE,
        'stateRoot',
        state_root,
        'timestamp',
        block_timestamp,
        'totalDifficulty',
        total_difficulty,
        'transactionsRoot',
        transactions_root,
        'uncles',
        uncles
    ) AS block_header_json
FROM
    {{ ref('silver__blocks') }} A
    LEFT JOIN {{ ref('silver__tx_count') }}
    d USING (block_number)
