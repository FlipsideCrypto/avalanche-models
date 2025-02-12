{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    incremental_strategy = 'delete+insert',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(hash,parent_hash,receipts_root,sha3_uncles)",
    tags = ['dexalot_non_realtime']
) }}

SELECT
    A.block_number AS block_number,
    hash AS block_hash,
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
    uncles as uncle_blocks,
    nonce,
    number,
    sha3_uncles,
    receipts_root,
    state_root,
    transactions_root,
    logs_bloom,
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
    ) AS block_header_json,
    blocks_id AS fact_blocks_id,
    inserted_timestamp,
    modified_timestamp,
    'dexalot' AS blockchain,
    hash
FROM
    {{ ref('silver_dexalot__blocks') }} A

{% if is_incremental() %}
WHERE
    A.modified_timestamp > (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY A.block_number
ORDER BY
    A.modified_timestamp DESC)) = 1
