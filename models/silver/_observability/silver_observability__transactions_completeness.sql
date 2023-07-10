{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = false
) }}

WITH look_back AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_hour") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) BETWEEN 24
        AND 96
),
block_range AS (
    SELECT
        MAX(block_number) AS end_block,
        MIN(block_number) AS start_block
    FROM
        look_back
),
blocks_count AS (
    SELECT
        block_number,
        tx_hash,
        block_hash
    FROM
        {{ ref("silver__confirmed_blocks") }}
    WHERE
        block_number <= (
            SELECT
                end_block
            FROM
                block_range
        )

{% if is_incremental() %}
AND (
    (
        block_number BETWEEN (
            SELECT
                start_block
            FROM
                block_range
        )
        AND (
            SELECT
                end_block
            FROM
                block_range
        )
    )
    OR ({% if var('OBSERV_FULL_TEST') %}
        block_number >= 0
    {% else %}
        block_number >= (
    SELECT
        MIN(VALUE) - 1
    FROM
        (
    SELECT
        blocks_impacted_array
    FROM
        {{ this }}
        qualify ROW_NUMBER() over (
    ORDER BY
        test_timestamp DESC) = 1), LATERAL FLATTEN(input => blocks_impacted_array))
    {% endif %})
)
{% endif %}
),
txs_count AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        block_hash
    FROM
        {{ ref("silver__transactions") }}
    WHERE
        block_number <= (
            SELECT
                end_block
            FROM
                block_range
        )

{% if is_incremental() %}
AND (
    (
        block_number BETWEEN (
            SELECT
                start_block
            FROM
                block_range
        )
        AND (
            SELECT
                end_block
            FROM
                block_range
        )
    )
    OR ({% if var('OBSERV_FULL_TEST') %}
        block_number >= 0
    {% else %}
        block_number >= (
    SELECT
        MIN(VALUE) - 1
    FROM
        (
    SELECT
        blocks_impacted_array
    FROM
        {{ this }}
        qualify ROW_NUMBER() over (
    ORDER BY
        test_timestamp DESC) = 1), LATERAL FLATTEN(input => blocks_impacted_array))
    {% endif %})
)
{% endif %}
),
impacted_blocks AS (
    SELECT
        DISTINCT COALESCE(
            b.block_number,
            t.block_number
        ) AS block_number
    FROM
        blocks_count b full
        OUTER JOIN txs_count t
        ON b.block_number = t.block_number
        AND b.block_hash = t.block_hash
        AND b.tx_hash = t.tx_hash
    WHERE
        (
            t.tx_hash IS NULL
            OR b.tx_hash IS NULL
        )
        AND t.block_number <= (
            SELECT
                MAX(block_number)
            FROM
                blocks_count
        )
)
SELECT
    'transactions' AS test_name,
    (
        SELECT
            MIN(block_number)
        FROM
            txs_count
    ) AS min_block,
    (
        SELECT
            MAX(block_number)
        FROM
            txs_count
    ) AS max_block,
    (
        SELECT
            MIN(block_timestamp)
        FROM
            txs_count
    ) AS min_block_timestamp,
    (
        SELECT
            MAX(block_timestamp)
        FROM
            txs_count
    ) AS max_block_timestamp,
    (
        SELECT
            COUNT(
                DISTINCT block_number
            )
        FROM
            txs_count
    ) AS blocks_tested,
    (
        SELECT
            COUNT(*)
        FROM
            impacted_blocks
    ) AS blocks_impacted_count,
    (
        SELECT
            ARRAY_AGG(block_number) within GROUP (
                ORDER BY
                    block_number
            )
        FROM
            impacted_blocks
    ) AS blocks_impacted_array,
    CURRENT_TIMESTAMP() AS test_timestamp
