{% macro evm_missing_blocks() %}
    WITH look_back AS (
        SELECT
            block_number
        FROM
            {{ ref("_max_block_by_hour") }}
            qualify ROW_NUMBER() over (
                ORDER BY
                    block_number DESC
            ) BETWEEN 11
            AND 72
    ),
    block_range AS (
        SELECT
            MAX(block_number) AS end_block,
            MIN(block_number) AS start_block
        FROM
            look_back
    ),
    blocks AS (
        SELECT
            block_number,
            block_timestamp,
            LAG(
                block_number,
                1
            ) over (
                ORDER BY
                    block_number ASC
            ) AS prev_BLOCK_NUMBER
        FROM
            {{ ref("silver__blocks") }}
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
    OR block_number >=(
        SELECT
            MIN(VALUE) - 1
        FROM
            (
                SELECT
                    blocks_impacted_array
                FROM
                    {{ this }}
                    qualify(ROW_NUMBER() over(
                ORDER BY
                    test_timestamp DESC) = 1)
            ),
            LATERAL FLATTEN (
                input => blocks_impacted_array
            )
    )
)
{% endif %}
),
impacted_blocks AS (
    SELECT
        DISTINCT block_number AS block_number
    FROM
        blocks
    WHERE
        block_number - prev_BLOCK_NUMBER <> 1
)
SELECT
    'block_gaps' AS test_name,
    '001' AS test_id,
    (
        SELECT
            MIN(block_number)
        FROM
            blocks
    ) AS min_block,
    (
        SELECT
            MAX(block_number)
        FROM
            blocks
    ) AS max_block,
    (
        SELECT
            MIN(block_timestamp)
        FROM
            blocks
    ) AS min_block_timestamp,
    (
        SELECT
            MAX(block_timestamp)
        FROM
            blocks
    ) AS max_block_timestamp,
    (
        SELECT
            COUNT(*)
        FROM
            blocks
    ) AS blocks_tested,
    (
        SELECT
            COUNT(
                DISTINCT block_number
            )
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
{% endmacro %}

{% macro evm_missing_transactions() %}
    WITH look_back AS (
        SELECT
            block_number
        FROM
            {{ ref("_max_block_by_hour") }}
            qualify ROW_NUMBER() over (
                ORDER BY
                    block_number DESC
            ) BETWEEN 11
            AND 72
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
    OR block_number >=(
        SELECT
            MIN(VALUE) - 1
        FROM
            (
                SELECT
                    blocks_impacted_array
                FROM
                    {{ this }}
                    qualify(ROW_NUMBER() over(
                ORDER BY
                    test_timestamp DESC) = 1)
            ),
            LATERAL FLATTEN (
                input => blocks_impacted_array
            )
    )
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
    OR block_number >=(
        SELECT
            MIN(VALUE) - 1
        FROM
            (
                SELECT
                    blocks_impacted_array
                FROM
                    {{ this }}
                    qualify(ROW_NUMBER() over(
                ORDER BY
                    test_timestamp DESC) = 1)
            ),
            LATERAL FLATTEN (
                input => blocks_impacted_array
            )
    )
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
    'missing_txs' AS test_name,
    '002' AS test_id,
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
{% endmacro %}

{% macro evm_missing_receipts() %}
    WITH look_back AS (
        SELECT
            block_number
        FROM
            {{ ref("_max_block_by_hour") }}
            qualify ROW_NUMBER() over (
                ORDER BY
                    block_number DESC
            ) BETWEEN 11
            AND 72
    ),
    block_range AS (
        SELECT
            MAX(block_number) AS end_block,
            MIN(block_number) AS start_block
        FROM
            look_back
    ),
    txs AS (
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
    OR block_number >=(
        SELECT
            MIN(VALUE) - 1
        FROM
            (
                SELECT
                    blocks_impacted_array
                FROM
                    {{ this }}
                    qualify(ROW_NUMBER() over(
                ORDER BY
                    test_timestamp DESC) = 1)
            ),
            LATERAL FLATTEN (
                input => blocks_impacted_array
            )
    )
)
{% endif %}
),
receipts AS (
    SELECT
        block_number,
        tx_hash,
        block_hash
    FROM
        {{ ref("silver__receipts") }}
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
    OR block_number >=(
        SELECT
            MIN(VALUE) - 1
        FROM
            (
                SELECT
                    blocks_impacted_array
                FROM
                    {{ this }}
                    qualify(ROW_NUMBER() over(
                ORDER BY
                    test_timestamp DESC) = 1)
            ),
            LATERAL FLATTEN (
                input => blocks_impacted_array
            )
    )
)
{% endif %}
),
impacted_blocks AS (
    SELECT
        DISTINCT COALESCE(
            t.block_number,
            r.block_number
        ) AS block_number
    FROM
        txs t full
        OUTER JOIN receipts r
        ON t.block_number = r.block_number
        AND t.block_hash = r.block_hash
        AND t.tx_hash = r.tx_hash
    WHERE
        r.tx_hash IS NULL
        OR t.tx_hash IS NULL
)
SELECT
    'missing_receipts' AS test_name,
    '003' AS test_id,
    (
        SELECT
            MIN(block_number)
        FROM
            receipts
    ) AS min_block,
    (
        SELECT
            MAX(block_number)
        FROM
            receipts
    ) AS max_block,
    (
        SELECT
            MIN(block_timestamp)
        FROM
            txs
    ) AS min_block_timestamp,
    (
        SELECT
            MAX(block_timestamp)
        FROM
            txs
    ) AS max_block_timestamp,
    (
        SELECT
            COUNT(
                DISTINCT block_number
            )
        FROM
            receipts
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
{% endmacro %}

{% macro evm_missing_traces() %}
    WITH look_back AS (
        SELECT
            block_number
        FROM
            {{ ref("_max_block_by_hour") }}
            qualify ROW_NUMBER() over (
                ORDER BY
                    block_number DESC
            ) BETWEEN 11
            AND 72
    ),
    block_range AS (
        SELECT
            MAX(block_number) AS end_block,
            MIN(block_number) AS start_block
        FROM
            look_back
    ),
    txs AS (
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
    OR block_number >=(
        SELECT
            MIN(VALUE) - 1
        FROM
            (
                SELECT
                    blocks_impacted_array
                FROM
                    {{ this }}
                    qualify(ROW_NUMBER() over(
                ORDER BY
                    test_timestamp DESC) = 1)
            ),
            LATERAL FLATTEN (
                input => blocks_impacted_array
            )
    )
)
{% endif %}
),
traces AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash
    FROM
        {{ ref("silver__traces") }}
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
    OR block_number >=(
        SELECT
            MIN(VALUE) - 1
        FROM
            (
                SELECT
                    blocks_impacted_array
                FROM
                    {{ this }}
                    qualify(ROW_NUMBER() over(
                ORDER BY
                    test_timestamp DESC) = 1)
            ),
            LATERAL FLATTEN (
                input => blocks_impacted_array
            )
    )
)
{% endif %}
),
impacted_blocks AS (
    SELECT
        DISTINCT COALESCE(
            t.block_number,
            r.block_number
        ) AS block_number
    FROM
        txs t full
        OUTER JOIN traces r
        ON t.block_number = r.block_number
        AND t.tx_hash = r.tx_hash
    WHERE
        r.tx_hash IS NULL
        OR t.tx_hash IS NULL
)
SELECT
    'missing_traces' AS test_name,
    '004' AS test_id,
    (
        SELECT
            MIN(block_number)
        FROM
            traces
    ) AS min_block,
    (
        SELECT
            MAX(block_number)
        FROM
            traces
    ) AS max_block,
    (
        SELECT
            MIN(block_timestamp)
        FROM
            traces
    ) AS min_block_timestamp,
    (
        SELECT
            MAX(block_timestamp)
        FROM
            traces
    ) AS max_block_timestamp,
    (
        SELECT
            COUNT(
                DISTINCT block_number
            )
        FROM
            traces
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
{% endmacro %}

{% macro evm_missing_logs() %}
    WITH look_back AS (
        SELECT
            block_number
        FROM
            {{ ref("_max_block_by_hour") }}
            qualify ROW_NUMBER() over (
                ORDER BY
                    block_number DESC
            ) BETWEEN 11
            AND 72
    ),
    block_range AS (
        SELECT
            MAX(block_number) AS end_block,
            MIN(block_number) AS start_block
        FROM
            look_back
    ),
    receipts AS (
        SELECT
            block_number,
            tx_hash
        FROM
            {{ ref("silver__receipts") }}
        WHERE
            block_number <= (
                SELECT
                    end_block
                FROM
                    block_range
            )
            AND ARRAY_SIZE(logs) > 0

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
    OR block_number >=(
        SELECT
            MIN(VALUE) - 1
        FROM
            (
                SELECT
                    blocks_impacted_array
                FROM
                    {{ this }}
                    qualify(ROW_NUMBER() over(
                ORDER BY
                    test_timestamp DESC) = 1)
            ),
            LATERAL FLATTEN (
                input => blocks_impacted_array
            )
    )
)
{% endif %}
),
logs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash
    FROM
        {{ ref("silver__logs") }}
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
    OR block_number >=(
        SELECT
            MIN(VALUE) - 1
        FROM
            (
                SELECT
                    blocks_impacted_array
                FROM
                    {{ this }}
                    qualify(ROW_NUMBER() over(
                ORDER BY
                    test_timestamp DESC) = 1)
            ),
            LATERAL FLATTEN (
                input => blocks_impacted_array
            )
    )
)
{% endif %}
),
impacted_blocks AS (
    SELECT
        DISTINCT COALESCE(
            t.block_number,
            r.block_number
        ) AS block_number
    FROM
        receipts t full
        OUTER JOIN logs r
        ON t.block_number = r.block_number
        AND t.tx_hash = r.tx_hash
    WHERE
        r.tx_hash IS NULL
        OR t.tx_hash IS NULL
)
SELECT
    'missing_event_logs' AS test_name,
    '005' AS test_id,
    (
        SELECT
            MIN(block_number)
        FROM
            logs
    ) AS min_block,
    (
        SELECT
            MAX(block_number)
        FROM
            logs
    ) AS max_block,
    (
        SELECT
            MIN(block_timestamp)
        FROM
            logs
    ) AS min_block_timestamp,
    (
        SELECT
            MAX(block_timestamp)
        FROM
            logs
    ) AS max_block_timestamp,
    (
        SELECT
            COUNT(
                DISTINCT block_number
            )
        FROM
            logs
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
{% endmacro %}
