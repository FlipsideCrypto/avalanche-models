{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH borrow_txns AS (

  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0x3a5151e57d3bc9798e7853034ac52293d1a0e12a2b44725e75b03b21f86477a6'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
repay_txns AS (
  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0xc8e512d8f188ca059984b5853d2bf653da902696b8512785b182b2c813789a6e'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
borrow0 AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Borrow' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            borrow_txns
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
pay_coll AS (
  SELECT
    tx_hash,
    CONCAT ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS collateral,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lending_pool_address,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS collateral_amount,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            borrow_txns
        )
        AND CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
borrow AS (
  SELECT
    A.*,
    b.collateral_amount,
    b.collateral AS collateral_address
  FROM
    borrow0 A
    LEFT JOIN pay_coll b
    ON A.tx_hash = b.tx_hash
    AND A.lending_pool_address = b.lending_pool_address
),
repay0 AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Repay' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS lender_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            repay_txns
        )
        AND CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
receive_coll AS (
  SELECT
    tx_hash,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS collateral,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lending_pool_address,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS collateral_amount,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            repay_txns
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
repay AS (
  SELECT
    A.*,
    b.collateral_amount,
    b.collateral AS collateral_address
  FROM
    repay0 A
    LEFT JOIN receive_coll b
    ON A.tx_hash = b.tx_hash
    AND A.lending_pool_address = b.lending_pool_address
),
total AS (
  SELECT
    *
  FROM
    borrow
  UNION ALL
  SELECT
    *
  FROM
    repay
),
prices AS (
  SELECT
    symbol,
    DATE_TRUNC(
      'hour',
      recorded_at
    ) AS HOUR,
    AVG(price) AS price
  FROM
    {{ source(
      'prices',
      'prices_v2'
    ) }} A
    JOIN {{ ref('sushi__dim_kashi_pairs') }}
    b
    ON A.symbol = b.asset_symbol
  WHERE
    1 = 1

{% if is_incremental() %}
AND HOUR :: DATE IN (
  SELECT
    DISTINCT block_timestamp :: DATE
  FROM
    borrow
)
{% else %}
  AND HOUR :: DATE >= '2021-09-01'
{% endif %}
GROUP BY
  1,
  2
),
collateral_prices AS (
  SELECT
    symbol,
    DATE_TRUNC(
      'hour',
      recorded_at
    ) AS HOUR,
    AVG(price) AS collateral_price
  FROM
    {{ source(
      'prices',
      'prices_v2'
    ) }} A
    JOIN {{ ref('sushi__dim_kashi_pairs') }}
    b
    ON A.symbol = b.collateral_symbol
  WHERE
    1 = 1

{% if is_incremental() %}
AND HOUR :: DATE IN (
  SELECT
    DISTINCT block_timestamp :: DATE
  FROM
    borrow
)
{% else %}
  AND HOUR :: DATE >= '2021-09-01'
{% endif %}
GROUP BY
  1,
  2
),
labels AS (
  SELECT
    *
  FROM
    {{ ref('sushi__dim_kashi_pairs') }}
),
labled_wo_prices AS (
  SELECT
    A.block_timestamp,
    A.block_number,
    A.tx_hash,
    A.action,
    A.origin_from_address,
    A.origin_to_address,
    A.origin_function_signature,
    A.asset,
    A.borrower2 AS borrower,
    A.borrower_is_a_contract,
    A.lending_pool_address,
    A.event_index,
    b.asset_decimals,
    CASE
      WHEN b.asset_decimals IS NULL THEN A.amount
      ELSE (A.amount / pow(10, b.asset_decimals))
    END AS asset_amount,
    CASE
      WHEN b.collateral_decimals IS NULL THEN A.collateral_amount
      ELSE (
        A.collateral_amount / pow(
          10,
          b.collateral_decimals
        )
      )
    END AS collateral_amount,
    b.pair_name AS lending_pool,
    b.asset_symbol AS symbol,
    A._log_id,
    b.collateral_symbol AS collateral_symbol,
    A.collateral_address,
    _inserted_timestamp
  FROM
    total A
    LEFT JOIN labels b
    ON A.lending_pool_address = b.pair_address
)
SELECT
  A.block_timestamp,
  A.block_number,
  A.tx_hash,
  A.action,
  A.origin_from_address,
  A.origin_to_address,
  A.origin_function_signature,
  A.borrower,
  A.borrower_is_a_contract,
  A.lending_pool_address,
  A.event_index,
  A.lending_pool,
  A.asset,
  A.symbol,
  A.asset_amount,
  (
    A.asset_amount * C.price
  ) AS asset_amount_USD,
  A.collateral_address,
  A.collateral_symbol,
  A.collateral_amount,
  (
    A.collateral_amount * d.collateral_price
  ) AS collateral_amount_USD,
  A._log_id,
  _inserted_timestamp
FROM
  labled_wo_prices A
  LEFT JOIN prices C
  ON A.symbol = C.symbol
  AND DATE_TRUNC(
    'hour',
    A.block_timestamp
  ) = C.hour
  LEFT JOIN collateral_prices d
  ON A.symbol = d.symbol
  AND DATE_TRUNC(
    'hour',
    A.block_timestamp
  ) = d.hour
