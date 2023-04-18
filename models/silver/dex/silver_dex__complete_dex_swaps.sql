{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE']
) }}

WITH contracts AS (

    SELECT
        address,
        symbol,
        NAME,
        decimals
    FROM
        {{ ref('core__dim_contracts') }}
),

prices AS (

    SELECT
        hour,
        token_address,
        price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address IN (
            SELECT
                DISTINCT address
            FROM
                contracts
        )
{% if is_incremental() %}
AND hour >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 2
    FROM
      {{ this }}
  )  
{% endif %}
), 

trader_joe_v1_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__trader_joe_v1_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE
    FROM
      {{ this }}
  )
{% endif %}
),

trader_joe_v2_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__trader_joe_v2_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE
    FROM
      {{ this }}
  )
{% endif %}
),

trader_joe_v2_1_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__trader_joe_v2_1_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE
    FROM
      {{ this }}
  )
{% endif %}
),

--union all standard dex CTEs here (excludes amount_usd)
all_dex_standard AS (
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  trader_joe_v1_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  trader_joe_v2_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  trader_joe_v2_1_swaps
),

--union all non-standard dex CTEs here (excludes amount_usd)
all_dex_custom AS (
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_in_usd,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id,
  _inserted_timestamp
FROM
  x
),

FINAL AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in,
    CASE
        WHEN s.decimals_in IS NOT NULL THEN ROUND(
            amount_in * p1.price, 2)
        ELSE NULL
    END AS amount_in_usd,
    amount_out,
    CASE
        WHEN s.decimals_out IS NOT NULL THEN ROUND(
            amount_out * p2.price, 2)
        ELSE NULL
    END AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
  FROM all_dex_standard s
  LEFT JOIN prices p1
    ON s.token_in = p1.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p1.hour
  LEFT JOIN prices p2
    ON s.token_out = p2.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p2.hour
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in,
    ROUND(
      amount_in_usd, 2) AS amount_in_usd,
    amount_out,
    ROUND(
      amount_out_usd, 2) AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
  FROM
    all_dex_custom c
)

SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  CASE
    WHEN ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_out_usd, 0)) > 0.5
      OR ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_in_usd, 0)) > 0.5 THEN NULL
    ELSE amount_in_usd
  END AS amount_in_usd,
  amount_out,
  CASE
    WHEN ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_in_usd, 0)) > 0.5
      OR ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_out_usd, 0)) > 0.5 THEN NULL
    ELSE amount_out_usd
  END AS amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id,
  _inserted_timestamp
FROM FINAL