{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

SELECT 
  l.block_number,
  block_timestamp,
  l.tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  l.contract_address,
  po.name AS pool_name,
  event_name,
  CASE 
   WHEN event_inputs:amount0In ::NUMERIC = 0 THEN event_inputs:amount1In ::NUMERIC / POW(10,18)
   ELSE event_inputs:amount0In ::NUMERIC / POW(10,18)
  END AS amount_in,
  NULL AS amount_in_usd,
  CASE 
   WHEN event_inputs:amount0Out ::NUMERIC = 0 THEN event_inputs:amount1Out ::NUMERIC / POW(10,18)
   ELSE event_inputs:amount0Out ::NUMERIC / POW(10,18)
  END AS amount_out,
  NULL AS amount_out_usd,
  event_inputs:sender ::STRING AS sender,
  event_index,
  'trader-joe' AS platform,
  ai.token0 AS token_in,
  ai.token1 AS token_out,
  cp.name AS symbol_in,
  co.name AS symbol_out,
  _log_id
FROM {{ ref('silver__logs') }} l

LEFT OUTER JOIN {{ ref('silver_dex__pools') }} ai
  ON l.contract_address = ai.pool

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} cp
  ON ai.token0 = cp.address

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} co
  ON ai.token1 = co.address  

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} po
  ON l.contract_address = po.address

WHERE l.topics[0] ::STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'
  AND l.origin_to_address = '0x60ae616a2155ee3d9a68541ba4544862310933d4' --Trader Joe Router Contract
  AND l.block_number >= 2504337 -- Earliest tx
  AND l.tx_status = 'SUCCESS'
  AND l.event_name = 'Swap'

