{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH tokens AS(

SELECT
  block_number,
  tx_hash,
  contract_address,
  event_inputs:value ::STRING as value
FROM avalanche.silver.logs 
WHERE topics[0] ::STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  AND origin_to_address = '0xe54ca86531e17ef3616d22ca28b0d458b6c89106' --Pangolin Router Contract
  AND block_number >= 57309 -- Earliest tx 
  AND event_name = 'Transfer'
  AND tx_status = 'SUCCESS'

)

SELECT 
  l.block_number,
  block_timestamp,
  l.tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  l.contract_address,
  po.address_name AS pool_name,
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
  'pangolin' AS platform,
  p.contract_address AS token_in,
  ou.contract_address AS token_out,
  cp.name AS symbol_in,
  co.name AS symbol_out,
  _log_id
FROM {{ ref('silver__logs') }} l

LEFT OUTER JOIN {{ ref('silver_dex__pools') }} i
  ON l.event_inputs:inputToken ::STRING = i.address
  
LEFT OUTER JOIN {{ ref('silver_dex__pools') }} o
  ON l.event_inputs:outputToken ::STRING = o.address 

LEFT OUTER JOIN tokens p
  ON CASE WHEN event_inputs:amount0In ::STRING = '0' THEN event_inputs:amount1In ::STRING
   ELSE event_inputs:amount0In END = p.value
  AND l.block_number = p.block_number
  AND l.tx_hash = p.tx_hash

LEFT OUTER JOIN tokens ou
  ON CASE WHEN event_inputs:amount0Out ::STRING = '0' THEN event_inputs:amount1Out ::STRING
   ELSE event_inputs:amount0Out END = ou.value
  AND l.block_number = ou.block_number
  AND l.tx_hash = ou.tx_hash

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} cp
  ON p.contract_address = cp.address

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} co
  ON ou.contract_address = co.address  

LEFT OUTER JOIN {{ ref('silver_dex__pools') }} po
  ON l.contract_address = po.address

WHERE l.topics[0] ::STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'
  AND l.origin_to_address = '0xe54ca86531e17ef3616d22ca28b0d458b6c89106' --Pangolin Router Contract
  AND l.block_number >= 57349 -- Earliest tx
  AND l.tx_status = 'SUCCESS'
  AND l.event_name = 'Swap'

