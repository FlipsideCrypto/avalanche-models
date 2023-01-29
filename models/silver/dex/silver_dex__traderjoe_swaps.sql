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
  event_inputs:value ::NUMERIC as value
FROM avalanche.silver.logs 
WHERE topics[0] ::STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  AND origin_to_address = '0x60ae616a2155ee3d9a68541ba4544862310933d4' --Trader Joe Router Contract
  AND block_number >= 2504337 -- Earliest tx 
  AND event_name = 'Transfer'

)



SELECT 
  l.block_number,
  block_timestamp,
  l.tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  l.contract_address,
  address_name AS pool_name,
  event_name,
  event_inputs:amount1In ::NUMERIC / POW(10,18) AS amount_in,
  event_inputs:amount0Out ::NUMERIC / POW(10,18) AS amount_out,
  event_inputs:sender ::STRING AS sender,
  event_index,
  'trader-joe' AS platform,
  p.contract_address AS token_in,
  o.contract_address AS token_out,
  cp.address AS symbol_in,
  co.address AS symbol_out,
  _log_id
FROM {{ ref('silver__logs') }} l

LEFT OUTER JOIN {{ ref('silver_dex__gmx_tj_pools') }} i
  ON l.event_inputs:inputToken ::STRING = i.address
  
LEFT OUTER JOIN {{ ref('silver_dex__gmx_tj_pools') }} o
  ON o.event_inputs:outputToken ::STRING = o.address 

LEFT OUTER JOIN tokens p
  ON l.event_inputs:amount1In ::NUMERIC = p.value
  AND l.block_number = p.block_number
  AND l.tx_hash = p.tx_hash

LEFT OUTER JOIN tokens o
  ON l.event_inputs:amount0Out  ::NUMERIC = o.value
  AND l.block_number = o.block_number
  AND l.tx_hash = o.tx_hash

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} cp
  ON p.contract_address = cp.address

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} co
  ON o.contract_address = co.address  

LEFT OUTER JOIN {{ ref('silver_dex__gmx_tj_pools') }} po
  ON l.contract_address = po.address

WHERE topics[0] ::STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'
  AND origin_to_address = '0x60ae616a2155ee3d9a68541ba4544862310933d4' --Trader Joe Router Contract
  AND block_number >= 2504337 -- Earliest tx
  AND tx_status = 'SUCCESS'
  AND event_name = 'Swap'

