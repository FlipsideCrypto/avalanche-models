{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

SELECT 
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  COALESCE(i.address_name,o.address_name) AS pool_name,
  event_name,
  event_inputs:inputValue ::NUMERIC / CASE WHEN event_inputs:inputToken::STRING IN('0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', '0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab') THEN POW(10,18) ELSE POW(10,6) END AS amount_in,
  NULL AS amount_in_usd,
  event_inputs:outputValue ::NUMERIC / CASE WHEN event_inputs:outputToken::STRING IN('0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', '0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab') THEN POW(10,18) ELSE POW(10,6) END AS amount_out,
  NULL AS amount_out_usd,
  event_inputs:sender ::STRING AS sender,
  event_index,
  'gmx' AS platform,
  event_inputs:inputToken ::STRING AS token_in,
  event_inputs:outputToken ::STRING AS token_out,
  cp.name AS symbol_in,
  co.name AS symbol_out,
  _log_id
FROM {{ ref('silver__logs') }}

LEFT OUTER JOIN {{ ref('silver_dex__pools') }} i
  ON event_inputs:inputToken ::STRING = i.address
  
LEFT OUTER JOIN {{ ref('silver_dex__pools') }} o
  ON event_inputs:outputToken ::STRING = o.address  

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} cp
  ON event_inputs:inputToken ::STRING = cp.address

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} co
  ON event_inputs:outputToken ::STRING = co.address    

WHERE topics[0] ::STRING = '0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062'
  AND origin_to_address = '0x5f719c2f1095f7b9fc68a68e35b51194f4b6abe8' --GMX Router Contract
  AND block_number >= 8351228 -- Contract Creation
  AND event_name = 'Swap'
  AND tx_status = 'SUCCESS'

