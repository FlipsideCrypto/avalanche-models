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
  CASE
    WHEN event_inputs:inputToken ::STRING = '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e' THEN 'USDC'
    WHEN event_inputs:inputToken ::STRING = '0x152b9d0fdc40c096757f570a51e494bd4b943e50' THEN 'BTC.b'
    WHEN event_inputs:inputToken ::STRING = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' THEN 'WAVAX'
    WHEN event_inputs:inputToken ::STRING = '0x50b7545627a5162f82a992c33b87adc75187b218' THEN 'WBTC.e'
    WHEN event_inputs:inputToken ::STRING = '0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664' THEN 'USDC.e'
    WHEN event_inputs:inputToken ::STRING = '0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab' THEN 'WETH.e'
    ELSE event_inputs:inputToken ::STRING
  END AS symbol_in,
  CASE
    WHEN event_inputs:outputToken ::STRING = '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e' THEN 'USDC'
    WHEN event_inputs:outputToken ::STRING = '0x152b9d0fdc40c096757f570a51e494bd4b943e50' THEN 'BTC.b'
    WHEN event_inputs:outputToken ::STRING = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' THEN 'WAVAX'
    WHEN event_inputs:outputToken ::STRING = '0x50b7545627a5162f82a992c33b87adc75187b218' THEN 'WBTC.e'
    WHEN event_inputs:outputToken ::STRING = '0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664' THEN 'USDC.e'
    WHEN event_inputs:outputToken ::STRING = '0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab' THEN 'WETH.e'
    ELSE event_inputs:outputToken ::STRING
  END AS symbol_out,
  _log_id
FROM {{ ref('silver__logs') }}

LEFT OUTER JOIN {{ ref('silver_dex__gmx_pools') }} i
  ON event_inputs:inputToken ::STRING = i.address
  
LEFT OUTER JOIN {{ ref('silver_dex__gmx_pools') }} o
  ON event_inputs:outputToken ::STRING = o.address  

WHERE topics[0] ::STRING = '0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062'
  AND origin_to_address = '0x5f719c2f1095f7b9fc68a68e35b51194f4b6abe8' --GMX Router Contract
  AND block_number >= 8351228 -- Contract Creation

