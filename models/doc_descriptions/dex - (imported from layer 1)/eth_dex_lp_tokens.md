{% docs eth_dex_lp_tokens %}

The address for the token included in the liquidity pool, as a JSON object. 

Query example to access the key:value pairing within the object:
SELECT
    DISTINCT pool_address AS unique_pools,
    tokens :token0 :: STRING AS token0,
    symbols: token0 :: STRING AS token0_symbol,
    decimals: token0 :: STRING AS token0_decimal
FROM avalanche.defi.dim_dex_liquidity_pools
WHERE token0 = '0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab'
;

{% enddocs %}