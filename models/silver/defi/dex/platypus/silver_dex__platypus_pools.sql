{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    tags = ['curated']
) }}

WITH contract_deployments AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address AS deployer_address,
        to_address AS contract_address,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        from_address = '0x416a7989a964c9ed60257b064efc3a30fe6bf2ee'
        AND TYPE ILIKE 'create%'
        AND tx_succeeded
        AND trace_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'

{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY to_address
ORDER BY
    block_timestamp ASC)) = 1
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    deployer_address,
    contract_address AS pool_address,
    _inserted_timestamp
FROM
    contract_deployments
