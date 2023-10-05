{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    tags = ['non_realtime']
) }}

WITH contract_deployments AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address AS deployer_address,
        to_address AS contract_address,
        _inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
    WHERE
        from_address IN (
            '0x05fb0089bec6d00b2f01f4096eb0e0488c79cd91',
            '0x7677bf119654d1fbcb46cb9014949bf16180b6ae'
        )
        AND TYPE ILIKE 'create%'
        AND tx_status ILIKE 'success'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND to_address NOT IN (
    SELECT
        DISTINCT pool_address
    FROM
        {{ this }}
)
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
FROM contract_deployments
