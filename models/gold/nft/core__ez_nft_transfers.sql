{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' 
            }
        }
    }
) }}

SELECT
    block_timestamp,
    block_number,
    tx_hash,
    event_index,
    event_type,
    contract_address AS nft_address,
    token_name as project_name,
    from_address AS nft_from_address,
    to_address AS nft_to_address,
    tokenId,
    erc1155_value
FROM
    {{ ref('silver__nft_transfers') }}

    left join {{ ref('silver__contracts') }} using (contract_address)

