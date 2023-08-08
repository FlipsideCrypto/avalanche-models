{{ config(
    materialized = 'incremental',
    unique_key = 'nft_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH raw_logs AS (

    SELECT
        *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN topics [0] :: STRING = '0x9634876a7ae7fcd98f91878964895038e3c5291ed5176557fe818e6a7edc6049' THEN 'CommissionSent'
            WHEN topics [0] :: STRING = '0x637d7d42dbff8be0a38276d141ac56dcd3235fb305480f6568b03e329c50ea62' THEN 'RoyaltyReceived'
            WHEN topics [0] :: STRING = '0xb01e54e29f65d01d12cc9c68660a6a04cf31f388669ab29d964abf266ca0419a' THEN 'PayoutCompleted'
            ELSE NULL
        END AS topics_event_name
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp :: DATE >= '2022-10-01'
        AND contract_address IN (
            '0xd106ec6e81e9b7f5bd33a6091a3c3e45b6183dc3',
            '0xa5128fbbd52a6572a8dad43b578bb3d693772447',
            -- english
            '0x1425d8a410d1bf8bfcf983048070a8ec2fd634d4' -- dutch
        )
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
auction_tag AS (
    SELECT
        DISTINCT tx_hash,
        contract_address AS auction_contract_address,
        IFF(
            contract_address = '0xa5128fbbd52a6572a8dad43b578bb3d693772447',
            'English Auction',
            'Dutch Auction'
        ) AS auction_label
    FROM
        raw_logs
    WHERE
        contract_address IN (
            '0xa5128fbbd52a6572a8dad43b578bb3d693772447',
            '0x1425d8a410d1bf8bfcf983048070a8ec2fd634d4'
        )
),
payout_raw AS (
    SELECT
        tx_hash,
        contract_address,
        event_index AS payout_event_index,
        topics,
        '0x' || SUBSTR(
            topics [1] :: STRING,
            27,
            40
        ) AS nft_address,
        utils.udf_hex_to_int(
            topics [2] :: STRING
        ) AS tokenid,
        '0x' || SUBSTR(
            topics [3] :: STRING,
            27,
            40
        ) AS nft_owner,
        -- payout receiver
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS sale_amount_raw,
        CONCAT(
            tx_hash,
            '-',
            nft_address,
            '-',
            tokenid,
            '-',
            nft_owner
        ) AS tx_identifier,
        topics_event_name,
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
    FROM
        raw_logs
    WHERE
        topics_event_name = 'PayoutCompleted'
        AND contract_address = '0xd106ec6e81e9b7f5bd33a6091a3c3e45b6183dc3'
),
commission_raw AS (
    SELECT
        tx_hash,
        event_index AS commission_event_index,
        '0x' || SUBSTR(
            topics [1] :: STRING,
            27,
            40
        ) AS nft_address,
        utils.udf_hex_to_int(
            topics [2] :: STRING
        ) AS tokenid,
        '0x' || SUBSTR(
            segmented_data [0] :: STRING,
            25,
            40
        ) AS nft_owner,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS platform_fee_raw,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS total_price_raw,
        segmented_data
    FROM
        raw_logs
    WHERE
        topics_event_name = 'CommissionSent'
        AND contract_address = '0xd106ec6e81e9b7f5bd33a6091a3c3e45b6183dc3'
),
royalty_raw AS (
    SELECT
        tx_hash,
        event_index AS royalty_event_index,
        '0x' || SUBSTR(
            topics [1] :: STRING,
            27,
            40
        ) AS nft_address,
        utils.udf_hex_to_int(
            topics [2] :: STRING
        ) AS tokenid,
        '0x' || SUBSTR(
            topics [3] :: STRING,
            27,
            40
        ) AS royalty_receiver,
        '0x' || SUBSTR(
            segmented_data [0] :: STRING,
            25,
            40
        ) AS nft_owner,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS creator_fee_raw_,
        CONCAT(
            tx_hash,
            '-',
            nft_address,
            '-',
            tokenid,
            '-',
            nft_owner
        ) AS tx_identifier
    FROM
        raw_logs
    WHERE
        topics_event_name = 'RoyaltyReceived'
        AND contract_address = '0xd106ec6e81e9b7f5bd33a6091a3c3e45b6183dc3'
),
royalty_agg AS (
    SELECT
        tx_identifier,
        SUM(creator_fee_raw_) AS creator_fee_raw
    FROM
        royalty_raw
    GROUP BY
        ALL
),
base AS (
    SELECT
        tx_hash,
        topics_event_name,
        tx_identifier,
        nft_address,
        tokenid,
        nft_owner,
        auction_contract_address,
        auction_label,
        contract_address,
        payout_event_index,
        COALESCE(
            sale_amount_raw,
            0
        ) AS sale_amount_raw,
        COALESCE(
            platform_fee_raw,
            0
        ) AS platform_fee_raw,
        COALESCE(
            creator_fee_raw,
            0
        ) AS creator_fee_raw,
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
    FROM
        payout_raw
        LEFT JOIN commission_raw USING (
            tx_hash,
            nft_address,
            tokenid,
            nft_owner
        )
        LEFT JOIN royalty_agg USING (tx_identifier)
        LEFT JOIN auction_tag USING (tx_hash)
),
nft_transfers AS (
    SELECT
        tx_hash,
        contract_address AS nft_address,
        from_address AS nft_from_address,
        to_address AS nft_to_address,
        tokenid,
        erc1155_value
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2022-10-01'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                base
        )
),
tx_data AS (
    SELECT
        tx_hash,
        tx_fee,
        input_data
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= '2022-10-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),
final_base AS (
    SELECT
        *
    FROM
        base
        INNER JOIN tx_data USING (tx_hash)
        LEFT JOIN nft_transfers USING (
            tx_hash,
            nft_address,
            tokenid
        )
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    payout_event_index,
    CASE
        WHEN origin_from_address = nft_owner THEN 'bid_won'
        WHEN auction_contract_address IS NOT NULL THEN 'bid_won'
        ELSE 'sale'
    END AS event_type,
    topics_event_name AS event_name,
    auction_contract_address,
    auction_label,
    tx_identifier,
    'salvor' AS platform_name,
    contract_address AS platform_address,
    'salvor v1' AS platform_exchange_version,
    nft_from_address,
    nft_owner AS seller_address,
    nft_to_address AS buyer_address,
    nft_address,
    tokenid,
    erc1155_value,
    sale_amount_raw,
    platform_fee_raw,
    creator_fee_raw,
    platform_fee_raw + creator_fee_raw AS total_fees_raw,
    total_fees_raw + sale_amount_raw AS total_price_raw,
    'AXAX' AS currency_address,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_fee,
    input_data,
    CONCAT(
        nft_address,
        '-',
        tokenid,
        '-',
        _log_id,
        '-',
        platform_exchange_version
    ) AS nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    final_base
