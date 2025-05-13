{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','nft','curated']
) }}

WITH raw_logs AS (

    SELECT
        *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN topics [0] :: STRING = '0x9634876a7ae7fcd98f91878964895038e3c5291ed5176557fe818e6a7edc6049' THEN 'CommissionSent'
            WHEN topics [0] :: STRING = '0x637d7d42dbff8be0a38276d141ac56dcd3235fb305480f6568b03e329c50ea62' THEN 'RoyaltyReceived'
            WHEN topics [0] :: STRING = '0xb01e54e29f65d01d12cc9c68660a6a04cf31f388669ab29d964abf266ca0419a' THEN 'PayoutCompleted'
            WHEN topics [0] :: STRING = '0x15d4649ef85f6d7a1e2068dd3d0c51d49d0257fa627d1e46abe3e1b3458d8b00' THEN 'AuctionSettled'
            WHEN topics [0] :: STRING = '0x2f258d8ad9499ea044033d10f2d28e770de5366a12c487afee76b4083e8edfb9' THEN 'ShareIncome'
        END AS topics_event_name,
        modified_timestamp AS _inserted_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2022-10-01'
        AND contract_address IN (
            '0xd106ec6e81e9b7f5bd33a6091a3c3e45b6183dc3',
            '0xa5128fbbd52a6572a8dad43b578bb3d693772447',
            -- english
            '0x1425d8a410d1bf8bfcf983048070a8ec2fd634d4' -- dutch
        )
        AND topics [0] :: STRING IN (
            '0x9634876a7ae7fcd98f91878964895038e3c5291ed5176557fe818e6a7edc6049',
            '0x637d7d42dbff8be0a38276d141ac56dcd3235fb305480f6568b03e329c50ea62',
            '0xb01e54e29f65d01d12cc9c68660a6a04cf31f388669ab29d964abf266ca0419a',
            '0x15d4649ef85f6d7a1e2068dd3d0c51d49d0257fa627d1e46abe3e1b3458d8b00',
            '0x2f258d8ad9499ea044033d10f2d28e770de5366a12c487afee76b4083e8edfb9'
        )
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        DATEADD('hour', -12, MAX(_inserted_timestamp))
    FROM
        {{ this }}
)
AND modified_timestamp >= DATEADD('day', -7, SYSDATE())
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
        ) :: STRING AS tokenid,
        '0x' || SUBSTR(
            topics [3] :: STRING,
            27,
            40
        ) AS nft_owner,
        -- payout receiver
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INT AS sale_amount_raw,
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
auction_settled_raw AS (
    SELECT
        tx_hash,
        event_index,
        contract_address,
        topics_event_name,
        topics,
        segmented_data,
        '0x' || SUBSTR(
            topics [1] :: STRING,
            27
        ) :: STRING AS nft_address,
        utils.udf_hex_to_int(
            topics [2] :: STRING
        ) :: STRING AS tokenid,
        '0x' || SUBSTR(
            topics [3] :: STRING,
            27
        ) AS nft_owner,
        '0x' || SUBSTR(
            segmented_data [0] :: STRING,
            25
        ) AS highest_bidder,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INT AS highest_bid,
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
        topics_event_name = 'AuctionSettled'
        AND contract_address = '0xa5128fbbd52a6572a8dad43b578bb3d693772447'
),
share_income_raw AS (
    SELECT
        tx_hash,
        event_index,
        topics,
        segmented_data,
        '0x' || SUBSTR(
            topics [1] :: STRING,
            27
        ) AS nft_address,
        utils.udf_hex_to_int(
            topics [2] :: STRING
        ) :: STRING AS tokenid,
        '0x' || SUBSTR(
            topics [3] :: STRING,
            27
        ) AS share_income_receiver,
        '0x' || SUBSTR(
            segmented_data [0] :: STRING,
            25
        ) AS nft_owner,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INT AS share_amount,
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
        topics_event_name = 'ShareIncome'
        AND contract_address = LOWER('0xd106ec6e81E9B7F5Bd33A6091A3c3e45B6183dc3')
),
share_income_agg AS (
    SELECT
        tx_identifier,
        SUM(share_amount) AS total_share_amount
    FROM
        share_income_raw
    GROUP BY
        ALL
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
        ) :: STRING AS tokenid,
        '0x' || SUBSTR(
            segmented_data [0] :: STRING,
            25,
            40
        ) AS nft_owner,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INT AS platform_fee_raw,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INT AS total_price_raw,
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
        ) :: STRING AS tokenid,
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
        ) :: INT AS creator_fee_raw_,
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
        COALESCE(
            highest_bid,
            0
        ) AS total_bid_raw,
        COALESCE(
            total_share_amount,
            0
        ) AS total_share_raw,
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        p._log_id,
        _inserted_timestamp
    FROM
        payout_raw p
        LEFT JOIN commission_raw USING (
            tx_hash,
            nft_address,
            tokenid,
            nft_owner
        )
        LEFT JOIN royalty_agg USING (tx_identifier)
        LEFT JOIN share_income_agg USING (tx_identifier)
        LEFT JOIN auction_settled_raw USING (
            tx_hash,
            tx_identifier
        )
        LEFT JOIN auction_tag USING (tx_hash)
),
nft_transfers AS (
    SELECT
        tx_hash,
        contract_address AS nft_address,
        from_address AS nft_from_address,
        to_address AS nft_to_address,
        token_id AS tokenid,
        NULL AS erc1155_value
    FROM
        {{ ref('nft__ez_nft_transfers') }}
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
        {{ ref('core__fact_transactions') }}
    WHERE
        block_timestamp :: DATE >= '2022-10-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'

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
    payout_event_index AS event_index,
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
    total_bid_raw,
    total_share_raw,
    platform_fee_raw + creator_fee_raw AS total_fees_raw,
    CASE
        WHEN auction_label IS NOT NULL THEN total_bid_raw
        WHEN auction_label IS NULL
        AND sale_amount_raw = 0 THEN (
            total_share_raw + total_fees_raw
        )
        WHEN auction_label IS NULL
        AND sale_amount_raw > 0 THEN (
            sale_amount_raw + total_fees_raw
        )
    END AS total_price_raw,
    'AVAX' AS currency_address,
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
