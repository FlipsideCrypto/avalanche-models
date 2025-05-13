{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','nft','curated']
) }}

WITH raw_decoded_logs AS (

    SELECT
        *
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp >= '2022-04-01'
        AND contract_address IN (
            '0xae079eda901f7727d0715aff8f82ba8295719977',
            '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
        )
        AND event_name IN (
            'TakerBid',
            'TakerAsk',
            'RoyaltyPayment',
            'Transfer'
        )
        AND tx_succeeded

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
sale_events AS (
    SELECT
        tx_hash,
        event_name,
        contract_address AS platform_address,
        decoded_log,
        decoded_log :amount :: INT AS quantity,
        decoded_log :collection :: STRING AS nft_address,
        decoded_log :tokenId :: STRING AS tokenid,
        decoded_log :currency :: STRING AS currency_address,
        decoded_log :maker :: STRING AS maker,
        decoded_log :taker :: STRING AS taker,
        IFF(
            event_name = 'TakerBid',
            taker,
            maker
        ) AS buyer_address,
        IFF(
            event_name = 'TakerBid',
            maker,
            taker
        ) AS seller_address,
        decoded_log :price :: INT AS total_price_raw,
        decoded_log :strategy :: STRING AS strategy,
        decoded_log :orderHash :: STRING AS orderhash,
        decoded_log :orderNonce :: STRING AS ordernonce,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS intra_tx_sale_rn,
        block_number,
        block_timestamp,
        event_index,
        CONCAT(
            tx_hash,
            event_index
        ) AS tx_hash_event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        raw_decoded_logs
    WHERE
        event_name IN (
            'TakerBid',
            'TakerAsk'
        )
        AND contract_address = '0xae079eda901f7727d0715aff8f82ba8295719977'
),
royalty_events AS (
    SELECT
        tx_hash,
        event_index AS royalty_event_index,
        decoded_log,
        decoded_log :amount :: INT AS creator_fee_raw_,
        decoded_log :currency :: STRING AS royalty_currency,
        decoded_log :collection :: STRING AS nft_address,
        decoded_log :tokenId :: STRING AS tokenid,
        decoded_log :royaltyRecipient :: STRING AS royalty_recipient,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS intra_tx_sale_rn
    FROM
        raw_decoded_logs
    WHERE
        event_name IN ('RoyaltyPayment')
        AND contract_address = '0xae079eda901f7727d0715aff8f82ba8295719977'
),
royalty_filter_raw AS (
    SELECT
        s.tx_hash,
        s.event_index,
        r.royalty_event_index,
        creator_fee_raw_
    FROM
        sale_events s
        LEFT JOIN royalty_events r
        ON s.tx_hash = r.tx_hash
        AND s.nft_address = r.nft_address
        AND s.tokenid = r.tokenid
        AND s.event_index > r.royalty_event_index
    WHERE
        royalty_event_index IS NOT NULL qualify ROW_NUMBER() over (
            PARTITION BY s.tx_hash,
            r.royalty_event_index
            ORDER BY
                s.event_index ASC
        ) = 1
),
royalty_filter_agg AS (
    SELECT
        tx_hash,
        event_index,
        SUM(creator_fee_raw_) AS creator_fee_raw
    FROM
        royalty_filter_raw
    GROUP BY
        ALL
),
sales_with_royalties AS (
    SELECT
        tx_hash,
        event_name,
        platform_address,
        quantity,
        nft_address,
        tokenid,
        currency_address,
        buyer_address,
        seller_address,
        total_price_raw,
        event_index,
        COALESCE(
            creator_fee_raw,
            0
        ) AS creator_fee_raw,
        block_number,
        block_timestamp,
        strategy,
        orderhash,
        ordernonce,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
    FROM
        sale_events
        LEFT JOIN royalty_filter_agg USING (
            tx_hash,
            event_index
        )
),
platform_fee_transfers AS (
    SELECT
        tx_hash,
        event_index AS platform_event_index,
        decoded_log :dst :: STRING AS platform_fee_recipient,
        decoded_log :src :: STRING AS platform_fee_payer,
        decoded_log :wad :: INT AS platform_fee_raw_
    FROM
        raw_decoded_logs
    WHERE
        event_name = 'Transfer'
        AND contract_address = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                sale_events
        )
        AND platform_fee_recipient IN (
            '0x64c4607ad853999ee5042ba8377bfc4099c273de',
            -- current
            '0x2fbb61a10b96254900c03f1644e9e1d2f5e76dd2' -- super old
        )
        AND (
            platform_fee_payer = '0xae079eda901f7727d0715aff8f82ba8295719977'
            OR platform_fee_payer IN (
                SELECT
                    buyer_address
                FROM
                    sale_events
            )
        )
),
platform_fee_filter_raw AS (
    SELECT
        s.tx_hash,
        event_index,
        platform_fee_raw_,
        platform_event_index
    FROM
        sales_with_royalties s
        LEFT JOIN platform_fee_transfers p
        ON s.tx_hash = p.tx_hash
        AND s.event_index > p.platform_event_index
    WHERE
        p.platform_event_index IS NOT NULL qualify ROW_NUMBER() over (
            PARTITION BY s.tx_hash,
            p.platform_event_index
            ORDER BY
                s.event_index ASC
        ) = 1
),
platform_fee_filter_agg AS (
    SELECT
        tx_hash,
        event_index,
        SUM(platform_fee_raw_) AS platform_fee_raw
    FROM
        platform_fee_filter_raw
    GROUP BY
        ALL
),
nft_address_type AS (
    SELECT
        contract_address AS nft_address,
        token_transfer_type
    FROM
        {{ ref('nft__ez_nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2022-04-01'
        AND nft_address IN (
            SELECT
                nft_address
            FROM
                sales_with_royalties
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        DATEADD('hour', -12, MAX(_inserted_timestamp))
    FROM
        {{ this }}
)
AND modified_timestamp >= DATEADD('day', -7, SYSDATE())
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY nft_address
    ORDER BY
        block_timestamp ASC
) = 1
),
final_base AS (
    SELECT
        tx_hash,
        event_name,
        event_index,
        platform_address,
        nft_address,
        tokenid,
        CASE
            WHEN token_transfer_type = 'erc721_Transfer' THEN NULL
            ELSE quantity
        END AS erc1155_value,
        currency_address,
        buyer_address,
        seller_address,
        total_price_raw,
        event_index,
        creator_fee_raw,
        COALESCE(
            platform_fee_raw,
            0
        ) AS platform_fee_raw,
        block_number,
        block_timestamp,
        strategy,
        orderhash,
        ordernonce,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
    FROM
        sales_with_royalties
        LEFT JOIN platform_fee_filter_agg USING (
            tx_hash,
            event_index
        )
        LEFT JOIN nft_address_type USING (nft_address)
),
tx_data AS (
    SELECT
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature,
        tx_fee,
        input_data
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        block_timestamp :: DATE >= '2022-04-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                final_base
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
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    IFF(
        event_name = 'TakerBid',
        'sale',
        'bid_won'
    ) AS event_type,
    'joepegs' AS platform_name,
    platform_address,
    'JoepegExchange' AS platform_exchange_version,
    --uses this 0xbb01d7ad46a1229f8383f4e863abf4461b427745
    seller_address,
    buyer_address,
    nft_address,
    tokenid,
    erc1155_value,
    total_price_raw,
    creator_fee_raw,
    platform_fee_raw,
    platform_fee_raw + creator_fee_raw AS total_fees_raw,
    currency_address,
    strategy,
    orderhash,
    ordernonce,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_fee,
    input_data,
    CONCAT(
        nft_address,
        '-',
        tokenId,
        '-',
        _log_id,
        '-',
        platform_exchange_version
    ) AS nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    final_base
    INNER JOIN tx_data USING (tx_hash)
