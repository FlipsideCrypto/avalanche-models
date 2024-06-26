{{ config (
    materialized = "view",
    tags = ['streamline_dexalot_complete','streamline_dexalot_blocks']
) }}

SELECT
    _id AS block_number,
    REPLACE(
        concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
        ' ',
        ''
    ) AS block_number_hex
FROM
    {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
WHERE
    _id <= (
        SELECT
            COALESCE(
                block_number,
                0
            )
        FROM
            {{ ref("streamline__get_dexalot_chainhead") }}
    )
ORDER BY
    _id ASC
