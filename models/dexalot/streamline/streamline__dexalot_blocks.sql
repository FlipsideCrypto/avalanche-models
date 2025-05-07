{{ config (
    materialized = "view",
    tags = ['streamline_dexalot_complete']
) }}

SELECT
    _id AS block_number,
    REPLACE(
        concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
        ' ',
        ''
    ) AS block_number_hex
FROM
    {{ ref('admin__number_sequence') }}
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
    and _id > 21248026
ORDER BY
    _id ASC
