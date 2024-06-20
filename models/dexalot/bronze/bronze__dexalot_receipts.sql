{{ config (
    materialized = 'view'
) }}
{{ fsc_utils.streamline_external_table_query_v2(
    model = "dexalot_receipts",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER )"
) }}
