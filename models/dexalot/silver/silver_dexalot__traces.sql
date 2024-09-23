-- depends_on: {{ ref('bronze_dexalot__streamline_traces') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    full_refresh = false,
    tags = ['dexalot_non_realtime']
) }}

{{ fsc_evm.silver_traces_v1(
    full_reload_start_block = 25000000,
    full_reload_blocks = 2000000,
    schema_name = 'bronze_dexalot',
    use_partition_key = true
) }}
