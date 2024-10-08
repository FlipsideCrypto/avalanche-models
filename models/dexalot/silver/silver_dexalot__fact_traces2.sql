{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number'],
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_number,tx_hash,from_address,to_address,trace_address,type), SUBSTRING(input,output,type,trace_address,error_reason,revert_reason)",
    full_refresh = false,
    tags = ['dexalot_non_realtime']
) }}

{{ fsc_evm.gold_traces_v2(
    full_reload_start_block = 25000000,
    full_reload_blocks = 2000000,
    schema_name = 'silver_dexalot',
    uses_tx_status = true
) }}