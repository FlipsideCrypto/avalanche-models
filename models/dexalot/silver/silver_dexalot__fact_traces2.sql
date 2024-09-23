{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number'],
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_number,tx_hash,from_address,to_address,trace_address,type,identifier), SUBSTRING(input,output,type,trace_address,identifier,error_reason,revert_reason)",
    full_refresh = false
) }}

{{ fsc_evm.gold_traces_v2(
    full_reload_start_block = 24000000,
    full_reload_blocks = 2000000,
    schema_name = 'silver_dexalot',
    full_reload_mode = true
) }}