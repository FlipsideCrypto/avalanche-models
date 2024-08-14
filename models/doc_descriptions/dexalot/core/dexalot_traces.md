{% docs dexalot_traces_block_no %}

The block number of this transaction.

{% enddocs %}


{% docs dexalot_traces_blocktime %}

The block timestamp of this transaction.

{% enddocs %}


{% docs dexalot_traces_call_data %}

The raw JSON data for this trace.

{% enddocs %}


{% docs dexalot_traces_from %}

The sending address of this trace. This is not necessarily the from address of the transaction. 

{% enddocs %}


{% docs dexalot_traces_gas %}

The gas supplied for this trace.

{% enddocs %}


{% docs dexalot_traces_gas_used %}

The gas used for this trace.

{% enddocs %}


{% docs dexalot_traces_identifier %}

This field represents the position and type of the trace within the transaction. 

{% enddocs %}


{% docs dexalot_trace_index %}

The index of the trace within the transaction.

{% enddocs %}


{% docs dexalot_traces_input %}

The input data for this trace.

{% enddocs %}


{% docs dexalot_traces_output %}

The output data for this trace.

{% enddocs %}


{% docs dexalot_traces_sub %}

The amount of nested sub traces for this trace.

{% enddocs %}


{% docs dexalot_traces_table_doc %}

Data is reliable starting on block 21248026. This table contains flattened trace data for internal contract calls on the Dexalot Blockchain. Hex encoded fields can be decoded to integers by using `utils.udf_hex_to_int()`.

{% enddocs %}


{% docs dexalot_traces_to %}

The receiving address of this trace. This is not necessarily the to address of the transaction. 

{% enddocs %}


{% docs dexalot_traces_tx_hash %}

The transaction hash for the trace. Please note, this is not necessarily unique in this table as transactions frequently have multiple traces. 

{% enddocs %}


{% docs dexalot_traces_type %}

The type of internal transaction. Common trace types are `CALL`, `DELEGATECALL`, and `STATICCALL`.

{% enddocs %}


{% docs dexalot_traces_value %}

The amount of ETH transferred in this trace.

{% enddocs %}


{% docs dexalot_traces_value_hex %}

The amount of ETH transferred in this trace, in hexadecimal format.

{% enddocs %}


{% docs dexalot_traces_trace_succeeded %}

The status of the trace, where TRUE = SUCCESS and FALSE = FAILED.

{% enddocs %}

{% docs dexalot_traces_error_reason %}

The reason for the trace failure, if any.

{% enddocs %}

{% docs dexalot_traces_revert_reason %}

The reason for the trace revert, if any.

{% enddocs %}

{% docs dexalot_traces_trace_index %}

The index of the trace within the transaction. 

{% enddocs %}