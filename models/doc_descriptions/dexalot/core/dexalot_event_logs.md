{% docs dexalot_event_index %}

Event number within a transaction.

{% enddocs %}


{% docs dexalot_event_inputs %}

The decoded event inputs for a given event.

{% enddocs %}


{% docs dexalot_event_name %}

The decoded event name for a given event.

{% enddocs %}


{% docs dexalot_event_removed %}

Whether the event has been removed from the transaction.

{% enddocs %}


{% docs dexalot_origin_sig %}

The function signature of this transaction.

{% enddocs %}


{% docs dexalot_log_id_events %}

This is the primary key for this table. This is a concatenation of the transaction hash and the event index at which the event occurred. 

{% enddocs %}


{% docs dexalot_logs_contract_address %}

The address interacted with for a given event.

{% enddocs %}


{% docs dexalot_logs_contract_name %}

The name of the contract or token, where possible.

{% enddocs %}


{% docs dexalot_logs_data %}

The un-decoded event data.

{% enddocs %}


{% docs dexalot_logs_table_doc %}

This table contains flattened event logs from transactions on the Dexalot Subnet. Transactions may have multiple events, which are denoted by the event index for a transaction hash. Therefore, this table is unique on the combination of transaction hash and event index.

{% enddocs %}


{% docs dexalot_logs_tx_hash %}

Transaction hash is a unique 66-character identifier that is generated when a transaction is executed. This field will not be unique in this table, as a given transaction can include multiple events.

{% enddocs %}


{% docs dexalot_origin_from %}

The from address of this transaction.

{% enddocs %}


{% docs dexalot_origin_to %}

The to address of this transaction. 

{% enddocs %}


{% docs dexalot_topics %}

The un-decoded event input topics.

{% enddocs %}