-- depends_on: {{ ref('test_silver__confirmed_blocks_recent') }}
{{ recent_missing_confirmed_txs(ref("test_silver__transactions_recent")) }}