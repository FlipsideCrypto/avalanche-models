{% macro add_database_or_schema_tags() %}
    {{ set_database_tag_value('BLOCKCHAIN_NAME','AVALANCHE') }}
    {{ set_database_tag_value('BLOCKCHAIN_TYPE','EVM') }}
{% endmacro %}