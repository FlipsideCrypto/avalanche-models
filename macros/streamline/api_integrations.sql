{% macro create_aws_avalanche_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
            CREATE api integration IF NOT EXISTS aws_avalanche_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-avalanche' api_allowed_prefixes = (
                'https://87yvk5d2sf.execute-api.us-east-1.amazonaws.com/prod/',
                'https://28hv9m0ra8.execute-api.us-east-1.amazonaws.com/dev/'
            ) enabled = TRUE;   
{% endset %}
        {% do run_query(sql) %}   
    {% endif %}
{% endmacro %}