{% macro create_aws_avalanche_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
            CREATE api integration IF NOT EXISTS aws_avalanche_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-avalanche' api_allowed_prefixes = (
                'https://87yvk5d2sf.execute-api.us-east-1.amazonaws.com/prod/',
                'https://28hv9m0ra8.execute-api.us-east-1.amazonaws.com/dev/'
            ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_avalanche_api_prod_v2 api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::924682671219:role/avalanche-api-prod-rolesnowflakeudfsAF733095-jBvAIUHiR70D' api_allowed_prefixes = (
            'https://94k2zsc41f.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}

    {% elif target.name == "dev" %}
        {{ log("Generating api integration for target:" ~ target.name, info=True) }}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_avalanche_api_stg_v2 api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/avalanche-api-stg-rolesnowflakeudfsAF733095-CCDPFHsmlGmu' api_allowed_prefixes = (
            'https://eb9c29d4el.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;    
        {% endset %}
        {% do run_query(sql) %}
        
    {% endif %}
{% endmacro %}
