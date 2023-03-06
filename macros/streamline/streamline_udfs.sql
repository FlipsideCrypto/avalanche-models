{% macro create_udf_get_chainhead() %}
    CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead(
    ) returns variant api_integration = aws_avalanche_api AS {% if target.name == "prod" %}
        'https://87yvk5d2sf.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        'https://28hv9m0ra8.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_json_rpc() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(
        json variant
    ) returns text api_integration = aws_avalanche_api AS {% if target.name == "prod" %}
        'https://87yvk5d2sf.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_json_rpc'
    {% else %}
        'https://28hv9m0ra8.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_json_rpc'
    {%- endif %};
{% endmacro %}
