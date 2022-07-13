{% macro run_sp_create_prod_community_clone() %}
    {% set clone_query %}
    call avalanche._internal.create_prod_clone(
        'avalanche',
        'avalanche_community_dev',
        'flipside_community_curator'
    );
    {% endset %}
    {% do run_query(clone_query) %}
{% endmacro %}
