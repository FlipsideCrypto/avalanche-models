{{ config (
    materialized = "ephemeral"
) }}

{% set models = [ (ref('silver_observability__blocks_completeness')), 
(ref('silver_observability__logs_completeness')), 
(ref('silver_observability__traces_completeness')), 
(ref('silver_observability__transactions_completeness')), 
(ref('silver_observability__receipts_completeness'))] 
%}

SELECT
    DISTINCT block_number AS block_number
FROM
    ({% for models in models %}
    SELECT
        VALUE :: INT AS block_number
    FROM
        (
    SELECT
        blocks_impacted_array
    FROM
        {{ models }}
        qualify ROW_NUMBER() over (
    ORDER BY
        test_timestamp DESC) = 1), LATERAL FLATTEN(input => blocks_impacted_array) {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %})
