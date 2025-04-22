{% macro log_run_metrics() %}
    {% set run_command = var("invocation_command", "unknown") %}
    {% set gha_run_id = var("github_run_id", "unknown") %}
    {% set gha_run_url = var("github_run_url", "unknown") %}
    {% set gha_workflow = var("github_workflow", "unknown") %}
    {% set gha_ref = var("github_ref", "unknown") %}
    {% set gha_actor = var("github_actor", "unknown") %}
    {% set run_vars = tojson(var("vars", {})) %}

    INSERT INTO {{ target.database }}.{{ target.schema }}.run_metrics (
        invocation_id,
        elapsed_time,
        invocation_command,
        vars,
        gha_run_id,
        gha_run_url,
        gha_workflow,
        gha_ref,
        gha_actor,
        logged_at
    )
    SELECT
        '{{ invocation_id }}' AS invocation_id,
        DATEDIFF('second', '{{ run_started_at }}'::timestamp, CURRENT_TIMESTAMP) AS elapsed_time,
        '{{ run_command }}' AS invocation_command,
        PARSE_JSON('{{ run_vars }}') AS vars,
        '{{ gha_run_id }}' AS gha_run_id,
        '{{ gha_run_url }}' AS gha_run_url,
        '{{ gha_workflow }}' AS gha_workflow,
        '{{ gha_ref }}' AS gha_ref,
        '{{ gha_actor }}' AS gha_actor,
        CURRENT_TIMESTAMP AS logged_at;
{% endmacro %}