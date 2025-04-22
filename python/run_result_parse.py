import json
from pathlib import Path
import snowflake.connector
from pprint import pprint
import os


def parse_model_results(data):
    invocation_id = data["metadata"].get("invocation_id", "unknown")
    results = data.get("results", [])

    parsed_models = []

    for result in results:
        relation_name = result.get("relation_name", "")
        parts = relation_name.split(".")
        database, schema, model_name = (parts + [None, None, None])[0:3]

        parsed = {
            "invocation_id": invocation_id,
            "execution_time": result.get("execution_time"),
            "rows_affected": result.get("adapter_response", {}).get("rows_affected"),
            "database": database,
            "schema": schema,
            "model_name": model_name,
        }

        parsed_models.append(parsed)

    return parsed_models


def parse_run_metadata(data):
    meta = data.get("metadata", {})
    args = meta.get("args", {})

    # Pull GHA context from environment variables
    gha_run_id = os.getenv("GITHUB_RUN_ID", "unknown")
    gha_repo = os.getenv("GITHUB_REPOSITORY", "")
    gha_run_url = f"https://github.com/{gha_repo}/actions/runs/{gha_run_id}" if gha_repo else "unknown"
    gha_workflow = os.getenv("GITHUB_WORKFLOW", "unknown")
    gha_ref = os.getenv("GITHUB_REF", "unknown")
    gha_actor = os.getenv("GITHUB_ACTOR", "unknown")

    return {
        "invocation_id": meta.get("invocation_id", "unknown"),
        "elapsed_time": data.get("elapsed_time"),
        "invocation_command": args.get("invocation_command", ""),
        "vars": meta.get("vars", {}),
        "gha_run_id": gha_run_id,
        "gha_run_url": gha_run_url,
        "gha_workflow": gha_workflow,
        "gha_ref": gha_ref,
        "gha_actor": gha_actor,
    }


def upload_to_snowflake(model_rows, run_meta, snowflake_config):
    print("🔌 Connecting to Snowflake...")
    conn = snowflake.connector.connect(**snowflake_config)
    cur = conn.cursor()

    print("📤 Inserting model-level metrics...")
    for row in model_rows:
        cur.execute("""
            INSERT INTO monitoring.model_metrics
            (invocation_id, model_name, database, schema, execution_time, rows_affected)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row['invocation_id'],
            row['model_name'],
            row['database'],
            row['schema'],
            row['execution_time'],
            row['rows_affected']
        ))

    print("📤 Inserting run-level summary...")

    if run_meta['vars']:
        cur.execute("""
            INSERT INTO monitoring.run_metrics
            (invocation_id, elapsed_time, invocation_command, vars, gha_run_id, gha_run_url, gha_workflow, gha_ref, gha_actor)
            VALUES (%s, %s, %s, PARSE_JSON(%s), %s, %s, %s, %s, %s)
        """, (
            run_meta['invocation_id'],
            run_meta['elapsed_time'],
            run_meta['invocation_command'],
            json.dumps(run_meta['vars']),
            run_meta['gha_run_id'],
            run_meta['gha_run_url'],
            run_meta['gha_workflow'],
            run_meta['gha_ref'],
            run_meta['gha_actor']
        ))
    else:
        cur.execute("""
            INSERT INTO monitoring.run_metrics
            (invocation_id, elapsed_time, invocation_command, vars, gha_run_id, gha_run_url, gha_workflow, gha_ref, gha_actor)
            VALUES (%s, %s, %s, NULL, %s, %s, %s, %s, %s)
        """, (
            run_meta['invocation_id'],
            run_meta['elapsed_time'],
            run_meta['invocation_command'],
            run_meta['gha_run_id'],
            run_meta['gha_run_url'],
            run_meta['gha_workflow'],
            run_meta['gha_ref'],
            run_meta['gha_actor']
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Done uploading to Snowflake.")


def main(filepath="target/run_results.json"):
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Couldn't find: {filepath}")

    with open(filepath, "r") as f:
        data = json.load(f)

    model_results = parse_model_results(data)
    run_metadata = parse_run_metadata(data)

    print("\n📦 Parsed model-level metrics:")
    pprint(model_results, sort_dicts=False)

    print("\n📋 Parsed run-level metadata:")
    pprint(run_metadata, sort_dicts=False)

    return model_results, run_metadata

if __name__ == "__main__":
    # You can also read these from environment variables or a config file
    snowflake_config = {
        "user": os.getenv("SNOWFLAKE_USER", "mattromano@flipsidecrypto.com"),
        "password": os.getenv("SNOWFLAKE_PASSWORD", "XXXX"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT", "XXXXX"),  # without .snowflakecomputing.com
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "DBT"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "avalanche_dev"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "monitoring"),
        "role": os.getenv("SNOWFLAKE_ROLE", "INTERNAL_DEV"),
    }

    model_results, run_metadata = main()
    upload_to_snowflake(model_results, run_metadata, snowflake_config)