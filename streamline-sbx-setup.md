## Sandbox integration setup

In order to perform a `sandbox` `streamline` integration you need to ![register](./macros/streamline/api_integrations.sql) you `sbx api gateway` endpoint. 

### DBT Global config
- The first step is to configure your `global dbt` profile:

```zsh
# create dbtl global config
touch ~/.dbt/profiles.yaml 
```

- And add the following into `~/.dbt/profiles.yaml`

```yaml
avalanche:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: vna27887.us-east-1
      role: DBT_CLOUD_AVALANCHE 
      user: <REPLACE_WIHT_YOUR_USER>@flipsidecrypto.com
      authenticator: externalbrowser
      region: us-east-1
      database: AVALANCHE_DEV
      warehouse: DBT
      schema: silver
      threads: 12
      client_session_keep_alive: False
      query_tag: dbt_<REPLACE_WITH_YOUR_USER>_dev
```

### Register Snowflake integration and UDF's

- Register the ![snowflake integration](/macros/streamline/api_integrations.sql)

```zsh
dbt run-operation create_aws_avalanche_api --target dev
```

- Add the udf to the ![create udfs macro](./macros/create_udfs.sql)

- Invoke the udf

```zsh
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' -m 1+models/silver/streamline/core/realtime/streamline__debug_traceBlockByNumber_realtime.sql --profile avalanche --target dev   
```