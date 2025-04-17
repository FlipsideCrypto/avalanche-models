{{ config(
    materialized = 'table'
) }}

SELECT 
  1 as id,
  1/0 as will_fail