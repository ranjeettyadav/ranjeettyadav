create or replace table `dmgcp-ingestion-poc.airflow_test.sample` as (select *,current_datetime as insert_time from `dmgcp-ingestion-poc.airflow_test.gcs_to_bq_table`)
