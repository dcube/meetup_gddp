-- ----------------------------------------------------------------------
-- monitor the Dags
-- ----------------------------------------------------------------------

use role sysadmin;

-- use system view from Snowflake.account_usage
-- to get dags and tasks history runs and metrics
create or replace table meetup_gddp.utils.dag_run_monitor as
with cte as
 (
    select
        th.run_id,
        dense_rank() over (order by th.run_id) as dag_run_number,
        min(th.query_start_time) over (partition by th.run_id) as dag_start_time,
        max(th.completed_time) over (partition by th.run_id) as dag_end_time,
        datediff('s', dag_start_time, dag_end_time) as dag_duration_s,
        tv.name as dag_name,
        split(dag_name,'_')[0]::string as workload_type,
        split(dag_name,'_')[1]::string as run_mode,
        th.root_task_id,
        th.scheduled_from,
        th.name as task_name,
        replace(th.name, tv.name || '$', '') as task_subname,
        th.database_name,
        th.schema_name,
        case
            when th.schema_name = 'TPCH_SF100_ICEBERG' then 'ICEBERG'
            else 'NATIVE'
            end as data_format,
        first_value(qh.warehouse_size) ignore nulls
            over ( partition by th.run_id order by th.query_start_time) as dag_warehouse_size,
        th.query_id,
        th.query_text,
        th.query_start_time as task_start_time,
        th.completed_time as task_completed_time,
        th.state,
        qah.warehouse_name,
        qh.warehouse_size,
        case warehouse_size
            when 'Medium'   then 1
            when 'Large'    then 2
            when 'X-Large'  then 3
            else null
            end::int as warehouse_size_order,
        coalesce(credits_attributed_compute,0)
            + coalesce(credits_used_query_acceleration,0)
                as total_credits,
        qh.compilation_time/1000 as compilation_time_s,
        qh.queued_provisioning_time/1000 as queued_provisioning_time_s,
        qh.queued_overload_time/1000 as queued_overload_time_s,
        qh.total_elapsed_time/1000 as total_elapsed_time_s,
        qh.partitions_scanned,
        qh.partitions_total,
        qh.bytes_spilled_to_local_storage,
        qh.bytes_spilled_to_remote_storage,
        qh.rows_produced,
        qh.rows_inserted,
        qh.rows_updated,
        qh.rows_deleted,
        qh.rows_unloaded,
        qh.rows_written_to_result
    from
        snowflake.account_usage.task_history as th
        inner join snowflake.account_usage.task_versions as tv
            on tv.id = th.root_task_id
            and tv.graph_version = th.graph_version
        left join snowflake.account_usage.query_attribution_history as qah
            on qah.query_id = th.query_id
        inner join snowflake.account_usage.query_history as qh
            on qh.query_id = th.query_id
    where
        th.database_name = 'MEETUP_GDDP'
    and th.query_start_time >= '2024-11-22 02:44:50'
)

select
    *,
    dense_rank() over (
        partition by dag_name, data_format
        order by dag_warehouse_size
        ) as test_case_number
from
    cte;


-- Create dynamic table to get dags metrics
create or replace dynamic table MEETUP_GDDP.UTILS.DAG_RUN_STATS
target_lag = 'DOWNSTREAM'
warehouse = 'MANAGE'
refresh_mode = AUTO
initialize = ON_CREATE
 as
select
    dag_name,
    workload_type,
    run_mode,
    data_format,
    warehouse_size,
    warehouse_size_order,
    run_id,
    dag_duration_s,
    count(*) as dag_runs,
    sum(total_credits) as dag_credits,
    -- dag_credits * 2.4 as cost_standard,
    -- dag_credits * 3.9 as cost_enterprise,
    -- dag_credits * 5.2 as cost_business_critical,
    sum(rows_produced) as rows_produced,
    sum(partitions_scanned) as partitions_scanned,
    sum(partitions_total) as partitions_total,
from meetup_gddp.utils.dag_run_monitor
where warehouse_size is not null
group by all
;

select
    run_id,
    dag_run_number,
    dag_name,
    workload_type,
    run_mode,
    task_subname,
    duration_s,
    total_credits,
    compilation_time_s,
    queued_provisioning_time_s,
    queued_overload_time_s,
    total_elapsed_time_s,
    partitions_scanned,
    partitions_total,
    rows_produced
from meetup_gddp.utils.dag_run_monitor
where total_credits > 0;
