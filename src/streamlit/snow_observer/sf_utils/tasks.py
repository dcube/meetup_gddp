"""..."""
from snowflake.snowpark.session import Session
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.column import Column as col
from snowflake.snowpark.functions import lit, concat_ws, max as sf_max, sum as sf_sum, when, last_value, to_timestamp_ntz
from snowflake.snowpark.window import Window
from sf_utils.dataframe import uppercase_all_column_names


def get_dags(session: Session) -> DataFrame:
    """ list dags on account """
    df_dags: DataFrame = uppercase_all_column_names(session.sql("SHOW TASKS IN ACCOUNT")) \
        .filter(col("PREDECESSORS") == []) \
        .withColumn(
            "DAG_FQN",
            concat_ws(
                lit("."),
                col("DATABASE_NAME"),
                col("SCHEMA_NAME"),
                col("NAME"))
            ) \
        .withColumnRenamed(col("DATABASE_NAME"), "DATABASE") \
        .withColumnRenamed(col("SCHEMA_NAME"), "SCHEMA") \
        .withColumnRenamed(col("NAME"), "DAG") \
        .withColumnRenamed(col("STATE"), "DAG_STATE") \
        .select("DAG_FQN", "DAG", "OWNER", "DAG_STATE", "SCHEDULE")
    return df_dags


def get_dags_runs(session: Session) -> DataFrame:
    """ list complete dags """
    df_dags: DataFrame = get_dags(session)
    df_dags_runs: DataFrame = session.table("SNOWFLAKE.ACCOUNT_USAGE.COMPLETE_TASK_GRAPHS") \
        .withColumn(
            "DAG_FQN",
            concat_ws(
                lit("."),
                col("DATABASE_NAME"),
                col("SCHEMA_NAME"),
                col("ROOT_TASK_NAME"))
            ) \
        .withColumnRenamed(col("DATABASE_NAME"), "DATABASE") \
        .withColumnRenamed(col("SCHEMA_NAME"), "SCHEMA") \
        .withColumnRenamed(col("ROOT_TASK_NAME"), "DAG") \
        .withColumn("LAST_RUN", last_value(to_timestamp_ntz(col("SCHEDULED_TIME")))
            .over(Window.partition_by(col("DAG_FQN")).order_by(col("SCHEDULED_TIME").desc()))) \
        .withColumn("RUN_FROM", last_value(col("SCHEDULED_FROM"))
            .over(Window.partition_by(col("DAG_FQN")).order_by(col("SCHEDULED_TIME").desc()))) \
        .withColumn("NEXT_RUN", last_value(to_timestamp_ntz(col("NEXT_SCHEDULED_TIME")))
            .over(Window.partition_by(col("DAG_FQN")).order_by(col("SCHEDULED_TIME").desc()))) \
        .withColumn("NEXT_RUN",
            when(col("NEXT_RUN") == to_timestamp_ntz(lit("1970-01-01 01:00:00")), None)
            .otherwise(col("NEXT_RUN")))

    df_dag_runs_complete = df_dags.join(
        right=df_dags_runs,
        on="DAG_FQN",
        how="LEFT",
        rsuffix="_"
    )

    # Select and transform the necessary columns
    df_dag_runs_complete = df_dag_runs_complete \
        .group_by(col("DATABASE"), col("SCHEMA"), col("DAG"), col("OWNER"), col("DAG_STATE"), col("SCHEDULE"), col("LAST_RUN"), col("RUN_FROM")) \
        .agg(
            sf_sum(when(col("STATE") == "SUCCEEDED", 1).otherwise(0)).alias("SUCCEEDED"),
            sf_sum(when(col("STATE") == "FAILED", 1).otherwise(0)).alias("FAILED"),
        )

    return df_dag_runs_complete


def get_dag_tasks_graph(session: Session, root_task_name: str) -> DataFrame:
    """get task dag"""
    return session.sql(f"""
        select
            t.name,
            t.predecessors,
            t.state,
            t.definition
        from
            table(
                information_schema.task_dependents(task_name => '{root_task_name}', recursive => TRUE)
                ) t
        """)
