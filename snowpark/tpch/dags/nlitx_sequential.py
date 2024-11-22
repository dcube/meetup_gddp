import os
from snowflake.snowpark.session import Session
from snowflake.core import Root
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation, CreateMode


if __name__ == "__main__":
    session = Session.builder.create()
    session.use_role("SYSADMIN")
    root = Root(session)

    for sch in ["TPCH_SF100", "TPCH_SF100_ICEBERG"]:
        with DAG("NLITX_SEQUENTIAL", warehouse="ANALYSIS") as dag:
            previous_task = None
            for idx in range(1, 23):
                # Create a task that runs some SQL.
                # read the tables definition from the json file and load data
                filepath = f"{os.environ['WORKSPACE_PATH']}/snowcli/tpch_queries"
                basename = "tpch_" + "%02d" % idx
                with open(file=f"{filepath}/{basename}.sql", mode="r", encoding="utf-8") as f:
                    sql_query = f.read()
                    task_idx = DAGTask(
                        f"SLECT_{basename.upper()}",
                        sql_query,
                        warehouse="ANALYSIS"
                    )

                    if previous_task:
                        task_idx.add_predecessors(previous_task)

                    dag.add_task(task_idx)
                    previous_task = task_idx

            schema = root.databases["MEETUP_GDDP"].schemas[sch]
            dag_op = DAGOperation(schema)
            dag_op.deploy(dag, CreateMode.or_replace)
