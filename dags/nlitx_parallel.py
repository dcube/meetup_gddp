import os
from snowflake.snowpark.session import Session
from snowflake.core import Root
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation, CreateMode


if __name__ == "__main__":
    session = Session.builder.create()
    root = Root(session)

    for sch in ["TPCH_SF100", "TPCH_SF100_ICEBERG"]:
        with DAG("NLITX_PARALLEL", warehouse="LOAD") as dag:
            for idx in range(1, 23):
                # Create a task that runs some SQL.
                # read the tables definition from the json file and load data
                filepath = f"{os.environ['WORKSPACE_PATH']}/snowcli/tpch_queries"
                basename = "tpch_" + "%02d" % idx
                with open(file=f"{filepath}/{basename}.sql", mode="r", encoding="utf-8") as f:
                    sql_query = f.read()
                    task_ = DAGTask(
                        basename.upper(),
                        sql_query,
                        warehouse="LOAD"
                    )
                    dag.add_task(task_)

            schema = root.databases["MEETUP_GDDP"].schemas[sch]
            dag_op = DAGOperation(schema)
            dag_op.deploy(dag, CreateMode.or_replace)
