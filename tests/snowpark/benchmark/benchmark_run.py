import time
import json
from snowflake.snowpark.session import Session
from snowflake.core import Root
from snowflake.core.warehouse import Warehouse
from snowflake.core.task.dagv1 import DAGOperation


def run_dag(sf_root: Root, sf_db: str, sf_schema: str,
            dag: str, wh_size: str, loop_idx: int) -> None:
    """ run dag and wait for its completion"""
    # set the execution context to run the dag and retrieve its status
    sf_root.session.use_warehouse("MANAGE")
    sf_root.session.use_database(sf_db)
    sf_root.session.use_schema(sf_schema)

    # get the dag warehouse
    dag_wh = str(sf_root.session.sql(f"show tasks like '{dag}'").collect()[0]["warehouse"])

    # get the dag warehouse current state
    wh = sf_root.warehouses[dag_wh].fetch()
    # suspend the warehouse to force cold start
    if wh.state != "SUSPENDED":
        sf_root.warehouses[dag_wh].suspend()

    # update warehouse size and stop it to be run cold start
    sf_root.warehouses[dag_wh].create_or_update(
        Warehouse(name=dag_wh, warehouse_size=wh_size, auto_suspend=60)
        )

    # submit dag run
    print(f"Dag {dag} on {sf_db}.{sf_schema} with warehouse {dag_wh} ({wh_size}) loop {loop_idx} SUBMITTED")
    start_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

    # init context for DAGOperation
    db_schema = sf_root.databases[sf_db].schemas[sf_schema]
    dag_op = DAGOperation(db_schema)
    dag_op.run(dag=dag)

    # wait for the dag run completed
    while True:
        time.sleep(10)

        df_dag_status = sf_root.session.sql(
            f"""
            SELECT ctg.STATE
            FROM
                table(information_schema.Complete_TASK_GRAPHS(ROOT_TASK_NAME => '{dag}')) ctg
                inner join
                    TABLE(
                        INFORMATION_SCHEMA.TASK_HISTORY(
                            SCHEDULED_TIME_RANGE_START => TO_TIMESTAMP_LTZ(CONVERT_TIMEZONE('GMT','Europe/Paris','{start_time}')),
                            TASK_NAME => '{dag}'
                            )
                        ) th ON TH.RUN_ID = CTG.RUN_ID
            WHERE
                TH.DATABASE_NAME = '{sf_db}'
            AND TH.SCHEMA_NAME = '{sf_schema}'
            """
            )

        if df_dag_status.count() == 1:
            break

    # suspend the warehouse
    try:
        wh = sf_root.warehouses[dag_wh].fetch()
        if wh.state != "SUSPENDED":
            sf_root.warehouses[dag_wh].suspend()
    except Exception:
        pass

    print(f"Dag {dag} on {sf_db}.{sf_schema} with warehouse {dag_wh} ({wh_size}) loop {loop_idx} ENDED")


if __name__ == "__main__":
    session = Session.builder.create()
    root = Root(session)

    with open(file="./benchmark_run.json", mode="r", encoding="utf-8") as file:
        tests_definition = json.load(file)

        # run each dag x times
        for idx in range(0, tests_definition["dag_loop"]):
            for test in tests_definition["tests"]:
                run_dag(
                    sf_root=root,
                    sf_db=test["db"],
                    sf_schema=test["schema"],
                    dag=test["dag"],
                    wh_size=test["warehouse_size"],
                    loop_idx=idx
                )
