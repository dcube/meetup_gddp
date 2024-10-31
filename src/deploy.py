"""
Script: deploy.py
Author: Frédéric BROSSARD
Last Updated: 16/09/2024
"""
import os
from datetime import datetime
import logging
import json
import yaml  # type: ignore[import-untyped]
from snowflake.snowpark import Session
from dcube.snowflake.deployer.tool import get_all_yml_files
from dcube.snowflake.deployer.infra import process_config_files


if __name__ == "__main__":
    start_time = datetime.now()
    print(f"Started at: {start_time}")

    # get script name without path and extension
    script_name = os.path.splitext(os.path.basename(__file__))[0]

    # Set the current directory to the directory of the running script
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    try:
        # Load the project configuration
        with open("project.yml", "r", encoding="utf-8") as project_file:
            project_config = yaml.safe_load(project_file)

        # create the log folder if it doesn't exists
        os.makedirs(project_config['log_path'], exist_ok=True)

        # Configure logging
        logging.basicConfig(
            filename=(
                f"{project_config['log_path']}"
                f"/{script_name}_{start_time.timestamp()}.log"
                ),
            level=project_config["log_level"],
            format=(
                "%(asctime)s - %(levelname)s - %(filename)s"
                " - %(funcName)s - %(message)s"
                )
            )

        logging.info("Deployment STARTED")

        # Create a local Snowpark session
        with Session.builder.getOrCreate() as session:
            # define session query tag
            session.query_tag = json.dumps(
                {
                    "program": os.path.basename(__file__),
                    "run": int(datetime.timestamp(start_time))
                }
            )

            # deploy infra
            process_config_files(
                session,
                get_all_yml_files(project_config["config_infra_path"]))

            # close the session
            session.close()

            logging.info("Deployment SUCCEEDED")

    except Exception as err:
        print(f"Failed with error {err}")
        logging.error("Deployment FAILED with %s", err)

    end_time = datetime.now()
    duration = end_time-start_time
    print(f"Ended at: {end_time}")
    print(f"Total duration: {duration} secs")
