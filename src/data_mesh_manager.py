"""..."""
import os
import sys
import logging
from snowflake.snowpark.session import Session
from dcube.snowflake.mesh.mesh_contract import MeshContract
from dcube.snowflake.mesh.query_plan import QueryPlan

# Configure the logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s.%(funcName)s - %(message)s')
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))

# Get or create snowpark session
session: Session = Session.builder.getOrCreate()


def main():
    # get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # create a MeshManager object
    mesh_ctrt: MeshContract = MeshContract(
        mesh_contract_filepath=os.path.join(script_dir, "mesh_contract.yml"))

    # generate sql statements from the MeshManager object
    qp: QueryPlan = mesh_ctrt.plan()
    qp.save_to_file(os.path.join(script_dir, "target", "compiled.sql"))
    qp.apply()

if __name__ == "__main__":
    main()
