"""..."""
import os
import sys
import logging
from utils.mesh_manager import MeshManager

# Configure the logger
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))

if __name__ == "__main__":
    # get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # create a MeshManager object
    mesh_mngt = MeshManager(project_path=script_dir)

    # generate sql statements from the MeshManager object
    mesh_mngt.apply()
