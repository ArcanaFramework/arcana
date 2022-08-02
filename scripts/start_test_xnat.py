from argparse import ArgumentParser
from arcana.data.stores.medimage.xnat.tests.fixtures import (
    start_xnat_container_registry,
    start_xnat_repository,
)


parser = ArgumentParser()
parser.add_argument(
    "--xnat_root", default=None, help="Place XNAT archive directory into container"
)
parser.add_argument(
    "--rm",
    action="store_true",
    default=False,
    help="Remove XNAT container after it is stopped",
)
args = parser.parse_args()

start_xnat_repository(xnat_root_dir=args.xnat_root, remove=args.rm)
start_xnat_container_registry()
print("Successfully started XNAT repository and container registry")
