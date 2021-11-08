from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('--mount_archive', '-m', default=False, action='store_true',
                    help="Mount XNAT archive directory into container")
parser.add_argument('--rm', action='store_true', default=False,
                    help="Remove XNAT container after it is stopped")
args = parser.parse_args()
from arcana2.data.repositories.xnat.tests.fixtures import (
    start_xnat_container_registry, start_xnat_repository)

start_xnat_repository(mount_archive=args.mount_archive, remove=args.rm)
start_xnat_container_registry()
print('Successfully started XNAT repository and container registry')