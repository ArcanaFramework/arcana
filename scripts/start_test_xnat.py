from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('--internal_archive', default=False, action='store_true',
                    help="Don't mount XNAT archive directory into container")
parser.add_argument('--keep', action='store_true', default=False,
                    help="Don't remove XNAT container after it is stopped")
args = parser.parse_args()
from arcana2.data.repositories.xnat.tests.fixtures import (
    start_xnat_container_registry, start_xnat_repository)

start_xnat_repository(mount_archive=not args.internal_archive,
                      remove=not args.keep)
start_xnat_container_registry()
print('Successfully started XNAT repository and container registry')