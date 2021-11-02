from arcana2.data.repositories.xnat.tests.fixtures import (
    start_xnat_container_registry, start_xnat_repository)

start_xnat_repository()
start_xnat_container_registry()
print('Successfully started XNAT repository and container registry')