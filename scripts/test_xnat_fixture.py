from arcana2.test_fixtures.xnat import (
    xnat_repository, xnat_container_registry, xnat_archive_dir)


repo = xnat_repository(xnat_archive_dir())

registry = xnat_container_registry(next(repo))

print(next(registry))

try:
    next(registry)
except StopIteration:
    pass

try:
    next(repo)
except StopIteration:
    pass
