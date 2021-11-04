import tempfile
from argparse import ArgumentParser
from pathlib import Path
import docker
import logging
from arcana2.tasks.tests.fixtures import concatenate
from arcana2.data.repositories.xnat.cs import XnatViaCS
from arcana2.data.repositories.xnat.tests.fixtures import DOCKER_REGISTRY_URI
from arcana2.data.spaces.clinical import Clinical
from arcana2.data.types.general import text

parser = ArgumentParser()
parser.add_argument('image_name',
                    help="Name of the generated docker image")
parser.add_argument('--container_registry', default=DOCKER_REGISTRY_URI,
                    help="Container registry host to upload built image to")
args = parser.parse_args()


build_dir = Path(tempfile.mkdtemp())

# image_name = f'wrap4xnat{run_prefix}'
# image_tag = image_name + ':latest'

image_tag = f'{args.container_registry}/{args.image_name}:latest'

pydra_task = concatenate()

json_config = XnatViaCS.generate_json_config(
    pipeline_name=args.image_name.split('/')[-1],
    pydra_task=pydra_task,
    image_tag=image_tag,
    inputs=[
        ('in_file1', text, Clinical.session),
        ('in_file2', text, Clinical.session)],
    outputs=[
        ('out_file', text)],
    parameters=['duplicates'],
    description="Test wrap4xnat function",
    version='0.1',
    registry=args.container_registry,
    frequency=Clinical.session,
    info_url=None,
    debug_output=True)

dockerfile, build_dir = XnatViaCS.generate_dockerfile(
    pydra_task=pydra_task,
    json_config=json_config,
    maintainer='some.one@an.org',
    build_dir=build_dir,
    requirements=[],
    packages=[],
    extra_labels={})

print(f"Created dockerfile in {build_dir}")

dc = docker.from_env()
try:
    image, build_logs = dc.images.build(path=str(build_dir), tag=image_tag)
except docker.errors.BuildError as e:
    logging.error(f"Error building docker file in {build_dir}")
    logging.error('\n'.join(l.get('stream', '') for l in e.build_log))
    raise

dc.images.push(image_tag)
