import tempfile
from argparse import ArgumentParser
from pathlib import Path
import logging
import docker
import xnat
from arcana.tasks.tests.fixtures import concatenate
from arcana.data.stores.xnat.cs import XnatViaCS
from arcana.data.stores.xnat.tests.fixtures import DOCKER_REGISTRY_URI
from arcana.data.spaces.medicalimaging import ClinicalTrial
from arcana.data.types.general import text

parser = ArgumentParser()
parser.add_argument('image_name',
                    help="Name of the generated docker image")
parser.add_argument('--build', default=False, action='store_true',
                    help="Build the generated pipeline container")
parser.add_argument('--xnat_server', default=None, nargs='+',
                    help="The XNAT server it will be uploaded to")
parser.add_argument('--dataset_name', default=None,
                    help="The dataset to enable the container for")
parser.add_argument('--container_registry', default=None,
                    help="Container registry host to upload built image to")
args = parser.parse_args()


build_dir = Path(tempfile.mkdtemp())

# image_name = f'wrap4xnat{run_prefix}'
# image_tag = image_name + ':latest'
if args.container_registry is not None:
    image_tag = f'{args.container_registry}/{args.image_name}:latest'
else:
    image_tag = f'{args.image_name}:latest'

pydra_task = concatenate()

pipeline_name = args.image_name.split('/')[-1]

json_config = XnatViaCS.generate_json_config(
    pipeline_name=pipeline_name,
    pydra_task=pydra_task,
    image_tag=image_tag,
    inputs=[
        ('in_file1', text, ClinicalTrial.session),
        ('in_file2', text, ClinicalTrial.session)],
    outputs=[
        ('out_file', text)],
    parameters=['duplicates'],
    description="Test wrap4xnat function",
    version='0.1',
    registry=args.container_registry,
    frequency=ClinicalTrial.session,
    info_url=None)

dockerfile, build_dir = XnatViaCS.generate_dockerfile(
    pydra_task=pydra_task,
    json_config=json_config,
    maintainer='some.one@an.org',
    build_dir=build_dir,
    requirements=[],
    packages=[],
    extra_labels={})

print(f"Created dockerfile in {build_dir}")

if args.build:
    dc = docker.from_env()
    try:
        image, build_logs = dc.images.build(path=str(build_dir), tag=image_tag)
    except docker.errors.BuildError as e:
        logging.error(f"Error building docker file in {build_dir}")
        logging.error('\n'.join(l.get('stream', '') for l in e.build_log))
        raise
    print(f"Built {image_tag} image")

    if args.container_registry is not None:
        dc.images.push(image_tag)
        print(f"Uploaded {image_tag} to {args.container_registry} registry")

if args.xnat_server is not None:
    if len(args.xnat_server) > 1:
        server, user, password = args.xnat_server
    else:
        server = args.xnat_server[0]
        user = 'admin'
        password = 'admin'

    # Enable the command globally and in the project
    with xnat.connect(server, user=user, password=password) as xlogin:

        cmd_ids = [c['id'] for c in xlogin.get(f'/xapi/commands/').json()]
        for cmd_id in cmd_ids:
            xlogin.delete(f"/xapi/commands/{cmd_id}", accepted_status=[204])
        cmd_id = xlogin.post('/xapi/commands', json=json_config).json()

        xlogin.put(
            f'/xapi/commands/{cmd_id}/wrappers/{pipeline_name}/enabled')
        if args.dataset_name:
            xlogin.put(
                f'/xapi/projects/{args.dataset_name}/commands/{cmd_id}/wrappers/{pipeline_name}/enabled')
