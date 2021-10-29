from pathlib import Path
import json
import docker
from xnat.exceptions import XNATError
from arcana2.entrypoints.wrap4xnat import Wrap4XnatCmd
from arcana2.test_fixtures.xnat.xnat import get_mutable_dataset
from arcana2.dataspaces.clinical import Clinical
from arcana2.datatypes import text
from arcana2.test_fixtures.tasks import concatenate
from arcana2.repositories.xnat.cs import (
    generate_dockerfile, generate_json_config)


def test_generate_cs(xnat_repository, xnat_container_registry, run_prefix,
                     xnat_archive_dir):

    dataset = get_mutable_dataset(xnat_repository, xnat_archive_dir,
                                  'simple.direct')

    build_dir = Path('/Users/tclose/Desktop/docker-build')  # Path(tempfile.mkdtemp())

    # image_name = f'wrap4xnat{run_prefix}'
    # image_tag = image_name + ':latest'

    image_tag = 'arcana-concatenate:latest'

    pydra_task = concatenate()

    json_config = generate_json_config(
        pipeline_name='test-concatenate',
        pydra_task=pydra_task,
        image_tag=image_tag,
        inputs=[
            ('in_file1', text, Clinical.session),
            ('in_file2', text, Clinical.session)],
        outputs=[
            ('out_file', text)],
        parameters=['duplicates'],
        description="A pipeline to test Arcana's wrap4xnat function",
        version='0.1',
        registry=xnat_container_registry,
        frequency=Clinical.session,
        info_url=None,
        debug_output=True)

    with open('/Users/tclose/Desktop/test-command-json.json', 'w') as f:
        json.dump(json_config, f, indent='    ', sort_keys=True)

    # with open('/Users/tclose/Desktop/dcm2niix-command.json') as f:
    #     dcm2niix_config = json.load(f)

    # dcm2niix_config['name'] = dcm2niix_config['label'] = 'dcm2niix' + run_prefix

    dockerfile, build_dir = generate_dockerfile(
        pydra_task=pydra_task,
        json_config=json_config,
        maintainer='some.one@an.org',
        build_dir=build_dir,
        requirements=[],
        packages=[],
        extra_labels={})

    # dc = docker.from_env()
    # dc.images.build(path=str(build_dir), tag=image_tag)

    # image_path = f'{xnat_container_registry}/{image_tag}'

    # dc.images.push(image_path)


    with xnat_repository:

        # Pull image from test registry to XNAT container service
        # xnat_repository.login.post('/xapi/docker/pull', json={
        #     'image': image_tag,
        #     'save-commands': True})

        # Post json config to debug xnat instead of pulling image as it isn't
        # working and since we are mounting in Docker sock (i.e. sharing the
        # outer Docker) the image is already there
        try:
            xnat_repository.login.delete('/xapi/commands/1')
        except XNATError:
            pass
        result = xnat_repository.login.post('/xapi/commands', json=json_config)
        
        commands = xnat_repository.login.get('/xapi/commands')
        assert image_tag in commands   