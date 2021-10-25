from pathlib import Path
import tempfile
from pydra import mark
from pprint import pprint
from argparse import ArgumentParser
from arcana2.entrypoints.wrap4xnat import Wrap4XnatCmd
from arcana2.repositories.xnat.container_service import (
    generate_dockerfile, generate_json_config)
from arcana2.test_fixtures.xnat import get_mutable_dataset
from arcana2.datatypes import text
from arcana2.dataspaces.clinical import Clinical
from arcana2.test_fixtures.tasks import concatenate


def test_wrap4xnat_argparse():
    parser = ArgumentParser()
    Wrap4XnatCmd.construct_parser(parser)
    # args = parser.parse_args([])


def test_wrap4xnat(xnat_repository, xnat_container_registry, run_prefix,
                   xnat_archive_dir):

    dataset = get_mutable_dataset(xnat_repository, xnat_archive_dir,
                                  'simple.direct')

    build_dir = Path('/Users/tclose/Desktop/docker-build')  # Path(tempfile.mkdtemp())

    # image_name = f'wrap4xnat{run_prefix}'
    # image_tag = image_name + ':latest'

    image_tag = 'wrap4xnat:latest'    

    dockerfile, command_json = generate_dockerfile(
        pydra_task=concatenate(),
        image_tag=image_tag,
        inputs=[
            ('in_file1', text, Clinical.session),
            ('in_file2', text, Clinical.session)],
        outputs=[
            ('out_file', text)],
        parameters=['duplicates'],
        requirements=[],
        packages=[],
        build_dir=build_dir,
        frequency=Clinical.session,
        registry=xnat_container_registry,
        description="A container for testing arcana wrap4xnat",
        maintainer='some.one@an.org')
    
    # Wrap4XnatCmd().build(image_tag, build_dir)

    with xnat_repository:

        login = xnat_repository.login

        def f():
            login.post('/xapi/commands', json={
                'command': command_json,
                'image': image_tag})

        pprint(command_json)

        f()

        # login.post('/xapi/docker/pull', json={
        #     'image': image_tag,
        #     'save-commands': True})
        
        commands = login.get('/xapi/commands')
        assert image_name in commands   
