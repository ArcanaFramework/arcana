from pathlib import Path
import json
import docker
from arcana2.data.repositories.xnat.tests.fixtures import make_mutable_dataset
from arcana2.data.spaces.clinical import Clinical
from arcana2.data.types import text
from arcana2.tasks.tests.fixtures import concatenate
from arcana2.data.repositories.xnat.cs import XnatViaCS


PIPELINE_NAME = 'test-concatenate'

def test_generate_cs(xnat_repository, xnat_container_registry, run_prefix,
                     xnat_archive_dir):

    dataset = make_mutable_dataset(xnat_repository, xnat_archive_dir,
                                  'for_concatenate.direct')

    build_dir = Path('/Users/tclose/Desktop/docker-build')  # Path(tempfile.mkdtemp())

    # image_name = f'wrap4xnat{run_prefix}'
    # image_tag = image_name + ':latest'

    image_tag = 'arcana-concatenate:latest'

    pydra_task = concatenate()

    json_config = XnatViaCS.generate_json_config(
        pipeline_name=PIPELINE_NAME,
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

    dockerfile, build_dir = XnatViaCS.generate_dockerfile(
        pydra_task=pydra_task,
        json_config=json_config,
        maintainer='some.one@an.org',
        build_dir=build_dir,
        requirements=[],
        packages=[],
        extra_labels={})

    dc = docker.from_env()
    image, build_logs = dc.images.build(path=str(build_dir), tag=image_tag)


    image_path = f'{xnat_container_registry}/{image_tag}'

    dc.images.push(image_path)


    with xnat_repository:

        xlogin = xnat_repository.login

        # Pull image from test registry to XNAT container service
        # xlogin.post('/xapi/docker/pull', json={
        #     'image': image_tag,
        #     'save-commands': True})

        # Post json config to debug xnat instead of pulling image as it isn't
        # working and since we are mounting in Docker sock (i.e. sharing the
        # outer Docker) the image is already there

        # Delete existing commands
        cmd_ids = [c['id'] for c in xlogin.get(f'/xapi/commands/').json()]
        for cmd_id in cmd_ids:
            xlogin.delete(f"/xapi/commands/{cmd_id}", accepted_status=[204])
        cmd_id = xlogin.post('/xapi/commands', json=json_config).json()

        # Enable the command globally and in the project
        xlogin.put(
            f'/xapi/commands/{cmd_id}/wrappers/{PIPELINE_NAME}/enabled')
        xlogin.put(
            f'/xapi/projects/{dataset.name}/commands/{cmd_id}/wrappers/{PIPELINE_NAME}/enabled')

        test_xsession = next(iter(xlogin.projects[dataset.name].experiments.values()))

        # Launch container
        # result = xlogin.get(
        #     f'/xapi/projects/{dataset.name}/wrappers/{cmd_id}/launch?'
        #     f'SESSION={test_xsession.id}&format=json')

        # result = xlogin.post(
        #     f"/xapi/projects/{dataset.name}/wrappers/{cmd_id}/root/SESSION/launch",
        #     json={})
        
        # commands = xlogin.get('/xapi/commands')
        # assert image_tag in commands   