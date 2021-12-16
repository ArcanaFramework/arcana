from pathlib import Path
import json
import logging
import time
import tempfile
import docker.errors
from arcana2.data.stores.xnat.tests.fixtures import make_mutable_dataset
from arcana2.data.dimensions.clinical import Clinical
from arcana2.data.types import text
from arcana2.tasks.tests.fixtures import concatenate
from arcana2.data.stores.xnat.cs import XnatViaCS


PIPELINE_NAME = 'test-concatenate'

def test_deploy_cs_pipeline(xnat_repository, xnat_container_registry,
                            run_prefix):

    build_dir = Path(tempfile.mkdtemp())

    image_tag = f'arcana-concatenate{run_prefix}:latest'

    pipeline_name = 'detected_' + PIPELINE_NAME + run_prefix
    task_location = 'arcana2.tasks.tests.fixtures:concatenate'

    json_config = XnatViaCS.generate_json_config(
        pipeline_name=pipeline_name,
        task_location=task_location,
        image_tag=image_tag,
        inputs=[
            ('to_concat1', 'in_file1', text, Clinical.session),
            ('to_concat2', 'in_file2', text, Clinical.session)],
        outputs=[
            ('concatenated', 'out_file', text)],
        parameters=['duplicates'],
        description="A pipeline to test Arcana's wrap4xnat function",
        version='0.1',
        registry=xnat_container_registry,
        frequency=Clinical.session,
        info_url=None)

    build_dir = XnatViaCS.generate_dockerfile(
        task_location=task_location,
        json_config=json_config,
        maintainer='some.one@an.org',
        build_dir=build_dir,
        packages=[],
        python_packages=[],
        extra_labels={})

    dc = docker.from_env()
    try:
        dc.images.build(path=str(build_dir), tag=image_tag)
    except docker.errors.BuildError as e:
        logging.error(f"Error building docker file in {build_dir}")
        logging.error('\n'.join(l.get('stream', '') for l in e.build_log))
        raise

    image_path = f'{xnat_container_registry}/{image_tag}'

    dc.images.push(image_path)

    # Login to XNAT and attempt to pull the image and check the command has
    # been detected correctly
    with xnat_repository:

        xlogin = xnat_repository.login

        # Pull image from test registry to XNAT container service
        xlogin.post('/xapi/docker/pull', json={
            'image': image_tag,
            'save-commands': True})

        commands = {c['name']: c for c in xlogin.get(f'/xapi/commands/').json()}
        assert pipeline_name in commands, "Pipeline config wasn't detected automatically"
        assert json_config == commands[pipeline_name]


def test_run_cs_pipeline(xnat_repository, xnat_archive_dir,
                         xnat_container_registry, concatenate_container,
                         run_prefix):

    dataset = make_mutable_dataset(xnat_repository, xnat_archive_dir,
                                  'concatenate_test.direct')

    pipeline_name = PIPELINE_NAME + run_prefix

    json_config = XnatViaCS.generate_json_config(
        pipeline_name=pipeline_name,
        task_location='arcana2.tasks.tests.fixtures:concatenate',
        image_tag=concatenate_container,
        inputs=[
            ('in_file1', text, 'to_concat1', Clinical.session),
            ('in_file2', text, 'to_concat2', Clinical.session)],
        outputs=[
            ('out_file', text, 'concatenated')],
        parameters=['duplicates'],
        description="A pipeline to test Arcana's wrap4xnat function",
        version='0.1',
        registry=xnat_container_registry,
        frequency=Clinical.session,
        info_url=None)

    with xnat_repository:

        xlogin = xnat_repository.login

        cmd_id = xlogin.post('/xapi/commands', json=json_config).json()

        # Enable the command globally and in the project
        xlogin.put(
            f'/xapi/commands/{cmd_id}/wrappers/{pipeline_name}/enabled')
        xlogin.put(
            f'/xapi/projects/{dataset.id}/commands/{cmd_id}/wrappers/{pipeline_name}/enabled')

        test_xsession = next(iter(xlogin.projects[dataset.id].experiments.values()))

        launch_result = xlogin.post(
            f"/xapi/projects/{dataset.id}/wrappers/{cmd_id}/root/SESSION/launch",
            json={'SESSION': f'/archive/experiments/{test_xsession.id}',
                  'in_file1': 'scan1:text',
                  'in_file2': 'scan2:text',
                  'duplicates': '2'}).json()

        assert launch_result['status'] == 'success'
        workflow_id = launch_result['workflow-id']
        assert workflow_id != 'To be assigned'

        NUM_ATTEMPTS = 100
        SLEEP_PERIOD = 10
        max_runtime = NUM_ATTEMPTS * SLEEP_PERIOD

        for i in range(NUM_ATTEMPTS):
            wf_result = xlogin.get(f'/xapi/workflows/{workflow_id}').json()
            if wf_result['status'] not in ('Pending', 'Running', '_Queued'):
                break
            time.sleep(SLEEP_PERIOD)
        
        assert i != 99, f"Workflow {workflow_id} did not complete in {max_runtime}"
        assert wf_result['status'] == 'Complete'

        assert list(test_xsession.resources['out_file'].files) == ['out_file.txt']
