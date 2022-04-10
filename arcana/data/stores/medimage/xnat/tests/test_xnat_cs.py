from pathlib import Path
import logging
import time
import tempfile
import pytest
import docker.errors
from arcana.data.spaces.medimage import Clinical
from arcana.data.formats import Text
from arcana.deploy.medimage import build_xnat_cs_image, generate_xnat_cs_command
from arcana.test.fixtures.medimage import make_mutable_dataset


PIPELINE_NAME = 'test-concatenate'

def test_xnat_cs_pipeline(xnat_repository, xnat_archive_dir,
                          command_spec, run_prefix):

    dataset = make_mutable_dataset(xnat_repository, xnat_archive_dir,
                                  'concatenate_test.direct')

    # Append run_prefix to command name to avoid clash with previous test runs
    
    IMAGE_TAG = 'arcana-test-xnat-cs'

    # build_xnat_cs_image(
    #     image_tag=IMAGE_TAG,
    #     commands=[],
    #     authors=['some.one@an.org'],
    #     info_url='http://concatenate.readthefakedocs.io',
    #     system_packages=[],
    #     python_packages=[],
    #     readme='This is a test README',
    #     docker_registry='a.docker.registry.io',
    #     use_local_packages=True,
    #     arcana_install_extras=['test'])

    with xnat_repository:

        xlogin = xnat_repository.login

        # We manually set the command in the test XNAT instance as commands are
        # loaded from images when they are pulled from a registry and we use
        # the fact that the container service test XNAT instance shares the
        # outer Docker socket. Since we build the pipeline image with the same
        # socket there is no need to pull it.
        cmd_name = command_spec['name'] = 'xnat-cs-test' + run_prefix
        xnat_command = generate_xnat_cs_command(image_tag=IMAGE_TAG,
                                                **command_spec)
        cmd_id = xlogin.post('/xapi/commands', json=xnat_command).json()

        # Enable the command globally and in the project
        xlogin.put(
            f"/xapi/commands/{cmd_id}/wrappers/{cmd_name}/enabled")
        xlogin.put(
            f"/xapi/projects/{dataset.id}/commands/{cmd_id}/wrappers/{cmd_name}/enabled")

        test_xsession = next(iter(xlogin.projects[dataset.id].experiments.values()))

        launch_result = xlogin.post(
            f"/xapi/projects/{dataset.id}/wrappers/{cmd_id}/root/SESSION/launch",
            json={'SESSION': f'/archive/experiments/{test_xsession.id}',
                  'to_concat1': 'scan1:Text',
                  'to_concat2': 'scan2:Text',
                  'duplicates': '2'}).json()

        assert launch_result['status'] == 'success'
        workflow_id = launch_result['workflow-id']
        assert workflow_id != 'To be assigned'

        NUM_ATTEMPTS = 100
        SLEEP_PERIOD = 10
        max_runtime = NUM_ATTEMPTS * SLEEP_PERIOD

        INCOMPLETE_STATES = ('Pending', 'Running', '_Queued', 'Staging',
                             'Finalizing', 'Created')

        for i in range(NUM_ATTEMPTS):
            wf_result = xlogin.get(f'/xapi/workflows/{workflow_id}').json()
            if wf_result['status'] not in INCOMPLETE_STATES:
                break
            time.sleep(SLEEP_PERIOD)
        
        assert i != 99, f"Workflow {workflow_id} did not complete in {max_runtime}"
        assert wf_result['status'] == 'Complete'

        assert list(test_xsession.resources['concatenated'].files) == ['concatenated.txt']
