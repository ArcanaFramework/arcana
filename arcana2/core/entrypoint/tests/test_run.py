import os.path
from unittest.mock import Mock
import pytest
from argparse import ArgumentParser
from arcana2.core.entrypoint.run import RunAppCmd


def test_run_app(test_dicom_dataset_dir):
    parser = ArgumentParser()
    RunAppCmd.construct_parser(parser)
    args = parser.parse_args([
        'pydra.tasks.dcm2niix.Dcm2Niix',
        str(test_dicom_dataset_dir),
        '--repository', 'file_system',
        '--input', 'in_dir', 'sample-dicom', 'dicom',
        '--output', 'out_file', 'output-nifti', 'niftix_gz',
        '--dataspace', 'clinical.Clinical',
        '--hierarchy', 'session',
        '--dry_run',
        '--frequency', 'session'
        # '--ids', None,
        # '--container', None,
        # '--id_inference', None,
        # '--included', [],
        # '--excluded', [],
        # '--workflow_format', [],
        # '--app_arg', []
        ])
    pipeline = RunAppCmd().run(args)
    pipeline.workflow.pickle_task()
    pipeline(plugin='serial')