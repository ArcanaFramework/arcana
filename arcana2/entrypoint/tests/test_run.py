import os.path
from arcana2.entrypoint.run import RunAppCmd
from unittest import mock
# from pydra.mark import annotate, task


def test_run_app(test_data):
    args = mock.Mock()

    args.app = 'pydra.tasks.dcm2niix.Dcm2Niix'
    args.dataset_name = os.path.join(test_data, 'test-repo')
    args.repository = ['file_system']
    args.input = [('in_dir', 'sample-dicom', 'dicom')]
    args.output = [('out_file', 'output-nifti', 'niftix_gz',
                    'converted NIfTI file + JSON side car')]
    args.field_input = []
    args.field_output = []
    args.ids = None
    args.container = None
    args.id_inference = None
    args.included = []
    args.excluded = []
    args.required_format = []
    args.produced_format = []
    args.dimensions = 'Clinical'
    args.hierarchy = ['session']
    args.dry_run = False
    args.frequency = 'session'
    args.app_arg = []

    RunAppCmd().run(args)
