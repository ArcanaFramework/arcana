import os.path
from arcana2.entrypoint.run import RunAppCmd
from unittest import mock, TestCase
from unittest.mock import Mock
# from pydra.mark import annotate, task

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.__file__), '..', '..',
                             'data')

# @task
# @annotate({
#     'x': int,
#     'y': float,
#     'qualifier': str,
#     'negation': bool,
#     'return': {
#         'z': float,
#         'msg': str}})
# def test_task(x, y, qualifier, negation):
#     z = x + 5 * y
#     msg = f"The answer to the question was "
#     if negation:
#         msg += 'not '
#     msg += f"a {qualifier} {z}"
#     return z, msg

class TestRunApp(TestCase):

    def test_run_app(self):
        args = mock.Mock()

        args.app = 'pydra.tasks.dcm2niix.Dcm2Niix'
        args.dataset_name = os.path.join(TEST_DATA_DIR, 'test-repo')
        args.repository = ['file_system']
        args.input = [('in_dir', 'sample-dicom', 'dicom')]
        args.output = [('out_file', 'output-nifti', 'niftix_gz')]
        args.field_input = []
        args.field_output = []
        args.ids = None
        args.container = None
        args.id_inference = None
        args.included = []
        args.excluded = []
        args.data_structure = 'ClinicalTrial'
        args.dry_run = False
        args.frequency = 'session'
        args.app_arg = []

        RunAppCmd().run(args)
