# from arcana2.entrypoint.run import RunAppCmd
# from unittest.mock import Mock
# # from pydra.mark import annotate, task

# # @task
# # @annotate({
# #     'x': int,
# #     'y': float,
# #     'qualifier': str,
# #     'negation': bool,
# #     'return': {
# #         'z': float,
# #         'msg': str}})
# # def test_task(x, y, qualifier, negation):
# #     z = x + 5 * y
# #     msg = f"The answer to the question was "
# #     if negation:
# #         msg += 'not '
# #     msg += f"a {qualifier} {z}"
# #     return z, msg


# args = Mock()

# args.app = 'pydra.tasks.dcm2niix.Dcm2Niix'
# args.repository = [
#     'file_system', '/Users/tclose/Data/test-arcana2/test-repo', 'subject']
# args.input = [('sample-dicom', 'dicom')]
# args.field_input = []
# args.output = [('output-nifti', 'niftix_gz')]
# args.field_output = []
# args.ids = None
# args.container = None
# args.dry_run = False
# args.frequency = 'session'
# args.app_arg = []

# RunAppCmd().run(args)
