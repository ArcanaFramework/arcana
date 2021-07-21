from arcana2.entrypoint.run import RunAppCmd
from unittest.mock import Mock
from pydra.tasks.mrtrix3 import MRInfo

args = Mock()

args.app = MRInfo
args.input = []
args.field_input = []
args.output = []
args.field_output = []
args.ids = None
args.container = None
args.dry_run = False
args.frequency = 'session'
args.app_arg = []

RunAppCmd.run(args)
