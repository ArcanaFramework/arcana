from pathlib import Path
from pydra import set_input_validator

set_input_validator(True)
from pydra import Workflow  # noqa
from pydra.tasks.mrtrix3.utils import MRConvert  # noqa


wf = Workflow(name="path_mwe", input_spec=["in_file"])

wf.add(MRConvert(name="mrconvert", in_file=wf.lzin.in_file, out_file="test_dwi.mif"))

wf.set_output([("out_file", wf.mrconvert.lzout.out_file)])

wf(in_file=Path("/Users/tclose/git/workflows/arcana/test-data/nifti/T1w.nii.gz"))

wf()
