"""
Arcana

Copyright (c) 2012-2018 Thomas G. Close, Monash Biomedical Imaging,
Monash University, Melbourne, Australia

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

PACKAGE_NAME = "arcana"
CODE_URL = f"https://github.com/australian-imaging-service/{PACKAGE_NAME}"

__authors__ = [("Thomas G. Close", "tom.g.close@gmail.com")]

from ._version import __version__

from pydra import set_input_validator

set_input_validator(True)
# from .core.data.set import Dataset
# from .core.data.store import DataStore

# Should be set explicitly in all FSL interfaces, but this squashes the warning
# os.environ['FSLOUTPUTTYPE'] = 'NIFTI_GZ'
