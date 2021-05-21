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
import os
from .__about__ import __version__, __authors__
# from .analysis import (
#     Analysis, AnalysisMetaClass, MultiAnalysis, MultiAnalysisMetaClass,
    # SubCompSpec, Parameter, ParamSpec, SwitchSpec)
from .data import (
    FileGroupSpec, FileGroupMatcher, FieldSpec, FieldMatcher)
from .data.file_format import FileFormat
from .data import FileGroup, Field, FileGroupColumn, FieldColumn
# from .processor import (
#     SingleProc, MultiProc, SlurmProc)
# from .environment import StaticEnv, ModulesEnv
# Should be set explicitly in all FSL interfaces, but this squashes the warning
os.environ['FSLOUTPUTTYPE'] = 'NIFTI_GZ'
