from __future__ import annotations
import attrs
from typing import Iterable, Union, List


@attrs.define
class DocsFixture:

    yaml_src: str
    markdown: str
    licenses_to_provide: list[str] = attrs.field(factory=list)


# minimal_doc_spec = DocsFixture(
#     """
# version: &version '0.16.1'
# build_iteration: '9'
# authors:
#   - name: author_name
#     email: author@email.org
# info_url: https://example.com
# description: >-
#   a test specification
# command:
#   task: arcana.core.utils.testing.tasks:identity_file
#   row_frequency: session
#   inputs:
#     in_file:
#       datatype: common:Text
#       help_string: the input file
#   outputs:
#     out_file:
#       datatype: common:Text
#       help_string: the output file
#     """.strip(),
#     f"""
# ---
# source_file: spec.yaml
# title: spec
# weight: 10

# ---

# ## Package Info
# |Key|Value|
# |---|-----|
# |Name|spec|
# |App version|0.16.1|
# |Build iteration|9|
# |Base image|`{BaseImage().reference}`|
# |Maintainer|author_name (author@email.org)|
# |Info URL|https://example.com|
# |Short description|a test specification|

# a test specification

# ## Command
# |Key|Value|
# |---|-----|
# |Task|arcana.core.utils.testing.tasks:identity_file|
# |Operates on|session|
# #### Inputs
# |Name|Data type|Stored data type default|Description|
# |----|---------|------------------------|-----------|
# |`in_file`|<span data-toggle="tooltip" data-placement="bottom" title="text" aria-label="text">text (`.txt`)</span>|<span data-toggle="tooltip" data-placement="bottom" title="text" aria-label="text">text (`.txt`)</span>|the input file|

# #### Outputs
# |Name|Data type|Stored data type default|Description|
# |----|---------|------------------------|-----------|
# |`out_file`|<span data-toggle="tooltip" data-placement="bottom" title="text" aria-label="text">text (`.txt`)</span>|<span data-toggle="tooltip" data-placement="bottom" title="text" aria-label="text">text (`.txt`)</span>|the output file|

# #### Parameters
# |Name|Data type|Description|
# |----|---------|-----------|
# """.strip(),
# )

minimal_doc_spec = DocsFixture(
    """
version: &version '0.16.1'
authors:
  - name: author_name
    email: author@email.org
base_image:
  name: abc
  tag: *version
  package_manager: apt
info_url: https://example.com
description: >-
  a test of the YAML join functionality
command:
  task: arcana.core.utils.testing.tasks:identity_file
  row_frequency: session
  inputs:
    in_file:
      datatype: common:Text
      help_string: the input file
  outputs:
    out_file:
      datatype: common:Text
      help_string: the output file
    """.strip(),
    """
---
source_file: spec.yaml
title: spec
weight: 10

---

## Package Info
|Key|Value|
|---|-----|
|Name|spec|
|App version|0.16.1|
|Build iteration|0|
|Base image|`abc:0.16.1`|
|Maintainer|author_name (author@email.org)|
|Info URL|https://example.com|
|Short description|a test of the YAML join functionality|

a test of the YAML join functionality

## Command
|Key|Value|
|---|-----|
|Task|arcana.core.utils.testing.tasks:identity_file|
|Operates on|session|
#### Inputs
|Name|Data type|Stored data type default|Description|
|----|---------|------------------------|-----------|
|`in_file`|<span data-toggle="tooltip" data-placement="bottom" title="text" aria-label="text">text (`.txt`)</span>|<span data-toggle="tooltip" data-placement="bottom" title="text" aria-label="text">text (`.txt`)</span>|the input file|

#### Outputs
|Name|Data type|Stored data type default|Description|
|----|---------|------------------------|-----------|
|`out_file`|<span data-toggle="tooltip" data-placement="bottom" title="text" aria-label="text">text (`.txt`)</span>|<span data-toggle="tooltip" data-placement="bottom" title="text" aria-label="text">text (`.txt`)</span>|the output file|

#### Parameters
|Name|Data type|Description|
|----|---------|-----------|
""".strip(),
)

complete_doc_spec = DocsFixture(
    """
version: &version '0.16.1'
build_iteration: '10'
authors:
  - name: author_name
    email: author@email.org
base_image:
  name: abc
  tag: *version
  package_manager: yum
description: a description
long_description: >-
  a longer description
known_issues:
  - url: https://example.com
info_url: https://example.com
packages:
  system:
    vim: 99.1
    git:
  pip:
    - pydra
    - pydra-dcm2niix
  neurodocker:
    dcm2niix: v1.0.20201102
licenses:
  freesurfer:
    destination: /opt/freesurfer/license.txt
    description: >
      license description
    info_url: http://path.to.license.provider.org/licenses
command:
    task: arcana.tasks.bids:bids_app
    inputs:
      T1w:
        configuration:
          path: anat/T1w
        datatype: medimage:NiftiGzX
        help_string: "T1-weighted anatomical scan"
        default_column:
          datatype: medimage:Dicom
      T2w:
        configuration:
          path: anat/T2w
        datatype: medimage:NiftiGzX
        help_string: "T2-weighted anatomical scan"
        default_column:
          datatype: medimage:Dicom
      fMRI:
        datatype: medimage:NiftiGzX
        help_string: "functional MRI"
        configuration:
          path: func/task-rest_bold
        default_column:
          datatype: medimage:Dicom
    outputs:
      mriqc:
        datatype: common:Directory
        help_string: "MRIQC output directory"
        configuration:
          path: mriqc
    parameters:
      fmriprep_flags:
        field: flags
        datatype: str
        help_string: description of flags param
    row_frequency: session
    configuration:
      executable: /usr/local/miniconda/bin/mriqc
      dataset: /work/bids-dataset
      app_output_dir: /work/bids-app-output
    """.strip(),
    """
---
source_file: /var/folders/mz/yn83q2fd3s758w1j75d2nnw80000gn/T/tmp47_dxmyq/specs/spec.yaml
title: spec
weight: 10

---

## Package Info
|Key|Value|
|---|-----|
|Name|spec|
|App version|0.16.1|
|Build iteration|10|
|Base image|`abc:0.16.1`|
|Maintainer|author_name (author@email.org)|
|Info URL|https://example.com|
|Short description|a description|
|Known issues|https://example.com|

a longer description

### Required licenses
|Name|URL|Description|
|----|---|-----------|
|freesurfer|`http://path.to.license.provider.org/licenses`|license description|

## Command
|Key|Value|
|---|-----|
|Task|arcana.tasks.bids.app:bids_app|
|Operates on|session|
#### Inputs
|Name|Data type|Stored data type default|Description|
|----|---------|------------------------|-----------|
|`T1w`|<span data-toggle="tooltip" data-placement="bottom" title="niftigzx" aria-label="niftigzx">niftigzx (`.nii.gz`)</span>|<span data-toggle="tooltip" data-placement="bottom" title="dicom" aria-label="dicom">dicom (Directory)</span>|T1-weighted anatomical scan|
|`T2w`|<span data-toggle="tooltip" data-placement="bottom" title="niftigzx" aria-label="niftigzx">niftigzx (`.nii.gz`)</span>|<span data-toggle="tooltip" data-placement="bottom" title="dicom" aria-label="dicom">dicom (Directory)</span>|T2-weighted anatomical scan|
|`fMRI`|<span data-toggle="tooltip" data-placement="bottom" title="niftigzx" aria-label="niftigzx">niftigzx (`.nii.gz`)</span>|<span data-toggle="tooltip" data-placement="bottom" title="dicom" aria-label="dicom">dicom (Directory)</span>|functional MRI|

#### Outputs
|Name|Data type|Stored data type default|Description|
|----|---------|------------------------|-----------|
|`mriqc`|<span data-toggle="tooltip" data-placement="bottom" title="directory" aria-label="directory">directory</span>|<span data-toggle="tooltip" data-placement="bottom" title="directory" aria-label="directory">directory</span>|MRIQC output directory|

#### Parameters
|Name|Data type|Description|
|----|---------|-----------|
|`fmriprep_flags`|`str`|description of flags param|
""".strip(),
    ["freesurfer"],
)


def all_docs_fixtures() -> Iterable[Union[DocsFixture, List[DocsFixture]]]:
    from . import docs

    for k, v in docs.__dict__.items():
        if type(v) is DocsFixture or (type(v) is list and type(v[0]) is DocsFixture):
            yield k, v
