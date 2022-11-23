from __future__ import annotations
import attrs
from typing import Iterable, Union, List
from arcana.core.deploy.image.generic import ContainerImage


@attrs.define
class DocsFixture:

    yaml_src: str
    markdown: str
    licenses_to_provide: list[str] = attrs.field(factory=list)


minimal_doc_spec = DocsFixture(
    """
version: &version '0.16.1'
spec_version: '1.10'
authors:
  - name: author_name
    email: author@email.org
info_url: https://example.com
description: >-
  a test specification
command:
  task: arcana.test.tasks:identity_file
  row_frequency: session
  inputs
    - name: in_file
      format: common:Text
  outputs:
    - name: out_file
      format: common:Text
    """.strip(),
    f"""
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
|Spec version|1.10|
|Base image|`{ContainerImage.DEFAULT_BASE_IMAGE}`|
|Maintainer|author_name (author@email.org)|
|Info URL|https://example.com|

## Commands
    """.strip(),
)

yaml_constructors_join_spec = DocsFixture(
    """
version: &version '0.16.1'
spec_version: '1.10'
authors:
  - name: author_name
    email: author@email.org
base_image: !join [ 'abc', *version ]
info_url: https://example.com
description: >-
  a test of the YAML join functionality
command:
  task: arcana.test.tasks:identity_file
  row_frequency: session
  inputs
    - name: in_file
      format: common:Text
  outputs:
    - name: out_file
      format: common:Text
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
|Spec version|1.10|
|Base image|`abc0.16.1`|
|Maintainer|author_name (author@email.org)|
|Info URL|https://example.com|

## Commands
    """.strip(),
)

complete_doc_spec = DocsFixture(
    """
version: &version '0.16.1'
spec_version: '1.10'
authors:
  - name: author_name
    email: author@email.org
base_image: !join [ 'abc:', *version ]
description: a description
long_description: >-
  a longer description
known_issues:
  - url: https://example.com
info_url: https://example.com
package_manager: apt
system_packages:
  - name: vim
    version: 99.1
python_packages:
  - name: pydra
  - name: pydra-dcm2niix
package_templates:
  - name: dcm2niix
    version: v1.0.20201102
licenses:
  - name: freesurfer
    destination: /opt/freesurfer/license.txt
    description: >
      license description
    info_url: http://path.to.license.provider.org/licenses
command:
    task: arcana.tasks.bids:bids_app
    inputs: &inputs
      - name: T1w
        path: anat/T1w
        format: medimage:NiftiGzX
        stored_format: medimage:Dicom
        description: "T1-weighted anatomical scan"
      - name: T2w
        path: anat/T2w
        format: medimage:NiftiGzX
        stored_format: medimage:Dicom
        description: "T2-weighted anatomical scan"
      - name: fMRI
        path: func/task-rest_bold
        format: medimage:NiftiGzX
        stored_format: medimage:Dicom
        description: "functional MRI"
    outputs: &outputs
      - name: mriqc
        path: mriqc
        format: common:Directory
        description: "MRIQC output directory"
    parameters:
      - name: fmriprep_flags
        task_field: flags
        type: string
        description: description of flags param
    row_frequency: session
    configuration:
      inputs: *inputs
      outputs: *outputs
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
|Spec version|1.10|
|Base image|`abc:0.16.1`|
|Maintainer|author_name (author@email.org)|
|Info URL|https://example.com|
|Short description|a description|



### Required licenses
|URL|Info|
|---|----|
|`http://path.to.license.provider.org/licenses`|license description
|

## Command

|Key|Value|
|---|-----|

|Operates on|session|
|Known issues|https://example.com|
#### Inputs
|Name|Format|Description|
|----|------|-----------|
|`T1w`|<span data-toggle="tooltip" data-placement="bottom" title="dicom" aria-label="dicom">dicom (Directory)</span>|T1-weighted anatomical scan|
|`T2w`|<span data-toggle="tooltip" data-placement="bottom" title="dicom" aria-label="dicom">dicom (Directory)</span>|T2-weighted anatomical scan|
|`fMRI`|<span data-toggle="tooltip" data-placement="bottom" title="dicom" aria-label="dicom">dicom (Directory)</span>|functional MRI|

#### Outputs
|Name|Format|Description|
|----|------|-----------|
|`mriqc`|<span data-toggle="tooltip" data-placement="bottom" title="directory" aria-label="directory">directory</span>|MRIQC output directory|

#### Parameters
|Name|Data type|Description|
|----|---------|-----------|
|`fmriprep_flags`|`string`|description of flags param|

    """.strip(),
    ["freesurfer"],
)


def all_docs_fixtures() -> Iterable[Union[DocsFixture, List[DocsFixture]]]:
    from . import docs

    for k, v in docs.__dict__.items():
        if type(v) is DocsFixture or (type(v) is list and type(v[0]) is DocsFixture):
            yield k, v
