from typing import Iterable, Union, List


class DocsFixture:
    def __init__(self, yaml_src: str, markdown: str):
        self.yaml_src = yaml_src
        self.markdown = markdown


minimal_doc_spec = DocsFixture(
    """
pkg_version: &pkg_version '0.16.1'
wrapper_version: '1.10'
commands: []
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
|App version|0.16.1|
|XNAT wrapper version|1.10|

## Commands
    """.strip(),
)

yaml_constructors_join_spec = DocsFixture(
    """
pkg_version: &pkg_version '0.16.1'
wrapper_version: '1.10'
base_image: !join [ 'abc', *pkg_version ]
commands: []
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
|App version|0.16.1|
|XNAT wrapper version|1.10|
|Base image|`abc0.16.1`|

## Commands
    """.strip(),
)

yaml_constructors_concat_spec = DocsFixture(
    """
pkg_version: &pkg_version '0.16.1'
wrapper_version: '1.10'
base_image: !concat [ 'abc', *pkg_version ]
commands: []
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
|App version|0.16.1|
|XNAT wrapper version|1.10|
|Base image|`abc0.16.1`|

## Commands
    """.strip(),
)

complete_doc_spec = DocsFixture(
    """
pkg_version: &pkg_version '0.16.1'
wrapper_version: '1.10'
authors:
  - author_field
base_image: !join [ 'abc', *pkg_version ]
info_url: https://example.com
package_manager: apt
system_packages: []
python_packages:
  - name: pydra
  - name: pydra-dcm2niix
package_templates:
  - name: dcm2niix
    version: v1.0.20201102
commands:
  - name: mriqc
    version: 1a1
    workflow: arcana.tasks.bids:bids_app
    description: a description
    long_description: >-
      a longer description
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
    parameters:
    frequency: session
    configuration:
      inputs: *inputs
      outputs: *outputs
      executable: /usr/local/miniconda/bin/mriqc
      dataset: /work/bids-dataset
      app_output_dir: /work/bids-app-output
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
|App version|0.16.1|
|XNAT wrapper version|1.10|
|Base image|`abc0.16.1`|
|Info URL|https://example.com|

## Commands
### mriqc
a longer description

|Key|Value|
|---|-----|
|Short description|a description|
|Version|`1a1`|
|Executable|`/usr/local/miniconda/bin/mriqc`|
#### Inputs
|Path|Input format|Stored format|Description|
|----|------------|-------------|-----------|
|`T1w`|`medimage:NiftiGzX`|`medimage:Dicom`|T1-weighted anatomical scan|
|`T2w`|`medimage:NiftiGzX`|`medimage:Dicom`|T2-weighted anatomical scan|
|`fMRI`|`medimage:NiftiGzX`|`medimage:Dicom`|functional MRI|

#### Outputs
|Name|Output format|Stored format|Description|
|----|-------------|-------------|-----------|
|`mriqc`|`common:Directory`|`format`||
    """.strip(),
)


def all_docs_fixtures() -> Iterable[Union[DocsFixture, List[DocsFixture]]]:
    from . import docs

    for k, v in docs.__dict__.items():
        if type(v) is DocsFixture or (type(v) is list and type(v[0]) is DocsFixture):
            yield k, v
