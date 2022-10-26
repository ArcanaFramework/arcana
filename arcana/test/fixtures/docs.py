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
licenses:
  - source: freesurfer.txt
    destination: /opt/freesurfer/license.txt
    info: >
      license description
commands:
  - name: mriqc
    version: 1a1
    workflow: arcana.tasks.bids:bids_app
    description: a description
    long_description: >-
      a longer description
    known_issues:
      url: https://example.com
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
      - name: fmriprep_flags
        pydra_field: flags
        type: string
        description: description of flags param
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
|Base image|`abc0.16.1`|
|Info URL|https://example.com|

### Required licenses
|Source file|Info|
|-----------|----|
|`freesurfer.txt`|license description
|

## Commands
### mriqc
a longer description

|Key|Value|
|---|-----|
|Short description|a description|
|Known issues|https://example.com|
#### Inputs
|Name|Format|Description|
|----|------|-----------|
|`T1w`|<span data-toggle="tooltip" data-placement="bottom" title="medimage:Dicom" aria-label="medimage:Dicom">Dicom (Directory)</span>|T1-weighted anatomical scan|
|`T2w`|<span data-toggle="tooltip" data-placement="bottom" title="medimage:Dicom" aria-label="medimage:Dicom">Dicom (Directory)</span>|T2-weighted anatomical scan|
|`fMRI`|<span data-toggle="tooltip" data-placement="bottom" title="medimage:Dicom" aria-label="medimage:Dicom">Dicom (Directory)</span>|functional MRI|

#### Outputs
|Name|Format|Description|
|----|------|-----------|
|`mriqc`|||

#### Parameters
|Name|Data type|Description|
|----|---------|-----------|
|`fmriprep_flags`|`string`|description of flags param|
    """.strip(),
)


def all_docs_fixtures() -> Iterable[Union[DocsFixture, List[DocsFixture]]]:
    from . import docs

    for k, v in docs.__dict__.items():
        if type(v) is DocsFixture or (type(v) is list and type(v[0]) is DocsFixture):
            yield k, v
