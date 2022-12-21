from __future__ import annotations
from typing import Iterable, Union, List, Union, Dict, Tuple
import shutil
import re
import yaml
import tempfile
from pathlib import Path
import attrs
import pytest
import docker
from arcana.core.cli.deploy import (
    make_app,
    make_docs,
)
from arcana.core.utils.testing import show_cli_trace
from arcana.core.exceptions import ArcanaBuildError


def test_deploy_make_app_cli(command_spec, cli_runner, work_dir):

    DOCKER_ORG = "testorg"
    DOCKER_REGISTRY = "test.registry.org"
    IMAGE_GROUP_NAME = "testpkg"

    concatenate_spec = {
        "command": command_spec,
        "version": "1.0",
        "build_iteration": "1",
        "packages": {
            "system": ["vim"],  # just to test it out
            "pip": {"click": None},  # just to test it out
        },
        "authors": [{"name": "Some One", "email": "some.one@an.email.org"}],
        "info_url": "http://concatenate.readthefakedocs.io",
        "description": "a test image spec",
    }

    build_dir = work_dir / "build"
    build_dir.mkdir()
    spec_path = work_dir / DOCKER_ORG
    sub_dir = spec_path / IMAGE_GROUP_NAME
    sub_dir.mkdir(parents=True)
    with open(sub_dir / "concatenate.yml", "w") as f:
        yaml.dump(concatenate_spec, f)

    result = cli_runner(
        make_app,
        [
            str(spec_path),
            "common:App",
            "--build-dir",
            str(build_dir),
            "--registry",
            DOCKER_REGISTRY,
            "--loglevel",
            "warning",
            "--use-local-packages",
            "--install-extras",
            "test",
            "--raise-errors",
            "--use-test-config",
            "--dont-check-registry",
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    tag = result.output.strip().splitlines()[-1]
    assert tag == f"{DOCKER_REGISTRY}/{DOCKER_ORG}/{IMAGE_GROUP_NAME}.concatenate:1.0-1"

    # Clean up the built image
    dc = docker.from_env()
    dc.images.remove(tag)


def test_deploy_remake_app_cli(command_spec, docker_registry, cli_runner, run_prefix):
    """Tests the check to see whether"""

    IMAGE_GROUP_NAME = "testpkg-rebuild" + run_prefix

    def build_spec(spec, **kwargs):
        work_dir = Path(tempfile.mkdtemp())
        build_dir = work_dir / "build"
        build_dir.mkdir()
        spec_path = work_dir / "testorg"
        sub_dir = spec_path / IMAGE_GROUP_NAME
        sub_dir.mkdir(parents=True)
        with open(sub_dir / "concatenate.yml", "w") as f:
            yaml.dump(spec, f)

        result = cli_runner(
            make_app,
            [
                str(spec_path),
                "common:App",
                "--build-dir",
                str(build_dir),
                "--registry",
                docker_registry,
                "--loglevel",
                "warning",
                "--use-local-packages",
                "--install-extras",
                "test",
                "--raise-errors",
                "--check-registry",
                "--use-test-config",
            ],
            **kwargs,
        )
        return result

    concatenate_spec = {
        "command": command_spec,
        "version": "1.0",
        "build_iteration": "1",
        "packages": {"system": ["vim"]},
        "description": "a test image",
        "name": "test_deploy_rebuild_cli",
        "authors": [{"name": "Some One", "email": "some.one@an.email.org"}],
        "info_url": "http://concatenate.readthefakedocs.io",
    }

    # Build a basic image
    result = build_spec(concatenate_spec)
    assert result.exit_code == 0, show_cli_trace(result)
    assert result.output
    tag = result.output.strip().splitlines()[-1]
    try:
        dc = docker.from_env()
        dc.api.push(tag)

        # FIXME: Need to ensure that logs are captured properly then we can test this
        # result = build_spec(concatenate_spec)
        # assert "Skipping" in result.output

        # Modify the spec so it doesn't match the original that has just been
        # built (but don't increment the version number -> image tag so there
        # is a clash)
        concatenate_spec["packages"] = {"system": ["vim"]}

        with pytest.raises(ArcanaBuildError) as excinfo:
            build_spec(concatenate_spec, catch_exceptions=False)

        assert "doesn't match the one that was used to build the image" in str(
            excinfo.value
        )

        # Increment the version number to avoid the clash
        concatenate_spec["build_iteration"] = "2"

        result = build_spec(concatenate_spec)
        assert result.exit_code == 0, show_cli_trace(result)
        rebuilt_tag = result.output.strip().splitlines()[-1]
        dc.images.remove(rebuilt_tag)
    finally:
        # Clean up the built images
        dc.images.remove(tag)


@attrs.define
class DocsFixture:

    yaml_src: str
    markdown: str
    licenses_to_provide: list[str] = attrs.field(factory=list)


docs_fixtures = {
    "simple": DocsFixture(
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
  row_frequency: common:Samples[sample]
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
|Operates on|sample|
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
    ),
    "full": DocsFixture(
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
    task: bids:bids_app
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
    row_frequency: medimage:Clinical[session]
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
|Task|bids:bids_app|
|Operates on|session|
#### Inputs
|Name|Data type|Stored data type default|Description|
|----|---------|------------------------|-----------|
|`T1w`|<span data-toggle="tooltip" data-placement="bottom" title="niftigzx" aria-label="niftigzx">niftigzx (from 'medimage' extension)</span>|<span data-toggle="tooltip" data-placement="bottom" title="dicom" aria-label="dicom">dicom (from 'medimage' extension)</span>|T1-weighted anatomical scan|
|`T2w`|<span data-toggle="tooltip" data-placement="bottom" title="niftigzx" aria-label="niftigzx">niftigzx (from 'medimage' extension)</span>|<span data-toggle="tooltip" data-placement="bottom" title="dicom" aria-label="dicom">dicom (from 'medimage' extension)</span>|T2-weighted anatomical scan|
|`fMRI`|<span data-toggle="tooltip" data-placement="bottom" title="niftigzx" aria-label="niftigzx">niftigzx (from 'medimage' extension)</span>|<span data-toggle="tooltip" data-placement="bottom" title="dicom" aria-label="dicom">dicom (from 'medimage' extension)</span>|functional MRI|

#### Outputs
|Name|Data type|Stored data type default|Description|
|----|---------|------------------------|-----------|
|`mriqc`|<span data-toggle="tooltip" data-placement="bottom" title="directory" aria-label="directory">directory (directory)</span>|<span data-toggle="tooltip" data-placement="bottom" title="directory" aria-label="directory">directory (directory)</span>|MRIQC output directory|

#### Parameters
|Name|Data type|Description|
|----|---------|-----------|
|`fmriprep_flags`|`str`|description of flags param|
""".strip(),
        ["freesurfer"],
    ),
}


@pytest.mark.parametrize("fixture", docs_fixtures.items(), ids=lambda x: x[0])
def test_make_docs_cli(
    cli_runner, run_prefix, work_dir: Path, fixture: Tuple[str, DocsFixture]
):
    fixture_name, fixture_content = fixture

    # TODO handle multiple 'files' in a fixture
    print(f"Processing fixture: {fixture_name!r}")
    output = _make_docs(cli_runner, work_dir, fixture_content.yaml_src)

    strip_source_file_re = re.compile(r"source_file:.*")

    stripped_output = strip_source_file_re.sub("", output)
    stripped_reference = strip_source_file_re.sub("", fixture_content.markdown)

    assert (
        stripped_output == stripped_reference
    ), f"Fixture {fixture_name!r} didn't match output"


def _make_docs(
    cli_runner,
    work_dir: Path,
    docs: Union[str, Dict[str, str]],
    *args,
    flatten: bool = None,
) -> Union[str, Dict[str, str]]:
    out_dir = work_dir / "out"
    specs_dir = work_dir / "specs"
    if specs_dir.exists():
        shutil.rmtree(specs_dir)
    specs_dir.mkdir()

    if type(docs) is str:
        (specs_dir / "spec.yaml").write_text(docs)
    else:
        for name, content in docs.items():
            path = specs_dir / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)

    result = cli_runner(
        make_docs,
        [
            specs_dir.as_posix(),
            out_dir.as_posix(),
        ]
        + (["--flatten" if flatten else "--no-flatten"] if flatten is not None else [])
        + list(args),
    )

    assert result.exit_code == 0, show_cli_trace(result)

    if type(docs) is str:
        return (out_dir / "spec.md").read_text().strip()
    else:
        return {
            file.relative_to(out_dir).as_posix(): file.read_text().strip()
            for file in out_dir.glob("*.md")
        }
