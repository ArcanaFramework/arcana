from pathlib import Path
from pydra import mark
from arcana2.repositories.xnat.container_service import (
    generate_dockerfile, InputArg, OutputArg)


@mark.task
@mark.annotate({
    'return': {'out_file': Path}})
def concatenate(in_file1: Path, in_file2: Path, out_file: Path) -> Path:
    """Concatenates the contents of two files and writes them to a third

    Parameters
    ----------
    in_file1 : Path
        [description]
    in_file2 : Path
        [description]
    out_file : Path
        [description]

    Returns
    -------
    Path
        [description]
    """
    contents = []
    for fname in (in_file1, in_file2):
        with open(fname) as f:
            contents.append(f.read())
    with open(out_file, 'w') as f:
        f.write('\n'.join(contents))
    return out_file


def test_dockerfile(xnat_registry):
    dockerfile = generate_dockerfile(
        pydra_interface=concatenate,
        image_tag='test-concatenate',
        inputs=[],
        outputs=[],
        parameters=[],
        requirements=[],
        packages=[],
        description='A test wrapped command',
        maintainer='a.researcher@gmail.com',
        registry=xnat_registry,
        extra_labels={'an-extra-label': 'test value'})