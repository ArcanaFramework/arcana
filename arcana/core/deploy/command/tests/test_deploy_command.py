from functools import reduce
from operator import mul
import pytest
from arcana.core.utils.testing import make_dataset_locator
from arcana.core.utils.testing.data import (
    make_dataset,
    TestDatasetBlueprint,
    TestDataSpace,
)
from arcana.common.data import Text
from arcana.core.deploy.command.base import ContainerCommand
from arcana.exceptions import ArcanaDataMatchError

from pathlib import Path
import typing as ty
from pydra import mark
from arcana.mark import converter
from arcana.core.data.type import BaseFile


class EncodedText(BaseFile):
    """A text file where the characters ASCII codes are shifted on conversion
    from text
    """

    ext = "enc"

    @classmethod
    @converter(Text)
    def encode(cls, fs_path: ty.Union[str, Path], shift: int = 0):
        shift = int(shift)
        node = encoder_task(in_file=fs_path, shift=shift)
        return node, node.lzout.out


class DecodedText(Text):
    @classmethod
    @converter(EncodedText)
    def decode(cls, fs_path: Path, shift: int = 0):
        shift = int(shift)
        node = encoder_task(
            in_file=fs_path, shift=-shift, out_file="out_file.txt"
        )  # Just shift it backwards by the same amount
        return node, node.lzout.out


@mark.task
def encoder_task(
    in_file: ty.Union[str, Path],
    shift: int,
    out_file: ty.Union[str, Path] = "out_file.enc",
) -> ty.Union[str, Path]:
    with open(in_file) as f:
        contents = f.read()
    encoded = encode_text(contents, shift)
    with open(out_file, "w") as f:
        f.write(encoded)
    return Path(out_file).absolute()


def encode_text(text: str, shift: int) -> str:
    encoded = []
    for c in text:
        encoded.append(chr(ord(c) + shift))
    return "".join(encoded)


def test_command_execute(concatenate_task, saved_dataset, work_dir):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_locator = make_dataset_locator(saved_dataset)
    bp = saved_dataset.__annotations__["blueprint"]
    duplicates = 1

    command_spec = ContainerCommand(
        task="arcana.core.utils.testing.tasks:" + concatenate_task.__name__,
        row_frequency=bp.space.default(),
        inputs=[
            {
                "name": "source1",
                "datatype": "common:Text",
                "field": "in_file1",
                "help_string": "dummy",
            },
            {
                "name": "source2",
                "datatype": "common:Text",
                "field": "in_file2",
                "help_string": "dummy",
            },
        ],
        outputs=[
            {
                "name": "sink1",
                "datatype": "common:Text",
                "field": "out_file",
                "help_string": "dummy",
            }
        ],
        parameters=[
            {
                "name": "duplicates",
                "datatype": "int",
                "default": 2,
                "help_string": "dummy",
            }
        ],
    )
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    command_spec.execute(
        dataset_locator=dataset_locator,
        input_values=[
            ("source1", "file1"),
            ("source2", "file2"),
        ],
        output_values=[
            ("sink1", "concatenated"),
        ],
        parameter_values=[
            ("duplicates", str(duplicates)),
        ],
        raise_errors=True,
        plugin="serial",
        work_dir=str(work_dir),
        loglevel="debug",
        dataset_hierarchy=",".join(str(ln) for ln in bp.hierarchy),
        pipeline_name="test_pipeline",
    )
    # Add source column to saved dataset
    sink = saved_dataset.add_sink("concatenated", Text)
    assert len(sink) == reduce(mul, bp.dim_lengths)
    fnames = ["file1.txt", "file2.txt"]
    if concatenate_task.__name__.endswith("reverse"):
        fnames = [f[::-1] for f in fnames]
    expected_contents = "\n".join(fnames * duplicates)
    for item in sink:
        item.get(assume_exists=True)
        with open(item.fs_path) as f:
            contents = f.read()
        assert contents == expected_contents


def test_command_execute_fail(concatenate_task, saved_dataset, work_dir):
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_locator = make_dataset_locator(saved_dataset)
    bp = saved_dataset.__annotations__["blueprint"]
    duplicates = 1

    command_spec = ContainerCommand(
        task="arcana.core.utils.testing.tasks:" + concatenate_task.__name__,
        row_frequency=bp.space.default(),
        inputs=[
            {
                "name": "source1",
                "datatype": "common:Text",
                "field": "in_file1",
                "help_string": "dummy",
            },
            {
                "name": "source2",
                "datatype": "common:Directory",
                "field": "in_file2",
                "help_string": "dummy",
            },
        ],
        outputs=[
            {
                "name": "sink1",
                "datatype": "common:Text",
                "field": "out_file",
                "help_string": "dummy",
            }
        ],
        parameters=[
            {
                "name": "duplicates",
                "datatype": "int",
                "default": 2,
                "help_string": "dummy",
            }
        ],
    )

    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    with pytest.raises(ArcanaDataMatchError):
        command_spec.execute(
            dataset_locator=dataset_locator,
            input_values=[
                ("source1", "bad-file-path"),
                ("source2", "file2"),
            ],
            output_values=[
                ("sink1", "concatenated"),
            ],
            parameter_values=[
                ("duplicates", duplicates),
            ],
            raise_errors=True,
            plugin="serial",
            work_dir=str(work_dir),
            loglevel="debug",
            dataset_hierarchy=",".join(str(ln) for ln in bp.hierarchy),
            pipeline_name="test_pipeline",
        )


def test_command_execute_on_row(cli_runner, work_dir):

    # Create test dataset consisting of a single row with a range of filenames
    # from 0 to 4
    filenumbers = list(range(5))
    bp = TestDatasetBlueprint(
        [
            TestDataSpace.abcd
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [1, 1, 1, 1],
        [f"{i}.txt" for i in filenumbers],
        {},
        {},
        [],
    )
    dataset_path = work_dir / "numbered_dataset"
    dataset = make_dataset(bp, dataset_path)
    dataset.save()

    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_locator = make_dataset_locator(dataset)

    def get_dataset_filenumbers():
        dataset.refresh()
        row = next(dataset.rows())
        return sorted(int(i.path) for i in row.unresolved)

    assert get_dataset_filenumbers() == filenumbers

    command_spec = ContainerCommand(
        task="arcana.core.utils.testing.tasks:plus_10_to_filenumbers",
        row_frequency=bp.space.default(),
        inputs=[
            {
                "name": "a_row",
                "datatype": "arcana.core.data.row:DataRow",
                "field": "filenumber_row",
                "help_string": "dummy",
            },
        ],
    )

    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    command_spec.execute(
        dataset_locator=dataset_locator,
        input_values=[
            ("a_row", ""),
        ],
        raise_errors=True,
        plugin="serial",
        work_dir=str(work_dir),
        loglevel="debug",
        dataset_hierarchy=",".join(str(ln) for ln in bp.hierarchy),
        pipeline_name="test_pipeline",
    )

    assert get_dataset_filenumbers() == [i + 10 for i in filenumbers]


def test_command_execute_with_converter_args(saved_dataset, work_dir):
    """Test passing arguments to file format converter tasks via input/output
    "qualifiers", e.g. 'converter.shift=3' using the arcana-run-pipeline CLI
    tool (as used in the XNAT CS commands)
    """
    # Get CLI name for dataset (i.e. file system path prepended by 'file//')
    dataset_locator = make_dataset_locator(saved_dataset)
    bp = saved_dataset.__annotations__["blueprint"]
    # Start generating the arguments for the CLI
    # Add source to loaded dataset
    command_spec = ContainerCommand(
        task="arcana.core.utils.testing.tasks:identity_file",
        row_frequency=bp.space.default(),
        inputs=[
            {
                "name": "source",
                "datatype": "arcana.core.utils.testing.data.types:EncodedText",
                "default_column": {"datatype": "common:Text"},
                "field": "in_file",
                "help_string": "dummy",
            },
        ],
        outputs=[
            {
                "name": "sink1",
                "datatype": "arcana.core.utils.testing.data.types:EncodedText",
                "field": "out",
                "help_string": "dummy",
            },
            {
                "name": "sink2",
                "datatype": "arcana.core.utils.testing.data.types:EncodedText",
                "default_column": {
                    "datatype": "arcana.core.utils.testing.data.types:DecodedText"
                },
                "field": "out",
                "help_string": "dummy",
            },
        ],
    )

    command_spec.execute(
        dataset_locator=dataset_locator,
        input_values=[
            ("source", "file1 converter.shift=3"),
        ],
        output_values=[
            ("sink1", "encoded"),
            ("sink2", "decoded converter.shift=3"),
        ],
        raise_errors=True,
        plugin="serial",
        work_dir=str(work_dir),
        loglevel="debug",
        dataset_hierarchy=",".join(str(ln) for ln in bp.hierarchy),
        pipeline_name="test_pipeline",
    )
    # Add source column to saved dataset
    saved_dataset.add_sink("sink1", EncodedText, path="encoded")
    saved_dataset.add_sink("sink2", Text, path="decoded")
    unencoded_contents = "file1.txt"
    encoded_contents = (
        "iloh41w{w"  # 'file1.txt' characters shifted up by 3 in ASCII code
    )
    for row in saved_dataset.rows(frequency="abcd"):
        enc_item = row["sink1"]
        dec_item = row["sink2"]
        enc_item.get(assume_exists=True)
        dec_item.get(assume_exists=True)
        with open(enc_item.fs_path) as f:
            enc_contents = f.read()
        with open(dec_item.fs_path) as f:
            dec_contents = f.read()
        assert enc_contents == encoded_contents
        assert dec_contents == unencoded_contents
