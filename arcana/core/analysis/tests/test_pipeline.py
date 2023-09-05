import zipfile
import tempfile
from pathlib import Path
from fileformats.text import TextFile
from fileformats.application import Zip
from arcana.testing import TestDataSpace
from arcana.common import DirTree
from conftest import TEST_DATASET_BLUEPRINTS
from arcana.testing.tasks import concatenate


def test_pipeline(work_dir):
    dataset = TEST_DATASET_BLUEPRINTS["concatenate_test"].make_dataset(
        DirTree(), work_dir / "dataset"
    )

    dataset.add_source("file1", TextFile)
    dataset.add_source("file2", TextFile)
    dataset.add_sink("deriv", TextFile)

    pipeline = dataset.apply_pipeline(
        name="test_pipeline",
        workflow=concatenate(duplicates=2, name="concatenate"),
        inputs=[("file1", "in_file1"), ("file2", "in_file2")],
        outputs=[("deriv", "out_file")],
        row_frequency=TestDataSpace.abcd,
    )

    IDS = ["a0b0c0d0", "a0b0c0d1"]

    workflow = pipeline(cache_dir=work_dir / "pipeline-cache")
    workflow(ids=IDS, plugin="serial")

    for item in dataset["deriv"]:
        with open(item.fspath) as f:
            contents = f.read()
        assert contents == "\n".join(["file1.txt", "file2.txt"] * 2)


def test_pipeline_with_implicit_conversion(work_dir):
    """Input files are converted from zip to TextFile, concatenated and then
    written back as zip files into the data store"""
    dataset = TEST_DATASET_BLUEPRINTS["concatenate_zip_test"].make_dataset(
        DirTree(), work_dir / "dataset"
    )

    dataset.add_source("file1", Zip[TextFile])
    dataset.add_source("file2", Zip[TextFile])
    dataset.add_sink("deriv", Zip[TextFile])

    pipeline = dataset.apply_pipeline(
        name="test_pipeline",
        workflow=concatenate(duplicates=2, name="concatenate"),
        inputs=[("file1", "in_file1", TextFile), ("file2", "in_file2", TextFile)],
        outputs=[("deriv", "out_file", TextFile)],
        row_frequency=TestDataSpace.abcd,
    )

    IDS = ["a0b0c0d0", "a0b0c0d1"]

    with dataset.tree:
        workflow = pipeline(cache_dir=work_dir / "pipeline-cache")
        workflow(ids=IDS, plugin="serial")

    for item in dataset["deriv"]:
        tmp_dir = Path(tempfile.mkdtemp())
        with zipfile.ZipFile(item.fspath) as zfile:
            zfile.extractall(path=tmp_dir)
        with open(tmp_dir / "out_file.txt") as f:
            contents = f.read()
        assert contents == "\n".join(["file1.zip", "file2.zip"] * 2)
