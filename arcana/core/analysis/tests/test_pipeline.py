import zipfile
import tempfile
from pathlib import Path
from fileformats.common import Text, Zip
from arcana.core.utils.testing.data import TestDataSpace
from conftest import TEST_DATASET_BLUEPRINTS
from arcana.core.utils.testing.tasks import concatenate


def test_pipeline(simple_store, work_dir):
    dataset = simple_store.make_test_dataset(
        TEST_DATASET_BLUEPRINTS["concatenate_test"], work_dir / "dataset"
    )

    dataset.add_source("file1", Text)
    dataset.add_source("file2", Text)
    dataset.add_sink("deriv", Text)

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

    dataset.refresh()  # Reset cached values

    for item in dataset["deriv"]:
        with open(item.fs_path) as f:
            contents = f.read()
        assert contents == "\n".join(["file1.txt", "file2.txt"] * 2)


def test_pipeline_with_implicit_conversion(simple_store, work_dir):
    """Input files are converted from zip to Text, concatenated and then
    written back as zip files into the data store"""
    dataset = simple_store.make_test_dataset(
        TEST_DATASET_BLUEPRINTS["concatenate_zip_test"], work_dir / "dataset"
    )

    dataset.add_source("file1", Zip)
    dataset.add_source("file2", Zip)
    dataset.add_sink("deriv", Zip)

    pipeline = dataset.apply_pipeline(
        name="test_pipeline",
        workflow=concatenate(duplicates=2, name="concatenate"),
        inputs=[("file1", "in_file1", Text), ("file2", "in_file2", Text)],
        outputs=[("deriv", "out_file", Text)],
        row_frequency=TestDataSpace.abcd,
    )

    IDS = ["a0b0c0d0", "a0b0c0d1"]

    workflow = pipeline(cache_dir=work_dir / "pipeline-cache")
    workflow(ids=IDS, plugin="serial")

    dataset.refresh()

    for item in dataset["deriv"]:
        tmp_dir = Path(tempfile.mkdtemp())
        with zipfile.ZipFile(item.fs_path) as zfile:
            zfile.extractall(path=tmp_dir)
        with open(tmp_dir / "out_u_file.txt") as f:
            contents = f.read()
        assert contents == "\n".join(["file1.zip", "file2.zip"] * 2)
