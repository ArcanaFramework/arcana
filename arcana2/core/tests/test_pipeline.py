
import zipfile
import tempfile
from pathlib import Path
from arcana2.data.repositories.tests.fixtures import (
    make_dataset, TEST_DATASET_BLUEPRINTS, TestDataSpace)
from arcana2.tasks.tests.fixtures import concatenate
from arcana2.data.types.general import text, zip


def test_pipeline(work_dir):
    dataset = make_dataset(TEST_DATASET_BLUEPRINTS['concatenate_test'], work_dir)

    dataset.add_source('file1', text)
    dataset.add_source('file2', text)
    dataset.add_sink('deriv', text)

    pipeline = dataset.new_pipeline(
        name='test_pipeline',
        inputs=['file1', 'file2'],
        outputs=['deriv'],
        frequency=TestDataSpace.abcd)

    pipeline.add(concatenate(in_file1=pipeline.lzin.file1,
                             in_file2=pipeline.lzin.file2,
                             duplicates=2,
                             name='concatenate'))

    pipeline.set_output(('deriv', pipeline.concatenate.lzout.out_file))

    IDS = ['a0b0c0d0', 'a0b0c0d1']

    pipeline(ids=IDS, plugin='serial')

    for item in dataset['deriv']:
        with open(item.fs_path) as f:
            contents = f.read()
        assert contents == '\n'.join(['file1.txt', 'file2.txt'] * 2)


def test_pipeline_with_implicit_conversion(work_dir):
    """Input files are converted from zip to text, concatenated and then
    written back as zip files into the data store"""
    dataset = make_dataset(TEST_DATASET_BLUEPRINTS['concatenate_zip_test'],
                           work_dir)

    dataset.add_source('file1', zip)
    dataset.add_source('file2', zip)
    dataset.add_sink('deriv', zip)

    pipeline = dataset.new_pipeline(
        name='test_pipeline',
        inputs=[('file1', text), ('file2', text)],
        outputs=[('deriv', text)],
        frequency=TestDataSpace.abcd)

    pipeline.add(concatenate(in_file1=pipeline.lzin.file1,
                             in_file2=pipeline.lzin.file2,
                             duplicates=2,
                             name='concatenate'))

    pipeline.set_output(('deriv', pipeline.concatenate.lzout.out_file))

    IDS = ['a0b0c0d0', 'a0b0c0d1']

    pipeline(ids=IDS, plugin='serial')

    for item in dataset['deriv']:
        tmp_dir = Path(tempfile.mkdtemp())
        with zipfile.ZipFile(item.fs_path) as zfile:
            zfile.extractall(path=tmp_dir)
        with open(tmp_dir / 'out_file.txt') as f:
            contents = f.read()
        assert contents == '\n'.join(['file1.zip', 'file2.zip'] * 2)
