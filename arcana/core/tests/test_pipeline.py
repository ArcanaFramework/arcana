
import pytest
import zipfile
import tempfile
from pathlib import Path
from arcana.data.formats.common import Text, Zip
from arcana.core.data.set import Dataset
from arcana.tests.fixtures.common import (
    concatenate, make_dataset, TEST_DATASET_BLUEPRINTS, TestDataSpace)
# from pydra.tasks.fsl.preprocess.bet import BET
# from arcana.data.formats.medimage import Dicom, NiftiGz


def test_pipeline(work_dir):
    dataset = make_dataset(TEST_DATASET_BLUEPRINTS['concatenate_test'], work_dir)

    dataset.add_source('file1', Text)
    dataset.add_source('file2', Text)
    dataset.add_sink('deriv', Text)

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
    """Input files are converted from zip to Text, concatenated and then
    written back as zip files into the data store"""
    dataset = make_dataset(TEST_DATASET_BLUEPRINTS['concatenate_zip_test'],
                           work_dir)

    dataset.add_source('file1', Zip)
    dataset.add_source('file2', Zip)
    dataset.add_sink('deriv', Zip)

    pipeline = dataset.new_pipeline(
        name='test_pipeline',
        inputs=[('file1', Text), ('file2', Text)],
        outputs=[('deriv', Text)],
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


@pytest.mark.skip("Not implemented yet")
def test_apply_workflow(work_dir):

    # Load dataset
    my_dataset = Dataset.load('file///data/my-dataset',
                              hierarchy=['subject', 'session'])

    # Add source column to select T1-weighted images in each sub-directory
    my_dataset.add_source('T1w', '.*mprage.*', format=Dicom, is_regex=True)

    # Add sink column to store brain mask
    my_dataset.add_sink('brain_mask', 'derivs/brain_mask', format=NiftiGz)

    # Apply BET Pydra task, connecting it betwee between the source and sink
    my_dataset.apply_workflow(
        'brain_extraction',
        BET(),
        inputs=[('T1w', 'in_file', NiftiGz)],
        outputs=[('brain_mask', 'out_file')])

    # Generate brain mask derivative
    my_dataset.derive('brain_mask')