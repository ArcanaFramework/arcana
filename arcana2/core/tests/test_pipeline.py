
from arcana2.core.data.tests.fixtures import (
    make_dataset, TEST_DATASET_BLUEPRINTS, TestDataSpace)
from arcana2.tasks.tests.fixtures import concatenate
from arcana2.data.types.general import text


def test_pipeline(work_dir):
    dataset = make_dataset(TEST_DATASET_BLUEPRINTS['concatenate-test'], work_dir)

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
