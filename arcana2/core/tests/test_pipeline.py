
from arcana2.test_fixtures.dataset import (
    make_dataset, TestDatasetBlueprint, TestDataSpace)
from arcana2.test_fixtures.tasks import concatenate
from arcana2.datatypes.general import text


basic_dataset = TestDatasetBlueprint(
        [TestDataSpace.abcd],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        [1, 1, 1, 2],
        ['file1.txt', 'file2.txt'],
        {}, {}, [])


def test_pipeline(work_dir):
    dataset = make_dataset(basic_dataset, work_dir)

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

    for id in IDS:
        with open(dataset['deriv'][id].path) as f:
            contents = f.read()
        assert contents == '\n'.join(['file1.txt', 'file1.txt'] * 2)
