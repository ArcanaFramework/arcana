from pathlib import Path
import cloudpickle as cp
from pydra import mark, Workflow
from arcana2.core.data.set import Dataset
from arcana2.core.data.spec import DataSource, DataSink
from arcana2.data.stores.file_system import FileSystem
from arcana2.data.dimensions.clinical import Clinical as cl
from arcana2.data.types.neuroimaging import dicom, niftix_gz


def test_dataset_pickle(dataset: Dataset, tmp_dir: Path):
    fpath = tmp_dir / 'dataset.pkl'
    with fpath.open("wb") as fp:
        cp.dump(dataset, fp)
    with fpath.open("rb") as fp:
        reloaded = cp.load(fp)
    assert dataset == reloaded


def test_dataset_in_workflow_pickle(dataset: Dataset, tmp_dir: Path):

    # Create the outer workflow to link the analysis workflow with the
    # data node iteration and repository connection nodes
    wf = Workflow(name='test', input_spec=['a'])

    wf.add(func(
        a=wf.lzin.a,
        b=2,
        dataset=dataset,
        name='test_func'))

    wf.set_output(('c', wf.test_func.lzout.c))

    wf.pickle_task()


@mark.task
@mark.annotate({
   'a': int,
   'b': int,
   'dataset': Dataset,
   'return':{
       'c': int}})
def func(a, b, dataset):
   return a + b