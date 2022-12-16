from pathlib import Path
import cloudpickle as cp
from pydra import mark, Workflow
from arcana.core.data.set import Dataset
from arcana.core.utils.serialize import asdict, fromdict


def test_dataset_asdict_roundtrip(dataset):

    dct = asdict(dataset, omit=["store", "id"])
    undct = fromdict(dct, store=dataset.store, id=dataset.id)
    assert isinstance(dct, dict)
    assert "store" not in dct
    del dataset.__annotations__["blueprint"]
    assert dataset == undct


def test_dataset_pickle(dataset: Dataset, tmp_dir: Path):
    fpath = tmp_dir / "dataset.pkl"
    with fpath.open("wb") as fp:
        cp.dump(dataset, fp)
    with fpath.open("rb") as fp:
        reloaded = cp.load(fp)
    assert dataset == reloaded


def test_dataset_in_workflow_pickle(dataset: Dataset, tmp_dir: Path):

    # Create the outer workflow to link the analysis workflow with the
    # data row iteration and store connection rows
    wf = Workflow(name="test", input_spec=["a"])

    wf.add(func(a=wf.lzin.a, b=2, dataset=dataset, name="test_func"))

    wf.set_output(("c", wf.test_func.lzout.c))

    wf.pickle_task()


@mark.task
@mark.annotate({"a": int, "b": int, "dataset": Dataset, "return": {"c": int}})
def func(a, b, dataset):
    return a + b
