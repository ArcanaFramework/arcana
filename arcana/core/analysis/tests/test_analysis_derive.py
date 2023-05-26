from pathlib import Path
import typing as ty
import pytest
import pydra
from fileformats.archive import Zip
from fileformats.text import Plain as PlainText
from arcana.core.mark import analysis, column, parameter, pipeline
from arcana.core.analysis.salience import ColumnSalience as cs
from arcana.core.analysis.base import Analysis
from arcana.testing.tasks import identity
from arcana.testing.data.blueprint import (
    TestDatasetBlueprint,
    TestDataSpace,
    FileSetEntryBlueprint as FileBP,
)
from arcana.stdlib import DirTree
from arcana.core.data.set import Dataset


ANALSYS_DATASET_BLUEPRINTS = {
    "basic": TestDatasetBlueprint(
        hierarchy=[
            "abcd"
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        space=TestDataSpace,
        dim_lengths=[1, 1, 1, 1],
        entries=[
            FileBP(path="file1", datatype=PlainText, filenames=["file1.txt"]),
            FileBP(path="file2", datatype=PlainText, filenames=["file2.txt"]),
        ],
    )
}


@pytest.fixture(params=list(ANALSYS_DATASET_BLUEPRINTS))
def analysis_dataset(request, work_dir):
    dataset_id = work_dir / "test_dataset"
    blueprint = ANALSYS_DATASET_BLUEPRINTS[request.param]
    dataset = blueprint.make_dataset(DirTree(), dataset_id, name="")
    dataset.add_source(
        name="column1",
        datatype=PlainText,
        path="file1",
    )
    dataset.add_source(
        name="column2",
        datatype=PlainText,
        path="file2",
    )
    return dataset


@pytest.mark.xfail(reason="Hasn't been implemented yet", raises=NotImplementedError)
def test_analysis(analysis_dataset: Dataset, work_dir: Path):
    """Tests the complete "changeme" deployment pipeline by building and running an app
    against a test dataset"""

    @analysis(TestDataSpace)
    class ExampleAnalysis(Analysis):

        a_file: Zip[PlainText] = column(
            "a text file that has been zipped", salience=cs.primary
        )
        deriv_file: PlainText = column("a derived text file", salience=cs.publication)
        deriv_metric: int = column("a derived metric", salience=cs.publication)

        a_parameter: int = parameter("a basic parameter", default=1)

        @pipeline(deriv_file, deriv_metric)
        def concat_pipeline(
            self,
            wf: pydra.Workflow,
            a_file: PlainText,  # arg names must match columns or parameters
            a_parameter: int,  # " " "
        ) -> tuple[PlainText, int]:

            wf.add(identity(name="file_identity", in_=a_file))
            wf.add(identity(name="metric_identity", in_=a_parameter))

            return wf.file_identity.lzout.out, wf.metric_identity.lzout.out

    analysis_dataset.apply(
        "example_analysis",
        ExampleAnalysis,
        a_file="column1",
        a_parameter=10,
    )

    analysis_dataset.derive("example_analysis.deriv_file")

    for item in analysis_dataset.columns["example_analysis.deriv_file"]:
        assert item.contents == "file1.txt"
