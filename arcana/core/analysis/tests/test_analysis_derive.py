from pathlib import Path
import typing as ty
import pytest
import pydra
from fileformats.archive import Zip
from fileformats.text import Plain as PlainText
from arcana.core.mark import analysis, column, parameter, pipeline
from arcana.core.analysis.salience import ColumnSalience as cs
from arcana.testing.tasks import identity
from arcana.stdlib import Clinical
from arcana.core.data.set import Dataset


@pytest.mark.xfail(reason="Hasn't been implemented yet", raises=NotImplementedError)
def test_analysis(dataset: Dataset, work_dir: Path):
    """Tests the complete "changeme" deployment pipeline by building and running an app
    against a test dataset"""

    @analysis(Clinical)
    class ExampleAnalysis:

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
        ) -> ty.Tuple[PlainText, int]:

            wf.add(identity(name="file_identity", in_=a_file))
            wf.add(identity(name="metric_identity", in_=a_parameter))

            return (wf.file_identity.lzout.out, wf.metric_identity.lzout.out)

    dataset.apply(
        ExampleAnalysis(
            a_file="file1",
            a_parameter=10,
        )
    )

    dataset.derive("deriv_file")

    for item in dataset.columns["deriv_file"]:
        assert item.contents == "file1.txt"
