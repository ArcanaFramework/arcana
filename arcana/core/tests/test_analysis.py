import pytest
from arcana.data.spaces.medimage import Clinical
from arcana.test.tasks import concatenate
from arcana.core.mark import analysis, pipeline, parameter, column
from arcana.data.formats.common import Zip, Text


@pytest.skip
def test_analysis_validation():
    @analysis(Clinical)
    class AnAnalysis:

        file1: Zip = column("an arbitrary text file")
        file2: Text = column("another arbitrary text file")
        concatenated: Text = column("the output of concatenating file1 and file2")

        duplicates: int = parameter(
            "the number of times to duplicate the concatenation"
        )

        @pipeline(concatenated)
        def a_pipeline(self, wf, file1: Text, file2: Text, a_param: int):

            wf.add(
                concatenate(
                    name="a_node", in_file1=file1, in_file2=file2, duplicates=a_param
                )
            )

            return wf.a_node.lzout.out

    assert [p.name for p in AnAnalysis.parameters] == ["a_param"]
