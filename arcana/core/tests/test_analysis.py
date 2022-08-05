from arcana.data.spaces.medimage import Clinical
from arcana.test.tasks import concatenate
from arcana.core.mark import analysis, pipeline, parameter, column
from arcana.data.formats.common import Zip, Text


def test_analysis_validation():
    @analysis(Clinical)
    class AnAnalysis:

        file1: Zip
        file2: Text
        unzipped: Text = column()

        a_param: int = parameter()

        @pipeline(unzipped)
        def a_pipeline(self, wf, file1: Text, file2: Text, a_param: int):

            wf.add(concatenate(name="a_node", in_file1=file1, in_file2=file2))

            return wf.a_node.lzout.out

    assert [p.name for p in AnAnalysis.parameters] == ["a_param"]
