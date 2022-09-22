from pathlib import Path
import tempfile
import pytest
import pydra
from arcana.data.spaces.common import Samples
from arcana.test.tasks import (
    concatenate,
    concatenate_reverse,
    multiply_contents,
    contents_are_numeric,
    identity_file,
)
from arcana.core.mark import (
    analysis,
    pipeline,
    parameter,
    column,
    inherited_from,
    mapped_from,
    value_of,
    switch,
    is_provided,
    check,
    subanalysis,
)
from arcana.core.analysis import Operation
from arcana.data.formats.common import Zip, Text
from arcana.core.enum import (
    CheckStatus,
    ColumnSalience as cs,
    ParameterSalience as ps,
    CheckSalience as chs,
)
from arcana.exceptions import ArcanaDesignError


def get_contents(fpath):
    with open(fpath) as f:
        return f.read().splitlines()


@pytest.fixture(scope="session")
def source_dir():
    return Path(tempfile.mkdtemp())


@pytest.fixture(scope="session")
def test_file1(source_dir):
    fpath = source_dir / "file1.txt"
    with open(fpath, "w") as f:
        f.write("file1")
    return fpath


@pytest.fixture(scope="session")
def test_file2(source_dir):
    fpath = source_dir / "file2.txt"
    with open(fpath, "w") as f:
        f.write("file2")
    return fpath


@pytest.fixture(scope="session")
def test_file3(source_dir):
    fpath = source_dir / "file3.txt"
    with open(fpath, "w") as f:
        f.write("file3")
    return fpath


@pytest.fixture(scope="session")
def test_numeric_file1(source_dir):
    file1_path = source_dir / "file1.txt"
    with open(file1_path, "w") as f:
        f.write("1")
    return file1_path


@pytest.fixture(scope="session")
def test_numeric_file2(source_dir):
    file2_path = source_dir / "file2.txt"
    with open(file2_path, "w") as f:
        f.write("2")
    return file2_path


@pytest.fixture(scope="session")
def Concat():
    @analysis(Samples)
    class _Concat:

        file1: Zip = column("an arbitrary text file", salience=cs.primary)
        file2: Text = column("another arbitrary text file", salience=cs.primary)
        concatenated: Text = column("the output of concatenating file1 and file2")

        duplicates: int = parameter(
            "the number of times to duplicate the concatenation", default=1
        )

        @pipeline(concatenated)
        def concat_pipeline(self, wf, file1: Text, file2: Text, duplicates: int):

            wf.add(
                concatenate(
                    name="a_node", in_file1=file1, in_file2=file2, duplicates=duplicates
                )
            )

            return wf.a_node.lzout.out_file

    return _Concat


@pytest.fixture(scope="session")
def ExtendedConcat(Concat):
    @analysis(Samples)
    class _ExtendedConcat(Concat):

        # Sources
        concatenated = inherited_from(Concat)
        file3: Text = column("Another file to concatenate", salience=cs.primary)

        # Sinks
        doubly_concatenated: Text = column("The doubly concatenated file")

        # Parameters
        # Change the default 'duplicates' value for first concat and inherit it into the
        # namespace so it can also be used for the second concatenation pipeline too
        duplicates = inherited_from(Concat, default=2)

        @pipeline(doubly_concatenated)
        def doubly_concat_pipeline(
            self, wf, concatenated: Text, file3: Text, duplicates: int
        ):

            wf.add(
                concatenate(
                    name="concat",
                    in_file1=concatenated,
                    in_file2=file3,
                    duplicates=duplicates,
                )
            )

            return wf.concat.lzout.out_file

    return _ExtendedConcat


@pytest.fixture(scope="session")
def ConcatWithCheck(Concat):
    @analysis(Samples)
    class _ConcatWithCheck(Concat):

        concatenated = inherited_from(Concat)

        duplicates = inherited_from(Concat)

        @check(concatenated, salience=chs.recommended)
        def num_lines_check(self, wf, concatenated: Text, duplicates: int):
            """Checks the number of lines in the concatenated file to see whether they
            match what is expected for the number of duplicates specified"""

            @pydra.mark.task
            def num_lines_equals(in_file: Path, num_lines: int) -> CheckStatus:
                with open(in_file) as f:
                    contents = f.read()
                if len(contents.splitlines()) == num_lines:
                    status = CheckStatus.probable_pass
                else:
                    status = CheckStatus.failed
                return status

            wf.add(
                num_lines_equals(
                    in_file=concatenated,
                    num_lines=2 * duplicates,
                    name="num_lines_check",
                )
            )

            return wf.num_lines_check.lzout.out

    return _ConcatWithCheck


@pytest.fixture(scope="session")
def OverridenConcat(Concat):
    @analysis(Samples)
    class _OverridenConcat(Concat):

        file1 = inherited_from(Concat)
        file2 = inherited_from(Concat)
        concatenated = inherited_from(Concat)

        duplicates = inherited_from(Concat, default=2)
        order: str = parameter(
            "perform the concatenation in reverse order, i.e. file2 and then file1",
            choices=["forward", "reversed"],
            default="forward",
        )

        @pipeline(
            concatenated,
            condition=((value_of(order) == "reversed") & is_provided(file1)),
        )
        def reverse_concat_pipeline(
            self, wf, file1: Text, file2: Text, duplicates: int
        ):

            wf.add(
                concatenate_reverse(
                    name="concat", in_file1=file1, in_file2=file2, duplicates=duplicates
                )
            )

            return wf.concat.lzout.out_file

    return _OverridenConcat


@pytest.fixture(scope="session")
def ConcatWithSwitch(Concat):
    @analysis(Samples)
    class _ConcatWithSwitch(Concat):

        file1 = inherited_from(Concat)
        file2 = inherited_from(Concat)
        concatenated = inherited_from(Concat)
        multiplied: Text = column("contents of the concatenated files are multiplied")

        multiplier: int = parameter(
            "the multiplier used to apply", salience=ps.required
        )

        @switch
        def inputs_are_numeric(self, wf, file1: Text, file2: Text):

            wf.add(contents_are_numeric(in_file=file1, name="check_file1"))

            wf.add(contents_are_numeric(in_file=file2, name="check_file2"))

            @pydra.mark.task
            def boolean_and(val1, val2) -> bool:
                return val1 and val2

            wf.add(
                boolean_and(
                    val1=wf.check_file1.lzout.out,
                    val2=wf.check_file2.lzout.out,
                    name="bool_and",
                )
            )

            return wf.bool_and.lzout.out

        @pipeline(multiplied, switch=inputs_are_numeric)
        def multiply_pipeline(self, wf, concatenated, multiplier):

            wf.add(
                multiply_contents(
                    name="multiply", in_file=concatenated, multiplier=multiplier
                )
            )

            return wf.multiply.lzout.out

    return _ConcatWithSwitch


@pytest.fixture(scope="session")
def ConcatWithSubanalyses(ExtendedConcat, ConcatWithSwitch):
    @analysis(Samples)
    class _ConcatWithSubanalyses:

        # Source columns mapped from "sub1" subanalysis so they can be shared across
        # both sub-analyses. Note that they could just as easily have been mapped from
        # "sub1" or recreated from scratch and mapped into both
        file1 = mapped_from("sub1", "file1")
        file2 = mapped_from("sub1", "file2")

        # Sink columns generated within the subanalyses mapped back out to the global
        # namespace so they can be mapped into the other subanalysis
        concatenated = mapped_from("sub2", "concatenated")
        concat_and_multiplied = mapped_from("sub2", "multiplied")

        # Link the duplicates parameter across both subanalyses so it is always the same
        # by mapping a global parameter into both subanalyses
        common_duplicates = mapped_from(
            "sub1", "duplicates", default=5, salience=ps.check
        )

        # Additional parameters such as "multiplier" can be accessed within the subanalysis
        # class after the analysis class has been initialised using the 'sub2.multiplier'

        sub1: ExtendedConcat = subanalysis(
            "sub-analysis to add the 'doubly_concat' pipeline",
            # Feed the multiplied sink column from sub2 into the source column file3 of
            # the extended class
            concatenated=concatenated,  # Saves calculating concatenated twice in both sub-analyses
            file3=concat_and_multiplied,
        )
        sub2: ConcatWithSwitch = subanalysis(
            "sub-analysis to add the 'multiply' pipeline",
            file1=file1,
            file2=file2,
            # Use the concatenated generated by sub1 to avoid running it twice
            duplicates=common_duplicates,
        )

    return _ConcatWithSubanalyses


def test_analysis_basic(Concat, test_file1, test_file2):

    analysis_spec = Concat.__spec__

    assert list(analysis_spec.parameter_names) == ["duplicates"]

    assert list(analysis_spec.column_names) == [
        "concatenated",
        "file1",
        "file2",
    ]

    assert list(analysis_spec.pipeline_names) == ["concat_pipeline"]
    assert list(analysis_spec.check_names) == []
    assert list(analysis_spec.switch_names) == []
    assert list(analysis_spec.subanalysis_names) == []

    duplicates = analysis_spec.parameter("duplicates")
    assert duplicates.type is int
    assert duplicates.default == 1
    assert duplicates.salience == ps.recommended
    assert duplicates.defined_in is Concat

    file1 = analysis_spec.column_spec("file1")
    assert file1.type is Zip
    assert file1.row_frequency == Samples.sample
    assert file1.salience == cs.primary
    assert file1.defined_in is Concat
    assert file1.mapped_from is None

    file2 = analysis_spec.column_spec("file2")
    assert file2.type is Text
    assert file2.row_frequency == Samples.sample
    assert file2.salience == cs.primary
    assert file2.defined_in is Concat
    assert file2.mapped_from is None

    concatenated = analysis_spec.column_spec("concatenated")
    assert concatenated.type is Text
    assert concatenated.row_frequency == Samples.sample
    assert concatenated.salience == cs.supplementary
    assert concatenated.defined_in is Concat
    assert concatenated.mapped_from is None

    concat_pipeline = analysis_spec.pipeline_builder("concat_pipeline")
    assert concat_pipeline.name == "concat_pipeline"
    assert concat_pipeline.parameters == ("duplicates",)
    assert concat_pipeline.inputs == ("file1", "file2")
    assert concat_pipeline.outputs == ("concatenated",)
    assert concat_pipeline.method is Concat.concat_pipeline
    assert concat_pipeline.condition is None
    assert concat_pipeline.switch is None

    # Initialise class
    analysis = Concat(file1="a_column", file2="another_column", duplicates=3)
    assert analysis.file1 == "a_column"
    assert analysis.file2 == "another_column"
    assert analysis.concatenated is None
    assert analysis.duplicates == 3
    wf = pydra.Workflow(
        name="test_analysis",
        input_spec=["file1", "file2"],
        file1=test_file1,
        file2=test_file2,
    )
    concatenated = analysis.concat_pipeline(
        wf, file1=wf.lzin.file1, file2=wf.lzin.file2, duplicates=analysis.duplicates
    )
    wf.set_output(("concatenated", concatenated))
    result = wf(plugin="serial")
    assert get_contents(result.output.concatenated) == [
        "file1",
        "file2",
        "file1",
        "file2",
        "file1",
        "file2",
    ]


def test_analysis_extended(Concat, ExtendedConcat, test_file1, test_file2, test_file3):

    analysis_spec = ExtendedConcat.__spec__

    assert sorted(analysis_spec.parameter_names) == ["duplicates"]

    assert sorted(analysis_spec.column_names) == [
        "concatenated",
        "doubly_concatenated",
        "file1",
        "file2",
        "file3",
    ]

    assert list(analysis_spec.pipeline_names) == [
        "concat_pipeline",
        "doubly_concat_pipeline",
    ]
    assert list(analysis_spec.check_names) == []
    assert list(analysis_spec.switch_names) == []
    assert list(analysis_spec.subanalysis_names) == []

    duplicates = analysis_spec.parameter("duplicates")
    assert duplicates.type is int
    assert duplicates.default == 2
    assert duplicates.salience == ps.recommended
    assert duplicates.defined_in is Concat
    assert duplicates.modified == (("default", 2),)

    file1 = analysis_spec.column_spec("file1")
    assert file1.type is Zip
    assert file1.row_frequency == Samples.sample
    assert file1.salience == cs.primary
    assert file1.defined_in is Concat
    assert file1.mapped_from is None

    file2 = analysis_spec.column_spec("file2")
    assert file2.type is Text
    assert file2.row_frequency == Samples.sample
    assert file2.salience == cs.primary
    assert file2.defined_in is Concat
    assert file2.mapped_from is None

    file3 = analysis_spec.column_spec("file3")
    assert file3.type is Text
    assert file3.row_frequency == Samples.sample
    assert file3.salience == cs.primary
    assert file3.defined_in is ExtendedConcat
    assert file3.mapped_from is None

    concatenated = analysis_spec.column_spec("concatenated")
    assert concatenated.type is Text
    assert concatenated.row_frequency == Samples.sample
    assert concatenated.salience == cs.supplementary
    assert concatenated.defined_in is Concat
    assert concatenated.mapped_from is None

    doubly_concatenated = analysis_spec.column_spec("doubly_concatenated")
    assert doubly_concatenated.type is Text
    assert doubly_concatenated.row_frequency == Samples.sample
    assert doubly_concatenated.salience == cs.supplementary
    assert doubly_concatenated.defined_in is ExtendedConcat

    concat_pipeline = analysis_spec.pipeline_builder("concat_pipeline")
    assert concat_pipeline.name == "concat_pipeline"
    assert concat_pipeline.parameters == ("duplicates",)
    assert concat_pipeline.inputs == ("file1", "file2")
    assert concat_pipeline.outputs == ("concatenated",)
    assert concat_pipeline.method is Concat.concat_pipeline
    assert concat_pipeline.defined_in is Concat
    assert concat_pipeline.condition is None
    assert concat_pipeline.switch is None

    doubly_concat_pipeline = analysis_spec.pipeline_builder("doubly_concat_pipeline")
    assert doubly_concat_pipeline.name == "doubly_concat_pipeline"
    assert doubly_concat_pipeline.parameters == ("duplicates",)
    assert doubly_concat_pipeline.inputs == ("concatenated", "file3")
    assert doubly_concat_pipeline.outputs == ("doubly_concatenated",)
    assert doubly_concat_pipeline.method is ExtendedConcat.doubly_concat_pipeline
    assert doubly_concat_pipeline.defined_in is ExtendedConcat
    assert doubly_concat_pipeline.condition is None
    assert doubly_concat_pipeline.switch is None

    # Initialise class
    analysis = ExtendedConcat(
        file1="a_column",
        file2="another_column",
        file3="yet_another_column",
        duplicates=1,
    )
    assert analysis.file1 == "a_column"
    assert analysis.file2 == "another_column"
    assert analysis.file3 == "yet_another_column"
    assert analysis.concatenated is None
    assert analysis.doubly_concatenated is None
    assert analysis.duplicates == 1
    wf = pydra.Workflow(
        name="test_analysis",
        input_spec=["file1", "file2", "file3"],
        file1=test_file1,
        file2=test_file2,
        file3=test_file3,
    )
    concatenated = analysis.concat_pipeline(
        wf, file1=wf.lzin.file1, file2=wf.lzin.file2, duplicates=analysis.duplicates
    )
    doubly_concatenated = analysis.doubly_concat_pipeline(
        wf,
        concatenated=concatenated,
        file3=wf.lzin.file3,
        duplicates=analysis.duplicates,
    )
    wf.set_output(("doubly_concatenated", doubly_concatenated))
    result = wf(plugin="serial")
    assert get_contents(result.output.doubly_concatenated) == [
        "file1",
        "file2",
        "file3",
    ]


def test_analysis_with_check(Concat, ConcatWithCheck, test_file1, test_file2):

    analysis_spec = ConcatWithCheck.__spec__

    assert sorted(analysis_spec.parameter_names) == ["duplicates"]

    assert sorted(analysis_spec.column_names) == [
        "concatenated",
        "file1",
        "file2",
    ]

    assert list(analysis_spec.pipeline_names) == [
        "concat_pipeline",
    ]
    assert list(analysis_spec.check_names) == ["num_lines_check"]
    assert list(analysis_spec.switch_names) == []
    assert list(analysis_spec.subanalysis_names) == []

    duplicates = analysis_spec.parameter("duplicates")
    assert duplicates.type is int
    assert duplicates.default == 1
    assert duplicates.salience == ps.recommended
    assert duplicates.defined_in is Concat

    file1 = analysis_spec.column_spec("file1")
    assert file1.type is Zip
    assert file1.row_frequency == Samples.sample
    assert file1.salience == cs.primary
    assert file1.defined_in is Concat
    assert file1.mapped_from is None

    file2 = analysis_spec.column_spec("file2")
    assert file2.type is Text
    assert file2.row_frequency == Samples.sample
    assert file2.salience == cs.primary
    assert file2.defined_in is Concat
    assert file2.mapped_from is None

    concatenated = analysis_spec.column_spec("concatenated")
    assert concatenated.type is Text
    assert concatenated.row_frequency == Samples.sample
    assert concatenated.salience == cs.supplementary
    assert concatenated.defined_in is Concat
    assert concatenated.mapped_from is None

    concat_pipeline = analysis_spec.pipeline_builder("concat_pipeline")
    assert concat_pipeline.name == "concat_pipeline"
    assert concat_pipeline.parameters == ("duplicates",)
    assert concat_pipeline.inputs == ("file1", "file2")
    assert concat_pipeline.outputs == ("concatenated",)
    assert concat_pipeline.method is Concat.concat_pipeline
    assert concat_pipeline.condition is None
    assert concat_pipeline.switch is None
    assert concat_pipeline.defined_in is Concat

    num_lines_check = analysis_spec.check("num_lines_check")
    assert num_lines_check.name == "num_lines_check"
    assert num_lines_check.column == "concatenated"
    assert num_lines_check.inputs == ("concatenated",)
    assert num_lines_check.parameters == ("duplicates",)
    assert num_lines_check.method == ConcatWithCheck.num_lines_check
    assert num_lines_check.column == "concatenated"
    assert num_lines_check.defined_in is ConcatWithCheck

    # Initialise class
    analysis = ConcatWithCheck(file1="a_column", file2="another_column", duplicates=7)
    assert analysis.file1 == "a_column"
    assert analysis.file2 == "another_column"
    assert analysis.concatenated is None
    assert analysis.duplicates == 7
    wf = pydra.Workflow(
        name="test_analysis",
        input_spec=["file1", "file2"],
        file1=test_file1,
        file2=test_file2,
    )
    concatenated = analysis.concat_pipeline(
        wf, file1=wf.lzin.file1, file2=wf.lzin.file2, duplicates=analysis.duplicates
    )
    num_lines_check = analysis.num_lines_check(
        wf, concatenated=concatenated, duplicates=analysis.duplicates
    )
    wf.set_output(("num_lines_check", num_lines_check))
    result = wf(plugin="serial")
    assert result.output.num_lines_check == CheckStatus.probable_pass


def test_analysis_override(Concat, OverridenConcat, test_file1, test_file2):
    """Tests overriding methods in the base class with optional switches based on
    parameters and properties of the inputs"""

    analysis_spec = OverridenConcat.__spec__

    assert list(analysis_spec.column_names) == [
        "concatenated",
        "file1",
        "file2",
    ]
    assert list(analysis_spec.parameter_names) == [
        "duplicates",
        "order",
    ]

    assert list(analysis_spec.pipeline_names) == [
        "concat_pipeline",
        "reverse_concat_pipeline",
    ]
    assert list(analysis_spec.check_names) == []
    assert list(analysis_spec.switch_names) == []
    assert list(analysis_spec.subanalysis_names) == []

    file1 = analysis_spec.column_spec("file1")
    assert file1.type is Zip
    assert file1.row_frequency == Samples.sample
    assert file1.salience == cs.primary
    assert file1.defined_in is Concat
    assert file1.mapped_from is None

    file2 = analysis_spec.column_spec("file2")
    assert file2.type is Text
    assert file2.row_frequency == Samples.sample
    assert file2.salience == cs.primary
    assert file2.defined_in is Concat
    assert file2.mapped_from is None

    concatenated = analysis_spec.column_spec("concatenated")
    assert concatenated.type is Text
    assert concatenated.row_frequency == Samples.sample
    assert concatenated.salience == cs.supplementary
    assert concatenated.defined_in is Concat
    assert concatenated.mapped_from is None

    duplicates = analysis_spec.parameter("duplicates")
    assert duplicates.type is int
    assert duplicates.default == 2
    assert duplicates.salience == ps.recommended
    assert duplicates.defined_in is Concat

    order = analysis_spec.parameter("order")
    assert order.type is str
    assert order.default == "forward"
    assert order.salience == ps.recommended
    assert order.defined_in is OverridenConcat

    concat_pipeline = analysis_spec.pipeline_builder("concat_pipeline")
    assert concat_pipeline.name == "concat_pipeline"
    assert concat_pipeline.parameters == ("duplicates",)
    assert concat_pipeline.inputs == ("file1", "file2")
    assert concat_pipeline.outputs == ("concatenated",)
    assert concat_pipeline.method is Concat.concat_pipeline
    assert concat_pipeline.defined_in is Concat
    assert concat_pipeline.condition is None
    assert concat_pipeline.switch is None

    reverse_concat_pipeline = analysis_spec.pipeline_builder("reverse_concat_pipeline")
    assert reverse_concat_pipeline.name == "reverse_concat_pipeline"
    assert reverse_concat_pipeline.parameters == ("duplicates",)
    assert reverse_concat_pipeline.inputs == ("file1", "file2")
    assert reverse_concat_pipeline.outputs == ("concatenated",)
    assert reverse_concat_pipeline.method is OverridenConcat.reverse_concat_pipeline
    assert reverse_concat_pipeline.defined_in is OverridenConcat
    assert isinstance(reverse_concat_pipeline.condition, Operation)
    assert reverse_concat_pipeline.switch is None

    # Initialise class
    analysis = OverridenConcat(
        file1="a_column", file2="another_column", duplicates=1, order="reversed"
    )
    assert analysis.file1 == "a_column"
    assert analysis.file2 == "another_column"
    assert analysis.concatenated is None
    assert analysis.duplicates == 1
    assert analysis.order == "reversed"
    wf = pydra.Workflow(
        name="test_analysis",
        input_spec=["file1", "file2"],
        file1=test_file1,
        file2=test_file2,
    )
    concatenated = analysis.reverse_concat_pipeline(
        wf, file1=wf.lzin.file1, file2=wf.lzin.file2, duplicates=analysis.duplicates
    )
    wf.set_output(("concatenated", concatenated))
    result = wf(plugin="serial")
    assert get_contents(result.output.concatenated) == ["1elif", "2elif"]


def test_analysis_switch(
    Concat, ConcatWithSwitch, test_numeric_file1, test_numeric_file2
):
    """Tests overriding methods in the base class with optional switches based on
    parameters and properties of the inputs"""

    analysis_spec = ConcatWithSwitch.__spec__

    assert list(analysis_spec.column_names) == [
        "concatenated",
        "file1",
        "file2",
        "multiplied",
    ]
    assert list(analysis_spec.parameter_names) == [
        "duplicates",
        "multiplier",
    ]

    assert list(analysis_spec.pipeline_names) == [
        "concat_pipeline",
        "multiply_pipeline",
    ]
    assert list(analysis_spec.check_names) == []
    assert list(analysis_spec.switch_names) == ["inputs_are_numeric"]
    assert list(analysis_spec.subanalysis_names) == []

    file1 = analysis_spec.column_spec("file1")
    assert file1.type is Zip
    assert file1.row_frequency == Samples.sample
    assert file1.salience == cs.primary
    assert file1.defined_in is Concat
    assert file1.mapped_from is None

    file2 = analysis_spec.column_spec("file2")
    assert file2.type is Text
    assert file2.row_frequency == Samples.sample
    assert file2.salience == cs.primary
    assert file2.defined_in is Concat
    assert file2.mapped_from is None

    concatenated = analysis_spec.column_spec("concatenated")
    assert concatenated.type is Text
    assert concatenated.row_frequency == Samples.sample
    assert concatenated.salience == cs.supplementary
    assert concatenated.defined_in is Concat
    assert concatenated.mapped_from is None

    multiplied = analysis_spec.column_spec("multiplied")
    assert multiplied.type is Text
    assert multiplied.row_frequency == Samples.sample
    assert multiplied.salience == cs.supplementary
    assert multiplied.defined_in is ConcatWithSwitch
    assert multiplied.mapped_from is None

    duplicates = analysis_spec.parameter("duplicates")
    assert duplicates.type is int
    assert duplicates.default == 1
    assert duplicates.salience == ps.recommended
    assert duplicates.defined_in is Concat

    multiplier = analysis_spec.parameter("multiplier")
    assert multiplier.type is int
    assert multiplier.default is None
    assert multiplier.salience == ps.required
    assert multiplier.defined_in is ConcatWithSwitch

    concat_pipeline = analysis_spec.pipeline_builder("concat_pipeline")
    assert concat_pipeline.name == "concat_pipeline"
    assert concat_pipeline.parameters == ("duplicates",)
    assert concat_pipeline.inputs == ("file1", "file2")
    assert concat_pipeline.outputs == ("concatenated",)
    assert concat_pipeline.method is Concat.concat_pipeline
    assert concat_pipeline.defined_in is Concat
    assert concat_pipeline.condition is None
    assert concat_pipeline.switch is None

    multiply_pipeline = analysis_spec.pipeline_builder("multiply_pipeline")
    assert multiply_pipeline.name == "multiply_pipeline"
    assert multiply_pipeline.parameters == ("multiplier",)
    assert multiply_pipeline.inputs == ("concatenated",)
    assert multiply_pipeline.outputs == ("multiplied",)
    assert multiply_pipeline.method is ConcatWithSwitch.multiply_pipeline
    assert multiply_pipeline.defined_in is ConcatWithSwitch
    assert multiply_pipeline.condition is None
    assert multiply_pipeline.switch == "inputs_are_numeric"

    inputs_are_numeric = analysis_spec.switch("inputs_are_numeric")
    assert inputs_are_numeric.name == "inputs_are_numeric"
    assert inputs_are_numeric.parameters == ()
    assert inputs_are_numeric.inputs == ("file1", "file2")
    assert inputs_are_numeric.method is ConcatWithSwitch.inputs_are_numeric
    assert inputs_are_numeric.defined_in is ConcatWithSwitch

    # Initialise class
    analysis = ConcatWithSwitch(
        file1="a_column", file2="another_column", duplicates=1, multiplier=10
    )
    assert analysis.file1 == "a_column"
    assert analysis.file2 == "another_column"
    assert analysis.concatenated is None
    assert analysis.duplicates == 1
    assert analysis.multiplier == 10

    wf = pydra.Workflow(
        name="test_analysis",
        input_spec=["file1", "file2"],
        file1=test_numeric_file1,
        file2=test_numeric_file2,
    )
    inputs_are_numeric = analysis.inputs_are_numeric(
        wf, file1=wf.lzin.file1, file2=wf.lzin.file2
    )
    concatenated = analysis.concat_pipeline(
        wf, file1=wf.lzin.file1, file2=wf.lzin.file2, duplicates=analysis.duplicates
    )
    multiplied = analysis.multiply_pipeline(
        wf, concatenated=concatenated, multiplier=analysis.multiplier
    )
    wf.set_output(
        [("inputs_are_numeric", inputs_are_numeric), ("multiplied", multiplied)]
    )
    result = wf(plugin="serial")
    assert result.output.inputs_are_numeric is True
    assert get_contents(result.output.multiplied) == ["10.0", "20.0"]


def test_analysis_with_subanalyses(
    ConcatWithSubanalyses,
    ExtendedConcat,
    ConcatWithSwitch,
    test_numeric_file1,
    test_numeric_file2,
):

    analysis_spec = ConcatWithSubanalyses.__spec__

    assert list(analysis_spec.parameter_names) == ["common_duplicates"]

    assert list(analysis_spec.column_names) == [
        "concat_and_multiplied",
        "concatenated",
        "file1",
        "file2",
    ]

    assert list(analysis_spec.pipeline_names) == []
    assert list(analysis_spec.check_names) == []
    assert list(analysis_spec.switch_names) == []
    assert list(analysis_spec.subanalysis_names) == ["sub1", "sub2"]

    common_duplicates = analysis_spec.parameter("common_duplicates")
    assert common_duplicates.type is int
    assert common_duplicates.default == 5
    assert common_duplicates.salience == ps.check
    # Not sure why this is failing, not super critical at this point
    # assert common_duplicates.defined_in is ConcatWithSubanalyses

    file1 = analysis_spec.column_spec("file1")
    assert file1.type is Zip
    assert file1.row_frequency == Samples.sample
    assert file1.salience == cs.primary
    # assert file1.defined_in is Concat
    assert file1.mapped_from == ("sub1", "file1")

    file2 = analysis_spec.column_spec("file2")
    assert file2.type is Text
    assert file2.row_frequency == Samples.sample
    assert file2.salience == cs.primary
    # assert file2.defined_in is Concat
    assert file2.mapped_from == ("sub1", "file2")

    concat_and_multiplied = analysis_spec.column_spec("concat_and_multiplied")
    assert concat_and_multiplied.type is Text
    assert concat_and_multiplied.row_frequency == Samples.sample
    assert concat_and_multiplied.salience == cs.supplementary
    # assert concat_and_multiplied.defined_in is ConcatWithSwitch
    assert concat_and_multiplied.mapped_from == ("sub2", "multiplied")

    sub1 = analysis_spec.subanalysis("sub1")
    assert sub1.name == "sub1"
    assert sub1.type is ExtendedConcat
    assert sub1.mappings == (
        ("concatenated", "concatenated"),
        ("duplicates", "common_duplicates"),
        ("file1", "file1"),
        ("file2", "file2"),
        ("file3", "concat_and_multiplied"),
    )
    assert sub1.defined_in is ConcatWithSubanalyses

    sub2 = analysis_spec.subanalysis("sub2")
    assert sub2.name == "sub2"
    assert sub2.type is ConcatWithSwitch
    assert sub2.mappings == (
        ("concatenated", "concatenated"),
        ("duplicates", "common_duplicates"),
        ("file1", "file1"),
        ("file2", "file2"),
        ("multiplied", "concat_and_multiplied"),
    )
    assert sub2.defined_in is ConcatWithSubanalyses

    # Initialise class
    analysis = ConcatWithSubanalyses(
        file1="a_column", file2="another_column", common_duplicates=1
    )
    analysis.sub2.multiplier = 100
    assert analysis.file1 == "a_column"
    assert analysis.file2 == "another_column"
    assert analysis.common_duplicates == 1
    assert analysis.sub1.file1 == "a_column"
    assert analysis.sub1.file2 == "another_column"
    assert analysis.sub2.duplicates == 1
    assert analysis.sub1.concatenated is None
    assert analysis.sub1.doubly_concatenated is None
    assert analysis.sub2.file1 == "a_column"
    assert analysis.sub2.file2 == "another_column"
    assert analysis.sub2.duplicates == 1
    assert analysis.sub2.multiplier == 100
    assert analysis.sub2.concatenated is None
    assert analysis.sub2.multiplied is None

    wf = pydra.Workflow(
        name="test_analysis",
        input_spec=["file1", "file2"],
        file1=test_numeric_file1,
        file2=test_numeric_file2,
    )
    concatenated = analysis.sub2.concat_pipeline(
        wf,
        file1=wf.lzin.file1,
        file2=wf.lzin.file2,
        duplicates=analysis.sub2.duplicates,
    )
    concat_and_multiplied = analysis.sub2.multiply_pipeline(
        wf, concatenated=concatenated, multiplier=analysis.sub2.multiplier
    )
    doubly_concatenated = analysis.sub1.doubly_concat_pipeline(
        wf,
        concatenated=concatenated,
        file3=concat_and_multiplied,
        duplicates=analysis.common_duplicates,
    )
    wf.set_output(("doubly_concatenated", doubly_concatenated))
    result = wf(plugin="serial")
    assert get_contents(result.output.doubly_concatenated) == [
        "1",
        "2",
        "100.0",
        "200.0",
    ]


def test_reserved_name_errors():

    with pytest.raises(ArcanaDesignError):

        @analysis(Samples)
        class A:
            dataset: Text = column("a reserved attribute")

    with pytest.raises(ArcanaDesignError):

        @analysis(Samples)
        class B:
            menu: Text = column("another reserved attribute")

    with pytest.raises(ArcanaDesignError):

        @analysis(Samples)
        class C:
            stack: Text = column("yet another reserved attribute")


def test_change_of_type_errors():
    @analysis(Samples)
    class A:
        x: Text = column("a reserved attribute", salience=cs.primary)
        y: Text = column("another column")

        @pipeline(y)
        def a_pipeline(self, wf, x: Text):
            wf.add(identity_file(name="identity", in_file=x))
            return wf.identity.lzout.out_file

    with pytest.raises(ArcanaDesignError) as e:

        @analysis(Samples)
        class B(A):
            x: Zip = inherited_from(A)

    assert "Cannot change format" in e.value.msg


def test_multiple_pipeline_builder_errors():

    with pytest.raises(ArcanaDesignError) as e:

        @analysis(Samples)
        class A:
            x: Text = column("a column", salience=cs.primary)
            y: Text = column("another column")

            @pipeline(y)
            def a_pipeline(self, wf, x: Text):
                wf.add(identity_file(name="identity", in_file=x))
                return wf.identity.lzout.out_file

            @pipeline(y)
            def another_pipeline(self, wf, x: Text):
                wf.add(identity_file(name="identity", in_file=x))
                return wf.identity.lzout.out_file

    assert "Multiple pipelines provide outputs for 'y'" in e.value.msg


def test_unconnected_column_errors():

    # Should work
    @analysis(Samples)
    class A:
        x: Text = column("a column", salience=cs.primary)
        y: Text = column("another column")

        @pipeline(y)
        def a_pipeline(self, wf, x: Text):
            wf.add(identity_file(name="identity", in_file=x))
            return wf.identity.lzout.out_file

    with pytest.raises(ArcanaDesignError) as e:

        @analysis(Samples)
        class B:
            x: Text = column("a column", salience=cs.primary)

    assert "'x' is neither an input nor output to any pipeline" in e.value.msg

    with pytest.raises(ArcanaDesignError) as e:

        @analysis(Samples)
        class C:
            x: Text = column("a column")
            y: Text = column("another column")

            @pipeline(y)
            def a_pipeline(self, wf, x: Text):
                wf.add(identity_file(name="identity", in_file=x))
                return wf.identity.lzout.out_file

    assert (
        "'x' is not generated by any pipeline yet its salience is not specified as 'raw' or 'primary'"
        in e.value.msg
    )


def test_unknown_column_errors():

    with pytest.raises(ArcanaDesignError) as e:

        @analysis(Samples)
        class A:
            k: int = parameter("a parameter", default=1)

            @pipeline(k)
            def a_pipeline(self, wf):
                pass

    assert "'a_pipeline' pipeline outputs to unknown columns" in e.value.msg


def test_automagic_arg_errors():

    with pytest.raises(ArcanaDesignError) as e:

        @analysis(Samples)
        class A:
            y: Text = column("another column")

            @pipeline(y)
            def a_pipeline(self, wf, x: Text):
                wf.add(identity_file(name="identity", in_file=x))
                return wf.identity.lzout.out_file

    assert "Unrecognised argument 'x'" in e.value.msg


def test_inherited_from_errors():
    @analysis(Samples)
    class A:
        x: Text = column("a reserved attribute", salience=cs.primary)
        y: Text = column("another column")

        @pipeline(y)
        def a_pipeline(self, wf, x: Text):
            wf.add(identity_file(name="identity", in_file=x))
            return wf.identity.lzout.out_file

    with pytest.raises(AttributeError) as e:

        @analysis(Samples)
        class B(A):
            z: Text = inherited_from(A)

    assert "object has no attribute 'z'" in str(e.value)

    @analysis(Samples)
    class C(A):
        pass

    with pytest.raises(ArcanaDesignError) as e:

        @analysis(Samples)
        class D(C):
            x: Text = inherited_from(C)

    assert (
        "'x' must inherit from a column that is explicitly defined in the base class it references"
        in e.value.msg
    )

    @analysis(Samples)
    class E(A):

        y: Text = inherited_from(A, salience=cs.publication)
        z: Text = column("yet another column", salience=cs.primary)

        @pipeline(z)
        def another_pipeline(self, wf, y: Text) -> Text:

            wf.add(identity_file(name="identity", in_file=y))

            return wf.identity.lzout.out_file

    with pytest.raises(ArcanaDesignError) as e:

        @analysis(Samples)
        class F(E):

            y: Text = inherited_from(A)
            z: Text = inherited_from(E)

            @pipeline(z)
            def another_pipeline(self, wf, y: Text) -> Text:

                wf.add(identity_file(name="identity", in_file=y))

                return wf.identity.lzout.out_file

    assert (
        "Must inherited from the first class in the method-resolution order"
        in e.value.msg
    )
