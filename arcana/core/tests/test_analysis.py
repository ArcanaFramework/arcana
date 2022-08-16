import pytest
import pydra
from arcana.data.spaces.common import Samples
from arcana.test.tasks import (
    concatenate,
    concatenate_reverse,
    multiply_contents,
    check_contents_are_numeric,
)
from arcana.core.mark import (
    analysis,
    pipeline,
    parameter,
    column,
    inherit_from,
    value_of,
    switch,
    is_provided,
    check,
)
from arcana.data.formats.common import Zip, Text
from arcana.core.enum import ColumnSalience as cs, ParameterSalience as ps


@pytest.fixture
def concat_cls():
    @analysis(Samples)
    class Concat:

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

            return wf.a_node.lzout.out

    return Concat


def test_analysis_basic(concat_cls):

    assert list(concat_cls.__parameters__) == ["duplicates"]

    assert sorted(concat_cls.__column_specs__) == [
        "concatenated",
        "file1",
        "file2",
    ]

    duplicates = concat_cls.__parameters__["duplicates"]
    assert duplicates.type is int
    assert duplicates.default == 1
    assert duplicates.salience == ps.recommended
    assert not duplicates.inherited

    file1 = concat_cls.__column_specs__["file1"]
    assert file1.type is Zip
    assert file1.row_frequency == Samples.sample
    assert file1.salience == cs.primary
    assert not file1.inherited
    assert not file1.checks

    file2 = concat_cls.__column_specs__["file2"]
    assert file2.type is Text
    assert file2.row_frequency == Samples.sample
    assert file2.salience == cs.primary
    assert not file2.inherited
    assert not file2.checks

    concatenated = concat_cls.__column_specs__["concatenated"]
    assert concatenated.type is Text
    assert concatenated.row_frequency == Samples.sample
    assert concatenated.salience == cs.supplementary
    assert not concatenated.inherited
    assert sorted(concatenated.pipelines) == [None]
    assert not concatenated.checks

    concat_pipeline = concatenated.pipelines[None]
    assert concat_pipeline.name == "concat_pipeline"
    assert concat_pipeline.parameters == [duplicates]
    assert concat_pipeline.inputs == [file1, file2]
    assert concat_pipeline.outputs == [concatenated]
    assert concat_pipeline.method is concat_cls.concat_pipeline


def test_analysis_extend(concat_cls):
    @analysis(Samples)
    class ExtendedConcat(concat_cls):

        concatenated = inherit_from(concat_cls)
        file3: Text = column("Another file to concatenate", salience=cs.primary)

        doubly_concatenated: Text = column("The doubly concatenated file")

        duplicates = inherit_from(concat_cls)
        second_duplicates: int = parameter(
            "The number of times to duplicate the second concatenation", default=1
        )

        @pipeline(doubly_concatenated)
        def doubly_concat_pipeline(
            self, wf, concatenated: Text, file3: Text, second_duplicates: int
        ):

            wf.add(
                concatenate(
                    name="concat",
                    in_file1=concatenated,
                    in_file2=file3,
                    duplicates=second_duplicates,
                )
            )

            return wf.concat.lzout.out

        @check(file3)
        def num_lines_check(self, wf, file3: Text, duplicates: int):
            """Checks the number of lines in the concatenated file to see whether they
            match what is expected for the number of duplicates specified"""

            @pydra.mark.task
            def num_lines_equals(in_file, num_lines):
                with open(in_file) as f:
                    contents = f.read()
                return len(contents.splitlines()) == num_lines

            wf.add(
                num_lines_equals(
                    in_file=file3, num_lines=2 * duplicates, name="num_lines_check"
                )
            )

            return wf.num_lines_check.out

    assert sorted(ExtendedConcat.__parameters__) == ["duplicates", "second_duplicates"]

    assert sorted(ExtendedConcat.__column_specs__) == [
        "concatenated",
        "doubly_concatenated",
        "file1",
        "file2",
        "file3",
    ]

    duplicates = ExtendedConcat.__parameters__["duplicates"]
    assert duplicates.type is int
    assert duplicates.default == 1
    assert duplicates.salience == ps.recommended
    assert duplicates.inherited

    second_duplicates = ExtendedConcat.__parameters__["second_duplicates"]
    assert second_duplicates.type is int
    assert second_duplicates.default == 1
    assert second_duplicates.salience == ps.recommended
    assert not second_duplicates.inherited

    file1 = ExtendedConcat.__column_specs__["file1"]
    assert file1.type is Zip
    assert file1.row_frequency == Samples.sample
    assert file1.salience == cs.primary
    assert file1.inherited
    assert not file1.checks

    file2 = ExtendedConcat.__column_specs__["file2"]
    assert file2.type is Text
    assert file2.row_frequency == Samples.sample
    assert file2.salience == cs.primary
    assert file2.inherited
    assert not file2.checks

    file3 = ExtendedConcat.__column_specs__["file3"]
    assert file3.type is Text
    assert file3.row_frequency == Samples.sample
    assert file3.salience == cs.primary
    assert not file3.inherited
    assert len(file3.checks) == 1

    file3_check = file3.checks[0]
    assert file3_check.name == "num_lines_check"

    concatenated = ExtendedConcat.__column_specs__["concatenated"]
    assert concatenated.type is Text
    assert concatenated.row_frequency == Samples.sample
    assert concatenated.salience == cs.supplementary
    assert concatenated.inherited
    assert not concatenated.checks
    assert sorted(concatenated.pipelines) == [None]

    doubly_concatenated = ExtendedConcat.__column_specs__["doubly_concatenated"]
    assert doubly_concatenated.type is Text
    assert doubly_concatenated.row_frequency == Samples.sample
    assert not doubly_concatenated.inherited
    assert not doubly_concatenated.checks
    assert doubly_concatenated.salience == cs.supplementary

    concat_pipeline = concatenated.pipelines[None]
    assert concat_pipeline.name == "concat_pipeline"
    assert concat_pipeline.parameters == [duplicates]
    assert concat_pipeline.inputs == [file1, file2]
    assert concat_pipeline.outputs == [concatenated]
    assert concat_pipeline.method is concat_cls.concat_pipeline


def test_analysis_override(concat_cls):
    """Tests overriding methods in the base class with optional switches based on
    parameters and properties of the inputs"""

    @analysis(Samples)
    class OverridenConcat(concat_cls):

        file1: Zip = inherit_from(concat_cls)
        file2: Text = inherit_from(concat_cls)
        concatenated: Text = inherit_from(concat_cls)
        multiplied: Text = column("contents of the concatenated files are multiplied")

        duplicates = inherit_from(concat_cls, default=2)
        multiplier: int = parameter(
            "the multiplier used to apply", salience=ps.arbitrary
        )
        order: str = parameter(
            "perform the concatenation in reverse order, i.e. file2 and then file1",
            choices=["forward", "reversed"],
            default="forward",
        )

        @switch
        def inputs_are_numeric(self, wf, file1: Text, file2: Text):

            wf.add(check_contents_are_numeric(in_file=file1, name="check_file1"))

            wf.add(check_contents_are_numeric(in_file=file2, name="check_file2"))

            @pydra.mark.task
            def boolean_and(val1, val2) -> bool:
                return val1 and val2

            wf.add(
                boolean_and(
                    val1=wf.check_file1.out, val2=wf.check_file2.out, name="bool_and"
                )
            )

            return wf.bool_and.out

        @pipeline(
            concatenated,
            condition=(
                value_of(order) == "reversed"
                and is_provided(file1)
                and value_of(multiplier) < 10
            ),
        )
        def reverse_concat_pipeline(
            self, wf, file1: Text, file2: Text, duplicates: int
        ):

            wf.add(
                concatenate_reverse(
                    name="concat", in_file1=file1, in_file2=file2, duplicates=duplicates
                )
            )

            return wf.concat.lzout.out

        @pipeline(multiplied, condition=inputs_are_numeric)
        def multiply_pipeline(self, wf, concatenated, multiplier):

            wf.add(
                multiply_contents(
                    name="concat", in_file=concatenated, multiplier=multiplier
                )
            )

            return wf.concat.lzout.out

    assert sorted(OverridenConcat.__column_specs__) == [
        "concatenated",
        "file1",
        "file2",
        "multiplied",
    ]
    assert sorted(OverridenConcat.__parameters__) == [
        "duplicates",
        "multiplier",
        "order",
    ]

    file1 = OverridenConcat.__column_specs__["file1"]
    assert file1.type is Zip
    assert file1.row_frequency == Samples.sample
    assert file1.salience == cs.primary
    assert file1.inherited
    assert not file1.checks

    file2 = OverridenConcat.__column_specs__["file2"]
    assert file2.type is Text
    assert file2.row_frequency == Samples.sample
    assert file2.salience == cs.primary
    assert file2.inherited
    assert not file2.checks

    concatenated = OverridenConcat.__column_specs__["concatenated"]
    assert concatenated.type is Text
    assert concatenated.row_frequency == Samples.sample
    assert concatenated.salience == cs.supplementary
    assert concatenated.inherited
    assert not concatenated.checks

    multiplied = OverridenConcat.__column_specs__["multiplied"]
    assert multiplied.type is Text
    assert multiplied.row_frequency == Samples.sample
    assert multiplied.salience == cs.supplementary
    assert not multiplied.inherited
    assert not multiplied.checks

    duplicates = OverridenConcat.__parameters__["duplicates"]
    assert duplicates.type is int
    assert duplicates.default == 2
    assert duplicates.salience == ps.recommended
    assert multiplied.inherited

    multiplier = OverridenConcat.__parameters__["multiplier"]
    assert multiplier.type is int
    assert multiplier.default is None
    assert multiplier.salience == ps.arbitrary
    assert not multiplied.inherited

    order = OverridenConcat.__parameters__["order"]
    assert order.type is str
    assert order.default == "forward"
    assert order.salience == ps.recommended
    assert not multiplied.inherited
