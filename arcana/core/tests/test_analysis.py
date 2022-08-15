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
    inherit,
    value_of,
    switch,
    is_provided,
    check,
)
from arcana.data.formats.common import Zip, Text
from arcana.core.enum import ParameterSalience as ps


@pytest.fixture
def concat_cls():
    @analysis(Samples)
    class Concat:

        file1: Zip = column("an arbitrary text file")
        file2: Text = column("another arbitrary text file")
        concatenated: Text = column("the output of concatenating file1 and file2")

        duplicates: int = parameter(
            "the number of times to duplicate the concatenation", default=1
        )

        @pipeline(concatenated)
        def a_pipeline(self, wf, file1: Text, file2: Text, duplicates: int):

            wf.add(
                concatenate(
                    name="a_node", in_file1=file1, in_file2=file2, duplicates=duplicates
                )
            )

            return wf.a_node.lzout.out

    return Concat


def test_analysis_basic(concat_cls):

    assert list(concat_cls.__column_specs__) == [
        "file1",
        "file2",
        "concatenated",
    ]
    assert list(concat_cls.__parameters__) == ["duplicates"]


def test_analysis_extend(concat_cls):
    @analysis(Samples)
    class ExtendedConcat(concat_cls):

        concatenated = inherit(concat_cls.concatenated)
        file3: Text = column("Another file to concatenate")

        doubly_concatenated: Text = column("The doubly concatenated file")

        duplicates = inherit(concat_cls.duplicates)
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
        def check_file3(self, wf, file3: Text, duplicates: int):
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

    assert list(ExtendedConcat.__column_specs__) == [
        "file1",
        "file2",
        "concatenated",
        "file3",
        "doubly_concatenated",
    ]
    assert list(ExtendedConcat.__parameters__) == ["duplicates", "second_duplicates"]


def test_analysis_override(concat_cls):
    """Tests overriding methods in the base class with optional switches based on
    parameters and properties of the inputs"""

    @analysis(Samples)
    class OverridenConcat(concat_cls):

        file1: Zip = inherit(concat_cls.file1)
        file2: Text = inherit(concat_cls.file2)
        concatenated: Text = inherit(concat_cls.concatenated)
        multiplied: Text = column("contents of the concatenated files are multiplied")

        duplicates = inherit(concat_cls.duplicates, default=2)
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

    assert list(OverridenConcat.__column_specs__) == [
        "file1",
        "file2",
        "concatenated",
        "multiplied",
    ]
    assert list(OverridenConcat.__parameters__) == ["duplicates", "multiplier", "order"]
