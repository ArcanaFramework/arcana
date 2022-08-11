import pytest
from arcana.data.spaces.common import Samples
from arcana.test.tasks import concatenate, concatenate_reverse, multiply_contents
from arcana.core.mark import (
    analysis,
    pipeline,
    parameter,
    column,
    inherit,
    Equals,
    switch,
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

        @pipeline("concatenated")
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
        def inputs_are_numeric(self, file1: Text, file2: Text):
            for file in (file1, file2):
                with open(file.fs_path) as f:
                    contents = f.read()
                try:
                    float(contents.strip())
                except ValueError:
                    return False
            return True

        @pipeline(concatenated, condition=Equals(order, "reversed"))
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
