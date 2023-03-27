from __future__ import annotations
from operator import itemgetter
import pytest
from fileformats.text import Plain as PlainText
from arcana.core.exceptions import ArcanaUsageError, ArcanaDataTreeConstructionError
from arcana.dirtree import DirTree
from arcana.testing.data.blueprint import TestDatasetBlueprint, FileSetEntryBlueprint
from arcana.testing.data.space import TestDataSpace


TEST_INCLUSIONS = {
    "all": (
        TestDatasetBlueprint(  # dataset name
            space=TestDataSpace,
            hierarchy=["a", "b", "c", "abcd"],
            dim_lengths=[1, 2, 3, 4],
            entries=[
                FileSetEntryBlueprint(
                    path="file1", datatype=PlainText, filenames=["file1.txt"]
                ),
            ],
            id_patterns={"d": r"abcd::.*(d\d+)"},
        ),
        [
            "a0b0c0d0",
            "a0b0c0d1",
            "a0b0c0d2",
            "a0b0c0d3",
            "a0b0c1d0",
            "a0b0c1d1",
            "a0b0c1d2",
            "a0b0c1d3",
            "a0b0c2d0",
            "a0b0c2d1",
            "a0b0c2d2",
            "a0b0c2d3",
            "a0b1c0d0",
            "a0b1c0d1",
            "a0b1c0d2",
            "a0b1c0d3",
            "a0b1c1d0",
            "a0b1c1d1",
            "a0b1c1d2",
            "a0b1c1d3",
            "a0b1c2d0",
            "a0b1c2d1",
            "a0b1c2d2",
            "a0b1c2d3",
        ],
    ),
    "include": (
        TestDatasetBlueprint(  # dataset name
            space=TestDataSpace,
            hierarchy=["a", "b", "c", "abcd"],
            dim_lengths=[1, 2, 3, 4],
            entries=[
                FileSetEntryBlueprint(
                    path="file1", datatype=PlainText, filenames=["file1.txt"]
                ),
            ],
            include={
                "c": ["c1", "c2"],
                "a": "a0",
                "d": ["d1", "d2", "d3"],
            },
            id_patterns={"d": r"abcd::.*(d\d+)"},
        ),
        [
            "a0b0c1d1",
            "a0b0c1d2",
            "a0b0c1d3",
            "a0b0c2d1",
            "a0b0c2d2",
            "a0b0c2d3",
            "a0b1c1d1",
            "a0b1c1d2",
            "a0b1c1d3",
            "a0b1c2d1",
            "a0b1c2d2",
            "a0b1c2d3",
        ],
    ),
    "exclude": (
        TestDatasetBlueprint(  # dataset name
            space=TestDataSpace,
            hierarchy=["a", "b", "c", "abcd"],
            dim_lengths=[1, 2, 3, 4],
            entries=[
                FileSetEntryBlueprint(
                    path="file1", datatype=PlainText, filenames=["file1.txt"]
                ),
            ],
            exclude={
                "c": ["c2"],
                "abcd": ["a0b0c1d1", "a0b1c1d2"],
                "d": ["d3"],
            },
            id_patterns={"d": r"abcd::.*(d\d+)"},
        ),
        [
            "a0b0c0d0",
            "a0b0c0d1",
            "a0b0c0d2",
            "a0b0c1d0",
            "a0b0c1d2",
            "a0b1c0d0",
            "a0b1c0d1",
            "a0b1c0d2",
            "a0b1c1d0",
            "a0b1c1d1",
        ],
    ),
    "regex": (
        TestDatasetBlueprint(  # dataset name
            space=TestDataSpace,
            hierarchy=["a", "b", "c", "abcd"],
            dim_lengths=[1, 2, 3, 4],
            entries=[
                FileSetEntryBlueprint(
                    path="file1", datatype=PlainText, filenames=["file1.txt"]
                ),
            ],
            id_patterns={"bc": r"BC#b:id##c:id#"},
            include={
                "abcd": r"a\d+b\dc(\d+)d\1",
            },
            exclude={"bc": r"BC(\d)\1"},
        ),
        [
            "a0b0c0d0",
            "a0b0c1d1",
            "a0b0c2d2",
            "a0b1c0d0",
            "a0b1c1d1",
            "a0b1c2d2",
        ],
    ),
}


@pytest.mark.parametrize("fixture", TEST_INCLUSIONS.items(), ids=itemgetter(0))
def test_dataset_inclusion(
    fixture: tuple[str, tuple[TestDatasetBlueprint, list[str]]], work_dir
):
    test_name, (blueprint, expected) = fixture
    dataset_path = work_dir / test_name
    dataset = blueprint.make_dataset(store=DirTree(), dataset_id=dataset_path)
    assert sorted(dataset.row_ids()) == expected


def test_include_exclude_fail(work_dir):

    blueprint = TestDatasetBlueprint(  # dataset name
        space=TestDataSpace,
        hierarchy=["a", "b", "c", "abcd"],
        dim_lengths=[1, 2, 3, 4],
        entries=[
            FileSetEntryBlueprint(
                path="file1", datatype=PlainText, filenames=["file1.txt"]
            ),
        ],
        include={"session": r"a0.*d1.*"},
        exclude={"session": r"a0.*d2.*"},
    )
    with pytest.raises(
        ArcanaUsageError, match="Cannot provide both 'include' and 'exclude' arguments"
    ):
        blueprint.make_dataset(
            store=DirTree(), dataset_id=work_dir / "include-exclude-fail"
        )


def test_requires_id_pattern(work_dir):

    blueprint = TestDatasetBlueprint(  # dataset name
        space=TestDataSpace,
        hierarchy=["a", "b", "c", "abcd"],
        dim_lengths=[1, 2, 3, 4],
        entries=[
            FileSetEntryBlueprint(
                path="file1", datatype=PlainText, filenames=["file1.txt"]
            ),
        ],
    )
    with pytest.raises(
        ArcanaDataTreeConstructionError,
        match="ID clash between rows inserted into data tree",
    ):
        blueprint.make_dataset(
            store=DirTree(), dataset_id=work_dir / "include-exclude-fail"
        )
