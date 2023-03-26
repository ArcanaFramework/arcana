from operator import itemgetter
import pytest
from arcana.core.data.store import DataStore
from arcana.core.exceptions import ArcanaDataTreeConstructionError


ID_INFERENCE_TESTS = {
    "basic": (
        {"abcd": "a1b2c3d4"},
        {
            "a": r"abcd::a(\d+)b\d+c\d+d\d+",
            "b": r"abcd::a\d+b(\d+)c\d+d\d+",
            "c": r"abcd::a\d+b\d+c(\d+)d\d+",
            "d": r"abcd::a\d+b\d+c\d+d(\d+)",
        },
        {},
        {
            "a": "1",
            "b": "2",
            "c": "3",
            "d": "4",
        },
    ),
    "realistic": (
        {"subject": "CONTROL99"},
        {
            "group": r"subject:id:([A-Z]+).*",
            "member": r"subject:ID:[A-Z]+(\d+)",
        },
        {},
        {
            "group": "CONTROL",
            "member": "99",
        },
    ),
    "metadata": (
        {"subject": "FTD01111", "session": "FTDS9999"},
        {
            "timepoint": r"session:order",
        },
        {"session": {"order": 2}},
        {
            "timepoint": "2",
        },
    ),
    "templating": (
        {"subject": "FTD01111", "session": "MR999"},
        {
            "timepoint": r"#session::([A-Z]+).*##session:visit_id#",
        },
        {"session": {"visit_id": 3}},
        {
            "timepoint": "MR3",
        },
    ),
}


@pytest.mark.parametrize(
    "fixture",
    ID_INFERENCE_TESTS.items(),
    ids=itemgetter(0),
)
def test_id_inference(fixture):
    test_name, (explicit_ids, id_patterns, metadata, expected_ids) = fixture
    assert expected_ids == DataStore.infer_ids(
        ids=explicit_ids, id_patterns=id_patterns, metadata=metadata
    )


def test_id_inference_fail1():
    with pytest.raises(
        ArcanaDataTreeConstructionError, match="Provided ID-pattern component"
    ):
        DataStore.infer_ids(ids={"ab": "a0b0"}, id_patterns={"a": r"ab::xxx"})


def test_id_inference_fail2():
    with pytest.raises(
        ArcanaDataTreeConstructionError, match="Provided ID-pattern component"
    ):
        DataStore.infer_ids(ids={"ab": "a0b0"}, id_patterns={"a": r"ab::a\d+b\d+"})


def test_id_inference_fail3():
    with pytest.raises(
        ArcanaDataTreeConstructionError, match="Provided ID-pattern component"
    ):
        DataStore.infer_ids(ids={"ab": "a0b0"}, id_patterns={"a": r"ab::a(\d+)b(\d+)"})


def test_id_inference_fail4():
    with pytest.raises(
        ArcanaDataTreeConstructionError, match="row doesn't have the metadata field"
    ):
        DataStore.infer_ids(ids={"ab": "a0b0"}, id_patterns={"a": r"ab:metadata_field"})


def test_id_inference_fail5():
    with pytest.raises(
        ArcanaDataTreeConstructionError,
        match="Inferred IDs from decomposition conflict",
    ):
        DataStore.infer_ids(ids={"ab": "a0b0"}, id_patterns={"ab": r"ab::a(\d+)b\d+"})
