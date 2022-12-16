import typing as ty
import json
from pydra import mark
from pydra.engine.core import File
from pydra.engine.specs import BaseSpec, SpecInfo
from pydra.engine.task import FunctionTask
from arcana.core.data.type.base import DataType, FileGroup
from arcana.core.data.row import DataRow


def identity(**fields):
    return fields


def identity_task(task_name, fields):
    task = FunctionTask(
        identity,
        input_spec=SpecInfo(
            name=f"{task_name}Inputs",
            bases=(BaseSpec,),
            fields=[(s, DataType) for s in fields],
        ),
        output_spec=SpecInfo(
            name=f"{task_name}Outputs", bases=(BaseSpec,), fields=[("row", DataRow)]
        ),
    )
    return task


@mark.task
@mark.annotate({"in_file": FileGroup, "return": {"out_file": FileGroup}})
def identity_converter(in_file):
    return in_file


@mark.task
def extract_from_json(in_file: File, field_name: str) -> ty.Any:
    with open(in_file) as f:
        dct = json.load(f)
    return dct[field_name]  # FIXME: Should use JSONpath syntax
