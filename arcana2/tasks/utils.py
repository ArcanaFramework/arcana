from pydra.engine.task import FunctionTask
from pydra import mark
from pydra.engine.specs import BaseSpec, SpecInfo
from pydra.engine.task import FunctionTask
from arcana2.core.data.item import DataItem, FileGroup
from arcana2.core.data.node import DataNode

def identity(**fields):
    return fields

def identity_task(task_name, fields):
    task = FunctionTask(
        identity,
        input_spec=SpecInfo(
            name=f'{task_name}Inputs', bases=(BaseSpec,), fields=[
                (s, DataItem) for s in fields]),
        output_spec=SpecInfo(
            name=f'{task_name}Outputs', bases=(BaseSpec,), fields=[
                ('data_node', DataNode)]))
    return task


@mark.task
@mark.annotate({
    'in_file': FileGroup,
    'return': {
        'out_file': FileGroup}})
def identity_converter(in_file):
    return in_file
