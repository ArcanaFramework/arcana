from pydra.engine.task import FunctionTask
from pydra.engine.specs import BaseSpec, SpecInfo, FunctionTask

def identity(**fields):
    return fields

def identity_task(task_name, fields):
    task = FunctionTask(
        identity,
        input_spec=SpecInfo(
            name=f'{task_name}Inputs', bases=(BaseSpec,), fields=(
                [(s, DataItem) for s in sourced]),
        output_spec=SpecInfo(
            name=f'{task_name}Outputs', bases=(BaseSpec,), fields=[
                [('data_node', DataNode]))
    return task

