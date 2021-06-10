from copy import copy
import typing as ty
import attr
from pydra.engine.task import FunctionTask
from pydra.engine.specs import BaseSpec, SpecInfo


def generate_task(fields):

    def task(**fields):
        return tuple(fields.values())

    input_spec = SpecInfo(name="Inputs", fields=fields, bases=(BaseSpec,))
    output_spec = SpecInfo(name="Outputs", fields=fields, bases=(BaseSpec,))
        
    return FunctionTask(task, input_spec=input_spec, output_spec=output_spec)

my_task_cls = generate_task(['a', 'b', 'c'])

my_task = my_task_cls()

result = my_task(a=1, b=2, c=3)

print(result)