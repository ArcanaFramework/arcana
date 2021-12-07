import attr
from typing import Any
from pydra import Workflow, mark
from pydra.engine.core import TaskBase
from .type import FileFormat
from .item import FileGroup
from pydra.engine.task import FunctionTask
from pydra.engine.specs import BaseSpec, SpecInfo


@mark.task
@mark.annotate(
    {'file_group': Any,
    'return': {
        'path': str}})
def get_paths(file_group: Any, **kwargs):
    """Copies files into the CWD renaming so the basenames match
    except for extensions"""
    cpy = file_group.copy_to('./file-group', symlink=True)
    return cpy.fs_path


def collect_paths(file_format, **cache_paths):
    """Copies files into the CWD renaming so the basenames match
    except for extensions"""
    file_path = cache_paths.pop('path')
    side_cars = cache_paths if cache_paths else None
    return file_format.from_path(file_path, side_cars=side_cars)


@attr.s
class DataConverter:

    to_format: FileFormat = attr.ib()
    task: TaskBase = attr.ib()
    inputs: dict[str, str] = attr.ib()
    outputs: dict[str, str] = attr.ib()
    task_kwargs: dict[str, Any] = attr.ib()
    
    def __call__(self, name, **kwargs):
        """
        Create a Pydra workflow to perform the conversion from one file group
        to another by wrapping up 
        """
        
        wf = Workflow(name=name, input_spec=['to_convert'])

        # Add task collect the input paths to a common directory (as we
        # assume the converter expects)
        wf.add(get_paths(name='get_paths', in_file=wf.lzin.to_convert))

        # Add the actual converter node
        conv_kwargs = {self.inputs[i]:
                        getattr(wf.get_paths.lzout, i) for i in self.inputs}
        conv_kwargs.update(self.task_kwargs)
        conv_kwargs.update(kwargs)
        wf.add(self.task(name='converter', **conv_kwargs))

        collect_paths_task = FunctionTask(
            collect_paths,
            input_spec=SpecInfo(
                name=f'{name}Inputs',
                bases=(BaseSpec,),
                fields=[('path', str)]),
            output_spec=SpecInfo(
                name=f'{name}Outputs',
                bases=(BaseSpec,),
                fields=[('file_group', Any)]),
            name='output_interface')

        wf.add(
            collect_paths_task(
                name='collect_paths',
                file_format=self.to_format,
                **{o: getattr(wf.converter.lzout, self.outputs[o])
                for o in self.outputs}))

        # Set the outputs of the workflow
        wf.set_output(('converted', wf.collect_paths.lzout.file_group))

        return wf
