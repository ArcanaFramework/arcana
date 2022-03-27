import attr
import re
import tempfile
import typing as ty
import shutil
from pathlib import Path
from arcana import __version__
from pydra import Workflow, mark
from pydra.engine.task import (
    DockerTask, SingularityTask, ShellCommandTask)
from pydra.engine.specs import (
    LazyField, ShellSpec, SpecInfo, DockerSpec, SingularitySpec, ShellOutSpec)
from arcana.core.data.set import Dataset
from arcana.data.spaces.medimage import Clinical
from arcana.exceptions import ArcanaUsageError
from arcana.core.utils import func_task


def outputs_converter(outputs):
    """Sets the path of an output to '' if not provided or None"""
    return [o[:2] + ('',) if len(o) < 3 or o[2] is None else o for o in outputs]


@attr.s
class BidsApp:

    app_name: str = attr.ib(
        metadata={'help_string': 
            "Name of the BIDS app. Will be used to name the 'derivatives' "
            "sub-directory where the app outputs are stored"})
    image: str = attr.ib(
        metadata={'help_string': 'Name of the BIDS app image to wrap'})
    executable: str = attr.ib(
        metadata={'help_string': 'Name of the executable within the image to run (i.e. the entrypoint of the image). Required when extending the base image and launching Arcana within it'})
    inputs: ty.List[ty.Tuple[str, type, str]] = attr.ib(
        metadata={'help_string': (
            "The inputs to be inserted into the BIDS dataset (NAME, DTYPE, BIDS_PATH)")})
    outputs: ty.List[ty.Tuple[str, type, ty.Optional[str]]] = attr.ib(
        converter=outputs_converter,
        metadata={'help_string': (
            "The outputs to be extracted from the derivatives directory (NAME, DTYPE, BIDS_PATH)")})
    parameters: ty.Dict[str, type] = attr.ib(
        metadata={'help_string': 'The parameters of the app to be exposed to the interface'},
        default=None)

    def __call__(self, name=None, frequency: Clinical or str=Clinical.session,
                 virtualisation: str=None, dataset: ty.Optional[ty.Union[str, Path, Dataset]]=None) -> Workflow:
        """Creates a Pydra workflow which takes inputs and maps them to
        a BIDS dataset, executes a BIDS app and extracts outputs from
        the derivatives stored back in the BIDS dataset

        Parameters
        ----------
        name : str
            Name for the workflow
        frequency : Clinical
            Frequency to run the app at, i.e. per-"session" or per-"dataset"
        virtualisation : str or None
            The virtualisation method to run the main app task, can be one of
            None, 'docker' or 'singularity'
        dataset : str or Dataset
            The dataset to run the BIDS app on. If a string or Path is provided
            then a new BIDS dataset is created at that location with a single
            subject (sub-DEFAULT). If nothing is provided then a dataset is
            created at './bids_dataset'.

        Returns
        -------
        pydra.Workflow
            A Pydra workflow 
        """
        if self.parameters is None:
            parameters = {}
        if isinstance(frequency, str):
            frequency = Clinical[frequency]
        if name is None:
            name = self.app_name

        # Create BIDS dataset to hold translated data
        if dataset is None:
            dataset = Path(tempfile.mkdtemp()) / 'bids_dataset'
        if not isinstance(dataset, Dataset):
            dataset = BidsDataset.create(
                path=dataset,
                name=name + '_dataset',
                subject_ids=[self.DEFAULT_ID])

        # Ensure output paths all start with 'derivatives
        input_names = [i[0] for i in self.inputs]
        output_names = [o[0] for o in self.outputs]
        workflow = Workflow(
            name=name,
            input_spec=input_names + list(parameters) + ['id'])

        # Check id startswith 'sub-' as per BIDS
        workflow.add(bidsify_id(name='bidsify_id', id=workflow.lzin.id))

        # Can't use a decorated function as we need to allow for dynamic
        # arguments
        workflow.add(func_task(
            to_bids,
            in_fields=(
                [('frequency', Clinical),
                 ('inputs', ty.List[ty.Tuple[str, type, str]]),
                 ('dataset', Dataset or str),
                 ('id', str)]
                + [(i, str) for i in input_names]),
            out_fields=[('dataset', BidsDataset)],
            name='to_bids',
            frequency=frequency,
            inputs=self.inputs,
            dataset=dataset,
            id=workflow.bidsify_id.lzout.out,
            **{i: getattr(workflow.lzin, i) for i in input_names}))

        workflow.add(dataset_paths(
            app_name=self.app_name,
            dataset=workflow.to_bids.lzout.dataset,
            id=workflow.bidsify_id.lzout.out))
            
        app_completed = self.add_main_task(
            workflow=workflow,
            dataset_path=workflow.dataset_paths.lzout.base,
            output_path=workflow.dataset_paths.lzout.output,
            parameters={p: type(p) for p in parameters},
            frequency=frequency,
            id=workflow.bidsify_id.lzout.no_prefix,
            virtualisation=virtualisation)

        workflow.add(func_task(
            extract_bids,
            in_fields=[
                ('dataset', Dataset),
                ('frequency', Clinical),
                ('outputs', ty.List[ty.Tuple[str, type, str]]),
                ('path_prefix', str),
                ('id', str),
                ('app_completed', bool)],
            out_fields=[(o, str) for o in output_names],
            name='extract_bids',
            dataset=workflow.to_bids.lzout.dataset,
            path_prefix=workflow.dataset_paths.lzout.path_prefix,
            frequency=frequency,
            outputs=self.outputs,
            id=workflow.bidsify_id.lzout.out,
            app_completed=app_completed))

        for output_name in output_names:
            workflow.set_output(
                (output_name, getattr(workflow.extract_bids.lzout, output_name)))

        return workflow

    def add_main_task(self,
                      workflow: Workflow,
                      dataset_path: LazyField,
                      output_path: LazyField,
                      frequency: Clinical,
                      id: str,
                      parameters: ty.Dict[str, type]=None,
                      virtualisation=None) -> ShellCommandTask:

        if parameters is None:
            parameters = {}

        input_fields = [
            ("dataset_path", str,
             {"help_string": "Path to BIDS dataset in the container",
              "position": 1,
              "mandatory": True,
              "argstr": ""}),
            ("output_path", str,
             {"help_string": "Directory where outputs will be written in the container",
              "position": 2,
              "argstr": ""}),
            ("analysis_level", str,
             {"help_string": "The analysis level the app will be run at",
              "position": 3,
              "argstr": ""}),
            ("participant_label", ty.List[str],
             {"help_string": "The IDs to include in the analysis",
              "argstr": "--participant_label ",
              "position": 4})]

        output_fields=[
            ("completed", bool,
             {"help_string": "a simple flag to indicate app has completed",
              "callable": lambda: True})]

        for param, dtype in parameters.items():
            argstr = f'--{param}'
            if dtype is not bool:
                argstr += ' %s'
            input_fields.append((
                param, dtype, {
                    "help_string": f"Optional parameter {param}",
                    "argstr": argstr}))

        kwargs = {p: getattr(workflow.lzin, p) for p in parameters}

        if virtualisation is None:
            task_cls = ShellCommandTask
            base_spec_cls = ShellSpec
            kwargs['executable'] = self.executable
            app_output_path = output_path
        else:

            workflow.add(make_bindings(
                name='make_bindings',
                dataset_path=dataset_path))

            kwargs['bindings'] = workflow.make_bindings.lzout.bindings

            # Set input and output directories to "internal" paths within the
            # container
            dataset_path = self.CONTAINER_DATASET_PATH
            app_output_path = self.CONTAINER_DERIV_PATH
            kwargs['image'] = self.image

            if virtualisation == 'docker':
                task_cls = DockerTask
                base_spec_cls = DockerSpec
            elif virtualisation == 'singularity':
                task_cls = SingularityTask
                base_spec_cls = SingularitySpec
            else:
                raise ArcanaUsageError(
                    f"Unrecognised container type {virtualisation} "
                    "(can be docker or singularity)")

        if frequency == Clinical.session:
            analysis_level = 'participant'
            kwargs['participant_label'] = id
        else:
            analysis_level = 'group'

        workflow.add(task_cls(
            name='bids_app',
            input_spec=SpecInfo(name="Input", fields=input_fields,
                                bases=(base_spec_cls,)),
            output_spec=SpecInfo(name="Output", fields=output_fields,
                                 bases=(ShellOutSpec,)),
            dataset_path=dataset_path,
            output_path=app_output_path,
            analysis_level=analysis_level,
            **kwargs))

        if virtualisation is not None:
            workflow.add(copytree(
                name='copy_output_dir',
                src=workflow.make_bindings.lzout.tmp_output_dir,
                dest=output_path,
                app_completed=workflow.bids_app.lzout.completed))
            completed = workflow.copy_output_dir.lzout.out
        else:
            completed = workflow.bids_app.lzout.completed

        return completed

    # For running 
    CONTAINER_DERIV_PATH = '/arcana_bids_outputs'
    CONTAINER_DATASET_PATH = '/arcana_bids_dataset'

    DEFAULT_ID = 'sub-DEFAULT'


@mark.task
@mark.annotate(
    {'return':
        {'out': str,
         'no_prefix': str}})
def bidsify_id(id):
    if id == attr.NOTHING:
        id = BidsApp.DEFAULT_ID
    else:
        id = re.sub(r'[^a-zA-Z0-9]', '', id)
        if not id.startswith('sub-'):
            id = 'sub-' + id
    return id, id[len('sub-'):]


def to_bids(frequency, inputs, dataset, id, **input_values):
    """Takes generic inptus and stores them within a BIDS dataset
    """
    for inpt_name, inpt_type, inpt_path in inputs:
        dataset.add_sink(inpt_name, inpt_type, path=inpt_path)
    data_node = dataset.node(frequency, id)
    with dataset.store:
        for inpt_name, inpt_value in input_values.items():
            node_item = data_node[inpt_name]
            node_item.put(inpt_value) # Store value/path in store
    return dataset


@mark.task
@mark.annotate(
    {'return':
        {'base': str,
         'path_prefix': str,
         'output': str}})
def dataset_paths(app_name: str, dataset: Dataset, id: str):
    return (dataset.id,
            'derivatives' + '/' + app_name,
            str(Path(dataset.id) / 'derivatives' / app_name / id))


def extract_bids(dataset: Dataset,
                 frequency: Clinical,
                 outputs: ty.List[ty.Tuple[str, type, str]],
                 path_prefix: str,
                 id: str,
                 app_completed: bool):
    """Selects the items from the dataset corresponding to the input 
    sources and retrieves them from the store to a cache on 
    the host

    Parameters
    ----------
    """
    output_paths = []
    data_node = dataset.node(frequency, id)
    for output_name, output_type, output_path in outputs:
        dataset.add_sink(output_name, output_type,
                         path=path_prefix + '/' + output_path)
    with dataset.store:
        for output in outputs:
            item = data_node[output[0]]
            item.get()  # download to host if required
            output_paths.append(item.value)
    return tuple(output_paths) if len(outputs) > 1 else output_paths[0]


@mark.task
@mark.annotate(
    {'return':
        {'bindings': ty.List[ty.Tuple[str, str, str]],
         'tmp_output_dir': Path}})
def make_bindings(dataset_path: str):
    """Make bindings for directories to be mounted inside the container
        for both the input dataset and the output derivatives"""
    tmp_output_dir = tempfile.mkdtemp()
    bindings = [(str(dataset_path), BidsApp.CONTAINER_DATASET_PATH, 'ro'),
                (tmp_output_dir, BidsApp.CONTAINER_DERIV_PATH, 'rw')]
    return (bindings, Path(tmp_output_dir))


@mark.task
def copytree(src: str, dest: str, app_completed: bool) -> bool:
    shutil.copytree(src, dest)
    return app_completed