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
from arcana.data.stores.bids.dataset import BidsDataset
from arcana.exceptions import ArcanaUsageError
from arcana.core.utils import func_task, path2name, name2path


def bids_app(name: str,
             inputs: ty.List[ty.Tuple[str, type]],
             outputs: ty.List[ty.Tuple[str, type]],
             executable: str='',
             container_image: str= None,
             parameters: ty.Dict[str, type]=None,
             frequency: Clinical or str=Clinical.session,
             container_type: str='docker',
             dataset: ty.Optional[ty.Union[str, Path, Dataset]]=None) -> Workflow:
    """Creates a Pydra workflow which takes file inputs, maps them to
    a BIDS dataset, executes a BIDS app, and then extracts the
    the derivatives that were stored back in the BIDS dataset by the app

    Parameters
    ----------
    name : str
        Name of the workflow/BIDS app. Will be used to name the 'derivatives'
        sub-directory where the app outputs are stored
    inputs : list[tuple[str, type]]
        The inputs to be inserted into the BIDS dataset. Should be a list of tuples
        consisting of the the path the file/directory should be stored within a BIDS subject/session,
        e.g. anat/T1w, func/bold, and the DataFormat class it should be stored in, e.g.
        arcana.data.formats.bids.NiftiGzX.
    outputs : list[tuple[str, type]]
        The outputs to be extracted from the derivatives directory. Should be a list of tuples
        consisting of the the path the file/directory is saved by the app within a BIDS subject/session,
        e.g. freesurfer/recon-all, and the DataFormat class it is stored in, e.g.
        arcana.data.formats.common.Directory.
    executable : str, optional
        Name of the executable within the image to run (i.e. the entrypoint of the image).
        Required when extending the base image and launching Arcana within it
    container_image : str, optional
        Name of the BIDS app image to wrap
    parameters : str, optional
        a list of parameters of the app (i.e. CLI flags) to be exposed to the user
    frequency : Clinical, optional
        Frequency to run the app at, i.e. per-"session" or per-"dataset"
    container_type : str, optional
        The virtualisation method to run the main app task, can be one of
        'docker' or 'singularity'
    dataset : str or Dataset, optional
        The dataset to run the BIDS app on. If a string or Path is provided
        then a new BIDS dataset is created at that location with a single
        subject (sub-DEFAULT). If nothing is provided then a dataset is
        created at './bids_dataset'.

    Returns
    -------
    pydra.Workflow
        A Pydra workflow 
    """
    if parameters is None:
        parameters = {}
    if isinstance(frequency, str):
        frequency = Clinical[frequency]

    # Create BIDS dataset to hold translated data
    if dataset is None:
        dataset = Path(tempfile.mkdtemp()) / 'bids_dataset'
    if not isinstance(dataset, Dataset):
        dataset = BidsDataset.create(
            path=dataset,
            name=name + '_dataset',
            subject_ids=[DEFAULT_BIDS_ID])

    # Ensure output paths all start with 'derivatives
    input_names = [path2name(i[0]) for i in inputs]
    output_names = [path2name(o[0]) for o in outputs]
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
        inputs=inputs,
        dataset=dataset,
        id=workflow.bidsify_id.lzout.out,
        **{i: getattr(workflow.lzin, i) for i in input_names}))

    workflow.add(dataset_paths(
        app_name=name,
        dataset=workflow.to_bids.lzout.dataset,
        id=workflow.bidsify_id.lzout.out))
        
    app_completed = add_main_task(
        workflow=workflow,
        executable=executable,
        dataset_path=workflow.dataset_paths.lzout.base,
        output_path=workflow.dataset_paths.lzout.output,
        parameters={p: type(p) for p in parameters},
        frequency=frequency,
        id=workflow.bidsify_id.lzout.no_prefix,
        container_type=container_type,
        container_image=container_image)

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
        outputs=outputs,
        id=workflow.bidsify_id.lzout.out,
        app_completed=app_completed))

    for output_name in output_names:
        workflow.set_output(
            (output_name, getattr(workflow.extract_bids.lzout, output_name)))

    return workflow


def add_main_task(workflow: Workflow,
                  executable: str,
                  dataset_path: LazyField,
                  output_path: LazyField,
                  frequency: Clinical,
                  id: str,
                  parameters: ty.Dict[str, type]=None,
                  container_image=None,
                  container_type='docker') -> ShellCommandTask:

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

    # If 'image' is None, don't use any virtualisation (i.e. assume we are running from "inside" the
    # container or extension of it)
    if container_image is None:
        task_cls = ShellCommandTask
        base_spec_cls = ShellSpec
        kwargs['executable'] = executable
        app_output_path = output_path
    else:

        workflow.add(make_bindings(
            name='make_bindings',
            dataset_path=dataset_path))

        kwargs['bindings'] = workflow.make_bindings.lzout.bindings

        # Set input and output directories to "internal" paths within the
        # container
        dataset_path = CONTAINER_DATASET_PATH
        app_output_path = CONTAINER_DERIV_PATH
        kwargs['image'] = container_image

        if container_type == 'docker':
            task_cls = DockerTask
            base_spec_cls = DockerSpec
        elif container_type == 'singularity':
            task_cls = SingularityTask
            base_spec_cls = SingularitySpec
        else:
            raise ArcanaUsageError(
                f"Unrecognised container type {container_type} "
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

    if container_image is not None:
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

DEFAULT_BIDS_ID = 'sub-DEFAULT'


@mark.task
@mark.annotate(
    {'return':
        {'out': str,
         'no_prefix': str}})
def bidsify_id(id):
    if id == attr.NOTHING:
        id = DEFAULT_BIDS_ID
    else:
        id = re.sub(r'[^a-zA-Z0-9]', '', id)
        if not id.startswith('sub-'):
            id = 'sub-' + id
    return id, id[len('sub-'):]


def to_bids(frequency, inputs, dataset, id, **input_values):
    """Takes generic inptus and stores them within a BIDS dataset
    """
    for inpt_path, inpt_type in inputs:
        dataset.add_sink(path2name(inpt_path), inpt_type, path=inpt_path)
    data_node = dataset.node(frequency, id)
    with dataset.store:
        for inpt_name, inpt_value in input_values.items():
            if inpt_value is attr.NOTHING:
                raise ArcanaUsageError(
                    f"No value passed to {inpt_name}")
            node_item = data_node[inpt_name]
            node_item.put(inpt_value)  # Store value/path in store
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
                 outputs: ty.List[ty.Tuple[str, type]],
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
    for output_path, output_type in outputs:
        dataset.add_sink(path2name(output_path), output_type,
                         path=path_prefix + '/' + output_path)
    with dataset.store:
        for output in outputs:
            item = data_node[path2name(output[0])]
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
    bindings = [(str(dataset_path), CONTAINER_DATASET_PATH, 'ro'),
                (tmp_output_dir, CONTAINER_DERIV_PATH, 'rw')]
    return (bindings, Path(tmp_output_dir))


@mark.task
def copytree(src: str, dest: str, app_completed: bool) -> bool:
    shutil.copytree(src, dest)
    return app_completed
