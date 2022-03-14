import re
from dataclasses import dataclass
from pathlib import Path
import typing as ty
import tempfile
import logging
import click
from typing import Sequence, Dict
import arcana.data.formats
from arcana.exceptions import ArcanaUsageError
from arcana.core.data.format import FileFormat
from arcana.core.data.space import DataSpace
from arcana.core.enum import DataQuality
from arcana import __version__
from arcana.tasks.bids import construct_bids, extract_bids, bids_app
from arcana.core.cli import cli
from arcana.core.data.set import Dataset
from arcana.core.utils import (
    resolve_class, resolve_datatype, list_instances, set_loggers,
    parse_dimensions)


logger = logging.getLogger('arcana')

PYDRA_CACHE = 'pydra-cache'


@cli.group()
def derive():
    pass


sanitize_path_re = re.compile(r'[^a-zA-Z\d]')

def sanitize_path(path):
    return sanitize_path_re.sub(path, '_')


@derive.command(name='column', help="""Run a Pydra task/workflow on a dataset.

dataset
    The path to the dataset to run the workflow on.
pydra_task
    The import path to a Pydra task or workflow to run on the dataset, starting
    with the module path + ':' + task-name, e.g.
        pydra.tasks.fsl.preprocess.fast:FAST
    For convenience the 'pydra.tasks' prefix can be omitted,
    e.g. fsl.preprocess.fast:FAST)
""")
@click.argument('dataset')
@click.argument('pydra_task')
@click.option(
    '--input', '-i', 'input_specs', multiple=True, nargs=3,  metavar='<criteria>',
    help="""A file-group input to provide to the task that is matched by the 
    provided criteria.

    The INTERFACE_FIELD is the name of the field in the Pydra interface to
    connect the source to.

    REQUIRED_FORMAT is the format that the app requires the input in.
    If different from the FORMAT, an implicit conversions will
                    be attempted when required. The default is
                    'niftix_gz', which is the g-zipped NIfTI image file
                    + JSON side-car required for BIDS 

    The 3rd argument to the input contains the criteria used to match the
    input and can contain any combination of PATH, FORMAT, ORDER, QUALITY,
    METADATA and FREQUENCY separated by ':'. Criteria that are not needed can
    be omitted, e.g. to select the fist scan of the session with usable quality
    the following value,

        ::1:usable

    PATH the name regular expression (in Python syntax) of file-group or
    field name.

    FORMAT is the name or extension of the file-format the
    input is stored in in the dataset.
    
    ORDER is the order of the scan in the session to select if more than
    one match the other criteria. E.g. an order of '2' with a pattern of
    '.*bold.*' could match the second T1-weighted scan in the session
    
    QUALITY is the the minimum usuable quality to be considered for a match.
    Can be one of 'usable', 'questionable' or 'unusable'

    METADATA semicolon-separated list of header_vals values
                    in NAME=VALUE form. For DICOM headers
                    NAME is the numeric values of the DICOM tag, e.g
                    (0008,0008) -> 00080008
    FREQUENCY The frequency of the file-group within the dataset.
    Can be either 'dataset', 'group', 'subject',
    'timepoint', 'session', 'unique_subject', 'group_visit'
    or 'subject_timepoint'. Typically only required for
    derivatives

    Trailing args can be dropped if default, 

        e.g. --input in_file 't1_mprage.*'
        
    Preceding args that aren't required can be replaced by '*', 

        --input in_file.nii.gz 't1_mprage.*' * * questionable""")
@click.option(
    '--output', '-o', 'output_specs', multiple=True, nargs=3, 
    metavar='<spec>',
    help="""The outputs produced by the task to be stored.

    The INTERFACE_FIELD is the name of the output field in the Pydra
    interface to connect to the sink to.

    PRODUCED_FORMAT is the name of the file-format that the file be produced
    by the workflow in

    The STORE_AT arg specifies where the output should be stored within
    the data node of the dataset in the store.

    FORMAT is the name of
    the file-format the file will be stored at in the dataset.""")
@click.option(
    '--parameter', '-p', 'parameters', metavar='<name-val>', 
    multiple=True, nargs=2,
    help=("Parameter to pass to the app"))
@click.option(
    '--id', 'ids', multiple=True, default=None, 
    help=("IDs of the nodes to process (i.e. for the frequency that "
          "the app runs at)."))
@click.option(
    '--frequency', '-f', default='session',
    help=("The level at which the analysis is performed. One of (per) "
          "dataset, group, subject, timepoint, group_timepoint or "
          "session"))
@click.option(
    '--container', nargs=2, default=None,
    metavar='<engine-tag>',
    help=("The container engine ('docker'|'singularity') and the image"
            " to run the app in"))
@click.option(
    '--work', '-w', 'work_dir', default=None,
    help=("The location of the directory where the working files "
          "created during the pipeline execution will be stored"))
@click.option(
    '--pydra_plugin', default='cf',
    help=("The Pydra plugin with which to process the workflow"))
@click.option(
    '--virtualisation', default='none',
    type=click.Choice(['docker', 'singularity', 'none'], case_sensitive=False),
    help=("The virtualisation method to run with the task with (only "
          "applicable to BIDS app tasks)"))
@click.option(
    '--dry_run', is_flag=True, default=False,
    help=("Set up the workflow to test inputs but don't run the app"))
@click.option(
    '--loglevel', type=str, default='info',
    help=("The level of detail logging information is presented"))
def derive_column(dataset_path, pydra_task, input_specs, output_specs, parameters, ids, frequency,
        container, work_dir, pydra_plugin, virtualisation, dry_run, loglevel,
        ignore_blank_inputs):
    raise NotImplementedError

    set_loggers(loglevel)

    work_dir = Path(work_dir) if work_dir is not None else Path(tempfile.mkdtemp())
    work_dir.mkdir(exist_ok=True)

    dataset = Dataset.load(dataset_path)
    frequency = dataset.dimensions[frequency]
    inputs = add_input_sources(dataset, input_specs, frequency)
    outputs = add_output_sinks(dataset, output_specs, frequency)
    

    pipeline = dataset.new_pipeline(
        name=workflow_name(pydra_task),
        inputs=inputs,
        outputs=outputs,
        frequency=frequency,
        cache_dir=work_dir / PYDRA_CACHE)

    construct_pipeline(pipeline)

    if not dry_run:
        pipeline(ids=ids, plugin=pydra_plugin)

    logger.info(f'"{pydra_task}" app completed successfully')


def _datatype_from_path(path, default, datatype_name=None):
    datatype = None
    if datatype_name is not None:
        datatype = resolve_datatype(datatype_name.lower())
    elif ':' in path:
        path, datatype_name = str(path).split(':')
        datatype = resolve_datatype(datatype_name.lower())
    elif '.' in path:
        path = Path(path)
        path_ext = '.'.join(path.suffixes)
        # Strip suffix from path
        path = path.parent / path.stem
        # FIXME: Need a more robust way of determining datatype
        # from output path extension
        for dtype in list_instances(arcana.data.formats, FileFormat):
            if dtype.extension == path_ext:
                datatype = dtype
                break
    if datatype is None:
        datatype = default
    return path, datatype


def add_input_sources(dataset, inputs, default_frequency):
    """Parses input arguments into dictionary of DataSources

    Parameters
    ----------
    args : ArgumentParser.namespace
        The parsed arguments from a ArgumentParser.parse_args() method

    Returns
    -------
    ty.List[ty.Tuple[str, FileFormat]]
        A sequence of input names and their required formats

    Raises
    ------
    ArcanaUsageError
        If too many arguments are provided to the `--input` arg (max 7)
    ArcanaUsageError
        If the VAR_ARG is not provided to the input
    ArcanaUsageError
        If a path pattern is not provided
    ArcanaUsageError
        If a file_format is not provided
    """
    # Create file-group matchers
    parsed_inputs = []
    for name, input_datatype_str, criteria in inputs:
        parts = criteria.split(':')
        (pattern, stored_datatype_str,
         order, quality_threshold, metadata, frequency) = parts + [None] * (6 - len(parts))
        input_datatype = resolve_datatype(input_datatype_str)
        pattern, stored_datatype = _datatype_from_path(pattern, input_datatype,
                                                       stored_datatype_str)
        if frequency is None:
            frequency = default_frequency
        dataset.add_source(
            name=name,
            path=pattern,
            datatype=stored_datatype,
            frequency=frequency,
            order=order,
            metadata=metadata,
            is_regex=True,
            quality_threshold=quality_threshold)
        parsed_inputs.append((name, input_datatype))
    return parsed_inputs


def add_output_sinks(dataset, outputs, frequency):
    """Parses output arguments into dictionary of DataSinks

    Parameters
    ----------
    args : ArgumentParser.namespace
        The parsed arguments from a ArgumentParser.parse_args() method

    Returns
    -------
    ty.List[ty.Tuple[str, FileFormat]]
        A sequence of input names and the formats they are produced in
    """
    # Create outputs
    parsed_outputs = []
    for name, output_datatype_str, storage_spec in outputs:
        output_datatype = resolve_datatype(output_datatype_str)
        path, stored_datatype = _datatype_from_path(storage_spec, output_datatype)
        dataset.add_sink(
            name=name,
            path=path,
            datatype=stored_datatype,
            frequency=frequency)
        parsed_outputs.append((name, output_datatype))
    return parsed_outputs


def construct_pipeline(pipeline, pydra_task, virtualisation):
    
    task_cls = resolve_class(pydra_task, prefixes=['pydra.tasks'])

    kwargs = parse_parameters(task_cls)
    if virtualisation != 'none':
        kwargs['virtualisation'] = virtualisation

    # Add the app task
    pipeline.add(task_cls(name='app', **kwargs))

    # Connect source to inputs
    for input in pipeline.input_names:
        setattr(pipeline.app.inputs, input, getattr(pipeline.lzin, input))

    # Connect outputs to sink
    for output in pipeline.output_names:
        pipeline.set_output((output, getattr(pipeline.app.lzout, output)))

    return pipeline


def parse_parameters(parameters, task_cls):
    """Parses the args to be passed to the Pydra task, converting to the
    right types where required.

    Parameters
    ----------
    args : [type]
        The cmd args (as parsed by ArgumentParser)
    task_cls : [type]
        The Pydra task class

    Returns
    -------
    Dict[str, Any]
        Arg names and their values to pass to the app
    """
    app_args = {}
    task = task_cls()
    for name, val in parameters:
        try:
            arg_spec = next(s for s in task.input_spec.fields
                            if s[0] == name)
        except StopIteration:
            raise ArcanaUsageError(
                f"Unrecognised argument '{name}' passed to '--parameter' flag. "
                "Expecting one of '{}'".format(
                    "', '".join(f[0] for f in task.input_spec.fields
                                if not f[0].startswith('_'))))
        arg_type = arg_spec[1].type
        if arg_type is not str and issubclass(arg_type, Sequence):
            if len(arg_type.__args__) == 1:
                sub_type = arg_type.__args__[0]
            else:
                sub_type = str
            val = [sub_type(v) for v in re.split(r'[ ,;]+', val)]
        else:
            try:
                val = arg_type(val)
            except TypeError as e:
                raise ArcanaUsageError(
                    f"Value supplied to '{name}' field in {task.name} "
                    f"cannot be converted to type {arg_spec[1]}") from e
        app_args[name] = val
    return app_args


def workflow_name(pydra_task):
    return re.sub(r'[\.:]', '_', pydra_task)


# @dataclass
# class InputArg():
#     name: str
#     path: Path
#     input_datatype: FileFormat
#     stored_datatype: FileFormat
#     frequency: DataSpace
#     order: int
#     metadata: ty.Dict[str, str]
#     is_regex: bool
#     quality_threshold: DataQuality


# @dataclass
# class OutputArg():
#     name: str
#     path: Path
#     output_datatype: FileFormat
#     stored_datatype: FileFormat
    


@derive.command(name='output', help="""Derive an output""")
def derive_output():
    raise NotImplementedError



@derive.command(help="""Derive an output""")
def menu():
    raise NotImplementedError


@derive.command(
    name='ignore-diff',
    help="""Ignore difference between provenance of previously generated derivative
and new parameterisation""")
def ignore_diff():
    raise NotImplementedError