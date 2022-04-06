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
from arcana.core.data.space import DataSpace
from arcana.core.enum import DataQuality
from arcana import __version__
# from arcana.tasks.bids import construct_bids, extract_bids, bids_app
from arcana.core.cli import cli
from arcana.core.data.set import Dataset
from arcana.core.utils import (
    resolve_class, list_instances, set_loggers, parse_dimensions)


logger = logging.getLogger('arcana')

PYDRA_CACHE = 'pydra-cache'


@cli.group()
def derive():
    pass


sanitize_path_re = re.compile(r'[^a-zA-Z\d]')

def sanitize_path(path):
    return sanitize_path_re.sub(path, '_')


@derive.command(name='column', help="""Derive 

DATASET_ID_STR string containing the nick-name of the store, the ID of the dataset
(e.g. XNAT project ID or file-system directory) and the dataset's name in the
format <NICKNAME>//DATASET_ID:DATASET_NAME

NAME of the pipeline

WORKFLOW_LOCATION is the location to a Pydra workflow on the Python system path,
<MODULE>:<WORKFLOW>""")
@click.argument('dataset_id_str')
@click.argument('columns', nargs=-1)
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
    
    dataset = Dataset.load(id_str)

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


# def _format_from_path(path, default, format_name=None):
#     format = None
#     if format_name is not None:
#         format = resolve_format(format_name.lower())
#     elif ':' in path:
#         path, format_name = str(path).split(':')
#         format = resolve_format(format_name.lower())
#     elif '.' in path:
#         path = Path(path)
#         path_ext = '.'.join(path.suffixes)
#         # Strip suffix from path
#         path = path.parent / path.stem
#         # FIXME: Need a more robust way of determining format
#         # from output path extension
#         for dtype in list_instances(arcana.data.formats, FileFormat):
#             if dtype.extension == path_ext:
#                 format = dtype
#                 break
#     if format is None:
#         format = default
#     return path, format


def add_input_sources(dataset, inputs, default_frequency):
    """Parses input arguments into dictionary of DataSources

    Parameters
    ----------
    args : ArgumentParser.namespace
        The parsed arguments from a ArgumentParser.parse_args() method

    Returns
    -------
    ty.List[ty.Tuple[str, type]]
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
    for name, input_format_str, criteria in inputs:
        parts = criteria.split(':')
        (pattern, stored_format_str,
         order, quality_threshold, metadata, frequency) = parts + [None] * (6 - len(parts))
        input_format = resolve_format(input_format_str)
        pattern, stored_format = _format_from_path(pattern, input_format,
                                                       stored_format_str)
        if frequency is None:
            frequency = default_frequency
        dataset.add_source(
            name=name,
            path=pattern,
            format=stored_format,
            frequency=frequency,
            order=order,
            metadata=metadata,
            is_regex=True,
            quality_threshold=quality_threshold)
        parsed_inputs.append((name, input_format))
    return parsed_inputs


def add_output_sinks(dataset, outputs, frequency):
    """Parses output arguments into dictionary of DataSinks

    Parameters
    ----------
    args : ArgumentParser.namespace
        The parsed arguments from a ArgumentParser.parse_args() method

    Returns
    -------
    ty.List[ty.Tuple[str, type]]
        A sequence of input names and the formats they are produced in
    """
    # Create outputs
    parsed_outputs = []
    for name, output_format_str, storage_spec in outputs:
        output_format = resolve_format(output_format_str)
        path, stored_format = _format_from_path(storage_spec, output_format)
        dataset.add_sink(
            name=name,
            path=path,
            format=stored_format,
            frequency=frequency)
        parsed_outputs.append((name, output_format))
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
#     input_format: FileFormat
#     stored_format: FileFormat
#     frequency: DataSpace
#     order: int
#     metadata: ty.Dict[str, str]
#     is_regex: bool
#     quality_threshold: DataQuality


# @dataclass
# class OutputArg():
#     name: str
#     path: Path
#     output_format: FileFormat
#     stored_format: FileFormat
    


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