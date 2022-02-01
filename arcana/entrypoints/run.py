import re
from dataclasses import dataclass
from pathlib import Path
import typing as ty
import tempfile
import logging
from typing import Sequence, Dict
import arcana.data.types
from arcana.exceptions import ArcanaUsageError
from arcana.core.data.type import FileFormat
from arcana.core.data.dimensions import DataDimensions
from arcana.core.data.enum import DataQuality
from arcana import __version__
from arcana.tasks.bids import construct_bids, extract_bids, bids_app
from .dataset import BaseDatasetCmd
from arcana.core.utils import (
    resolve_class, resolve_datatype, list_instances, set_loggers)

logger = logging.getLogger('arcana')

PYDRA_CACHE = 'pydra-cache'


sanitize_path_re = re.compile(r'[^a-zA-Z\d]')

def sanitize_path(path):
    return sanitize_path_re.sub(path, '_')


class RunCmd(BaseDatasetCmd):
    """Runs a generic Pydra task/workflow 
    """

    cmd_name = 'run'
    desc = ("Runs an app against a dataset stored in a store. The app "
            "needs to be wrapped in a Pydra interface that is on the Python "
            "path")

    MAX_INPUT_ARGS = 7

    @classmethod
    def construct_parser(cls, parser):
        parser.add_argument(
            'app',
            help=("The path to a Pydra interface that wraps the app "
                  "convenience the 'pydra.tasks' prefix can be omitted "
                  "(e.g. fsl.preprocess.first.First)"))        
        parser.add_argument(
            '--frequency', '-f', default='session',
            help=("The level at which the analysis is performed. One of (per) "
                  "dataset, group, subject, timepoint, group_timepoint or "
                  "session"))
        parser.add_argument(
            '--parameter', '-p', metavar=('NAME', 'VAL'),
            action='append', default=[], nargs=2,
            help=("Parameter to pass to the app"))
        super().construct_parser(parser)
        parser.add_argument(
            '--input', '-i', action='append', default=[], nargs='+',
            help=cls.INPUT_HELP)
        parser.add_argument(
            '--output', '-o', action='append', default=[], nargs=3,
            metavar=('VAR', 'PRODUCED_DTYPE', 'STORE_AT'),
            help=cls.OUTPUT_HELP)
        parser.add_argument(
            '--ids', nargs='+', default=None,
            help=("IDs of the nodes to process (i.e. for the frequency that "
                  "the app runs at)."))
        parser.add_argument(
            '--container', nargs=2, default=None,
            metavar=('ENGINE', 'IMAGE'),
            help=("The container engine ('docker'|'singularity') and the image"
                  " to run the app in"))
        parser.add_argument(
            '--work', '-w', default=None,
            help=("The location of the directory where the working files "
                  "created during the pipeline execution will be stored"))
        parser.add_argument(
            '--pydra_plugin', default='cf',
            help=("The Pydra plugin with which to process the workflow"))
        parser.add_argument(
            '--virtualisation', default='none', type=str,
            choices=('docker', 'singularity', 'none'),
            help=("The virtualisation method to run with the task with (only applicable to BIDS app tasks)"))
        parser.add_argument(
            '--dry_run', action='store_true', default=False,
            help=("Set up the workflow to test inputs but don't run the app"))
        parser.add_argument(
            '--loglevel', type=str, default='info',
            help=("The level of detail logging information is presented"))
        parser.add_argument(
            '--ignore_blank_inputs', action='store_true', default=False,
            help=("Inputs with only 2 args (INTERFACE_FIELD and "
                  "REQUIRED_FORMAT), i.e. empty criteria, are allowed but "
                  "ignored. This enables inputs generated programmatically "
                  "(e.g. XNAT CS dialog) to be ignored if they are provided "
                  "an empty string. Otherwise, such inputs will raise an "
                  "error as they are likely due a manual mistake."))

    @classmethod
    def run(cls, args):

        set_loggers(args.loglevel)

        work_dir = Path(args.work) if args.work is not None else Path(tempfile.mkdtemp())
        work_dir.mkdir(exist_ok=True)

        dataset = cls.get_dataset(args, work_dir)
        inputs = cls.add_input_sources(args, dataset)
        outputs = cls.add_output_sinks(args, dataset)
        frequency = cls.parse_frequency(args)

        pipeline = dataset.new_pipeline(
            name=cls.workflow_name(args),
            inputs=inputs,
            outputs=outputs,
            frequency=frequency,
            cache_dir=work_dir / PYDRA_CACHE)

        cls.construct_pipeline(args, pipeline)

        if not args.dry_run:
            pipeline(ids=args.ids, plugin=args.pydra_plugin)

        logger.info(f'"{args.app}" app completed successfully')

    @classmethod
    def parse_input_args(cls, args):
        for i, inpt in enumerate(args.input):
            nargs = len(inpt)
            if nargs > cls.MAX_INPUT_ARGS:
                raise ArcanaUsageError(
                    f"Input {i} has too many input args, {nargs} instead "
                    f"of max {cls.MAX_INPUT_ARGS} ({inpt})")
            (var, input_datatype_name, pattern, order,
             quality, metadata, freq) = [
                a if a != '*' else None for a in (
                    inpt + [None] * (cls.MAX_INPUT_ARGS - len(inpt)))]
            if not var:
                raise ArcanaUsageError(
                    f"A Pydra task field must be provided for input {i} ({inpt})")
            if not input_datatype_name:
                raise ArcanaUsageError(
                    f"An input datatype to match must be provided for input {i} ({inpt})")
            if not pattern:
                if args.ignore_blank_inputs:
                    logger.warning("Ignoring '%s' input as pattern to match was not provided", var)
                    continue  # Empty inputs are ignored to help facilitate GUIs (e.g. XNAT CS)
                raise ArcanaUsageError(
                    f"A path pattern to match must be provided for input {i} ({inpt})")
            input_datatype = resolve_datatype(input_datatype_name)
            pattern, stored_datatype = cls._datatype_from_path(pattern,
                                                               input_datatype)
                
            yield InputArg(var, pattern, input_datatype, stored_datatype, freq,
                           order, metadata, True, quality)

    @classmethod
    def parse_output_args(cls, args):
        for output in args.output:
            var, output_datatype, store_at = output
            # When interface outputs include file format this argument won't
            # be necessary
            output_datatype = resolve_datatype(output_datatype)
            store_at, stored_datatype = cls._datatype_from_path(store_at,
                                                                output_datatype)
            yield OutputArg(name=var, path=store_at,
                            output_datatype=output_datatype,
                            stored_datatype=stored_datatype)

    @classmethod
    def _datatype_from_path(cls, path, default):
        datatype = None
        if ':' in path:
            path, datatype_name = str(path).split(':')
            datatype = resolve_datatype(datatype_name.lower())
        elif '.' in path:
            path = Path(path)
            path_ext = '.'.join(path.suffixes)
            # Strip suffix from path
            path = path.parent / path.stem
            # FIXME: Need a more robust way of determining datatype
            # from output path extension
            for dtype in list_instances(arcana.data.types, FileFormat):
                if dtype.extension == path_ext:
                    datatype = dtype
                    break
        if datatype is None:
            datatype = default
        return path, datatype

    @classmethod
    def add_input_sources(cls, args, dataset):
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
        inputs = []
        for arg in cls.parse_input_args(args):
            dataset.add_source(
                name=arg.name,
                path=arg.path,
                datatype=arg.stored_datatype,
                frequency=arg.frequency,
                order=arg.order,
                metadata=arg.metadata,
                is_regex=arg.is_regex,
                quality_threshold=arg.quality_threshold)
            inputs.append((arg.name, arg.input_datatype))
        return inputs

    @classmethod
    def add_output_sinks(cls, args, dataset):
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
        frequency = cls.parse_frequency(args)
        # Create outputs
        outputs = []
        for arg in cls.parse_output_args(args):
            dataset.add_sink(
                name=arg.name,
                path=arg.path,
                datatype=arg.stored_datatype,
                frequency=frequency)
            outputs.append((arg.name, arg.output_datatype))
        return outputs

    @classmethod
    def construct_pipeline(cls, args, pipeline):
        
        task_cls = resolve_class(args.app, prefixes=['pydra.tasks'])

        kwargs = cls.parse_parameters(args, task_cls)
        if args.virtualisation != 'none':
            kwargs['virtualisation'] = args.virtualisation

        # Add the app task
        pipeline.add(task_cls(name='app', **kwargs))

        # Connect source to inputs
        for input in pipeline.input_names:
            setattr(pipeline.app.inputs, input, getattr(pipeline.lzin, input))

        # Connect outputs to sink
        for output in pipeline.output_names:
            pipeline.set_output((output, getattr(pipeline.app.lzout, output)))

        return pipeline

    @classmethod
    def parse_parameters(cls, args, task_cls):
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
        for name, val in args.parameter:
            try:
                arg_spec = next(s for s in task.input_spec.fields
                                if s[0] == name)
            except StopIteration:
                raise ArcanaUsageError(
                    f"Unrecognised argument '{name}' passed to '--arg' flag. "
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
                except TypeError:
                    raise ArcanaUsageError(
                        f"Value supplied to '{name}' field in {task.name} "
                        f"cannot be converted to type {arg_spec[1]}") 
            app_args[name] = val
        return app_args

    VAR_ARG = ''

    VAR_DESC = f"""
        
    """

    @classmethod
    def parse_frequency(cls, args):
        return cls.parse_dataspace(args)[args.frequency]

    @classmethod
    def workflow_name(cls, args):
        return re.sub(r'[\.:]', '_', args.app)


    INPUT_HELP = """
        A file-group input to provide to the app that is matched by the 
        provided criteria.

        The INTERFACE_FIELD is the name of the field in the Pydra interface to
        connect the source to.

        REQUIRED_FORMAT is the format that the app requires the input in.
        If different from the FORMAT, an implicit conversions will
                        be attempted when required. The default is
                        'niftix_gz', which is the g-zipped NIfTI image file
                        + JSON side-car required for BIDS 

        PATH:FORMAT the name regular expression (in Python syntax) of file-group or
        field name, where FORMAT is the name or extension of the file-format the
        input is stored in in the dataset.

        Alternative criteria that can be used to match the file-group (e.g. scan)
        
        ORDER is the order of the scan in the session to select if more than
        one match the other criteria. E.g. an order of '2' with a pattern of
        '.*bold.*' could match the second T1-weighted scan in the session
        
        QUALITY is the the minimum usuable quality to be considered for a match.
        Can be one of 'usable', 'questionable' or 'unusable'

        semicolon-separated list of header_vals values
                        in NAME:VALUE form. For DICOM headers
                        NAME is the numeric values of the DICOM tag, e.g
                        (0008,0008) -> 00080008
            frequency - The frequency of the file-group within the dataset.
                        Can be either 'dataset', 'group', 'subject',
                        'timepoint', 'session', 'unique_subject', 'group_visit'
                        or 'subject_timepoint'. Typically only required for
                        derivatives

        Trailing args can be dropped if default, 

            e.g. --input in_file 't1_mprage.*'
            
        Preceding args that aren't required can be replaced by '*', 

            --input in_file.nii.gz 't1_mprage.*' * * questionable"""


    OUTPUT_HELP = """The outputs produced by the app to be stored in the "
        store.

        The INTERFACE_FIELD is the name of the output field in the Pydra
        interface to connect to the sink to.

        PRODUCED_FORMAT is the name of the file-format that the file be produced
        by the workflow in

        The STORE_AT:FORMAT arg specifies where the output should be stored within
        the data node of the dataset in the store. FORMAT is the name of
        the file-format the file will be stored at in the dataset.
        """


@dataclass
class InputArg():
    name: str
    path: Path
    input_datatype: FileFormat
    stored_datatype: FileFormat
    frequency: DataDimensions
    order: int
    metadata: ty.Dict[str, str]
    is_regex: bool
    quality_threshold: DataQuality


@dataclass
class OutputArg():
    name: str
    path: Path
    output_datatype: FileFormat
    stored_datatype: FileFormat
    
