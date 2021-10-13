from itertools import zip_longest, repeat
import re
from typing import Sequence
from pydra import Workflow
from arcana2.core.data.spec import DataSource
from arcana2.core.data.spec import DataSink
from arcana2.exceptions import ArcanaUsageError
from arcana2.__about__ import __version__
from arcana2.tasks.bids import construct_bids, extract_bids, bids_app
from .base import BaseDatasetCmd
from arcana2.core.utils import resolve_class, resolve_datatype


sanitize_path_re = re.compile(r'[^a-zA-Z\d]')

def sanitize_path(path):
    return sanitize_path_re.sub(path, '_')

class BaseRunCmd(BaseDatasetCmd):
    """Abstract base class for RunCmds
    """

    MAX_INPUT_ARGS = 8

    @classmethod
    def construct_parser(cls, parser):
        super().construct_parser(parser)
        parser.add_argument(
            '--container', nargs=2, default=None,
            metavar=('ENGINE', 'IMAGE'),
            help=("The container engine ('docker'|'singularity') and the image"
                  " to run the app in"))
        parser.add_argument(
            '--input', '-i', action='append', default=[], nargs='+',
            help=cls.INPUT_HELP.format(var_desc=cls.VAR_DESC))
        parser.add_argument(
            '--output', '-o', action='append', default=[], nargs='+',
            help=cls.OUTPUT_HELP.format(var_desc=cls.VAR_DESC))
        parser.add_argument(
            '--workflow_format', action='append', default=[], nargs=2,
            metavar=('NAME', 'FORMAT'),
            help=("The file format the app requires the input in/provides "
                  "outputs in. Only required when the format differs from the "
                  "format stored in the dataset"))
        parser.add_argument(
            '--ids', nargs='+', default=None,
            help=("IDs of the nodes to process (i.e. for the frequency that "
                  "the app runs at)."))
        parser.add_argument(
            '--dry_run', action='store_true', default=False,
            help=("Set up the workflow to test inputs but don't run the app"))

    @classmethod
    def run(cls, args):

        dataset = cls.get_dataset(args)
        inputs = cls.add_input_sources(args, dataset)
        outputs = cls.add_output_sinks(args, dataset)
        frequency = cls.parse_frequency(args)

        pipeline = dataset.new_pipeline(
            name=cls.workflow_name(args),
            inputs=inputs,
            outputs=outputs,
            frequency=frequency)

        cls.construct_pipeline(args, pipeline)

        if not args.dry_run:
            pipeline(ids=args.ids)

        return pipeline

    @classmethod
    def add_input_sources(cls, args, dataset):
        """Parses input arguments into dictionary of DataSources

        Parameters
        ----------
        args : ArgumentParser.namespace
            The parsed arguments from a ArgumentParser.parse_args() method

        Returns
        -------
        list[tuple[str, FileFormat]]
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
        for i, inpt in enumerate(args.input):
            nargs = len(inpt)
            if nargs > cls.MAX_INPUT_ARGS:
                raise ArcanaUsageError(
                    f"Input {i} has too many input args, {nargs} instead "
                    f"of max {cls.MAX_INPUT_ARGS} ({inpt})")
            (var, pattern, datatype_name, required_format_name, order,
             quality, metadata, freq) = [
                a if a != '*' else None for a in (
                    inpt + [None] * (cls.MAX_INPUT_ARGS - len(inpt)))]
            if not var:
                raise ArcanaUsageError(
                    f"{cls.VAR_ARG} must be provided for input {i} ({inpt})")
            if not pattern:
                raise ArcanaUsageError(
                    f"Path must be provided for input {i} ({inpt})")
            if not datatype_name:
                raise ArcanaUsageError(
                    f"Datatype must be provided for input {i} ({inpt})")
            datatype = resolve_datatype(datatype_name)
            if required_format_name is not None:
                required_format = resolve_datatype(required_format_name)
            else:
                required_format = datatype
            dataset.add_source(
                name=var,
                path=pattern,
                format=datatype,
                frequency=freq,
                order=order,
                metadata=metadata,
                is_regex=True,
                quality_threshold=quality)
            inputs.append((var, required_format))
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
        list[tuple[str, FileFormat]]
            A sequence of input names and the formats they are produced in
        """
        frequency = cls.parse_frequency(args)
        # Create outputs
        outputs = []
        for output in args.output:
            var, store_at, datatype_name = output[:3]
            datatype = resolve_datatype(datatype_name)
            produced_format = (resolve_datatype(output[3])
                               if len(output) == 4 else datatype)
            dataset.add_sink(
                name=var,
                path=store_at,
                format=datatype,
                frequency=frequency)
            outputs.append((var, produced_format))
        return outputs


    INPUT_HELP = """
        A file-group input to provide to the app that is matched by the 
        provided criteria.
        {var_desc}

        PATH the name regular expression (in Python syntax) of file-group or
        field name

        FORMAT is the name or extension of the file-format the
        input is stored in in the dataset. 

        REQUIRED_FORMAT is the format that the app requires the input in.
        If different from the FORMAT, an implicit conversions will
                        be attempted when required. The default is
                        'niftix_gz', which is the g-zipped NIfTI image file
                        + JSON side-car required for BIDS 

        Alternative criteria can be used to match the file-group (e.g. scan)
        
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
        repository.
        {var_desc}

        The STORE_AT arg specifies where the output should be stored within
        the data node of the dataset in the repository.

        FORMAT is the name of the file-format the file will be stored at in
        the dataset.

        PRODUCED_FORMAT is the name of the file-format that the file be produced
        by the workflow in
        """


class RunAppCmd(BaseRunCmd):

    desc = ("Runs an app against a dataset stored in a repository. The app "
            "needs to be wrapped in a Pydra interface that is on the Python "
            "path")

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
            '--app_arg', '-a', metavar=('FLAG',),
            action='append', default=[],
            help=("Flag to pass to the app interface"))
        super().construct_parser(parser)

    @classmethod
    def construct_pipeline(cls, args, pipeline):
        
        task_cls = resolve_class(args.app, prefixes=['pydra.tasks'])

        # Add the app task
        pipeline.add(task_cls(name='app',
                              **cls.parse_app_args(args, task_cls)))

        # Connect source to inputs
        for input in pipeline.input_names:
            setattr(pipeline.app.inputs, input, getattr(pipeline.source.lzout,
                                                        input))

        # Connect outputs to sink
        for output in pipeline.output_names:
            pipeline.set_output((output, getattr(pipeline.app.lzout, output)))

        return pipeline

    @classmethod
    def parse_app_args(cls, args, task_cls):
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
        for name, val in args.app_arg:
            try:
                arg_spec = next(s for s in task_cls.input_spec.fields
                                if s[0] == name)
            except StopIteration:
                raise ArcanaUsageError(
                    f"Unrecognised argument '{name}' passed to '--arg' flag. "
                    "Expecting one of '{}'".format(
                        "', '".join(task_cls.input_spec.fields)))
            arg_type = arg_spec[1]
            if arg_type is not str and issubclass(arg_type, Sequence):
                if len(arg_type.__args__) == 1:
                    sub_type = arg_type.__args__[0]
                else:
                    sub_type = str
                val = [sub_type(v) for v in re.split(r'[ ,;]+', val)]
            else:
                try:
                    val = arg_spec[1](val)
                except TypeError:
                    raise ArcanaUsageError(
                        f"Value supplied to '{name}' field in {task_cls} "
                        f"cannot be converted to type {arg_spec[1]}") 
            app_args[name] = val
        return app_args

    @classmethod
    def app_name(cls, args):
        return args.app.split('.')[-1].lower()

    VAR_ARG = 'INTERFACE_NAME'

    VAR_DESC = f"""
        The {VAR_ARG} is the attribute in the Pydra interface to connect
        the input to.
    """

    @classmethod
    def parse_frequency(cls, args):
        return cls.parse_dimensions(args)[args.frequency]

    @classmethod
    def workflow_name(cls, args):
        return args.app.replace('.', '_')


class RunBidsAppCmd(BaseRunCmd):

    desc = ("Runs a BIDS app against a dataset stored in a repository.")

    @classmethod
    def construct_parser(cls, parser):
        parser.add_argument(
            'entrypoint',
            help=("The entrypoint of the BIDS app"))
        parser.add_argument(
            '--analysis_level', default='participant',
            help=("The level at which the analysis is performed. Either "
                  "'participant' or 'group'"))
        parser.add_argument(
            '--flags', '-f', default='',
            help=("Arbitrary flags to pass onto the BIDS app (enclose in "
                  "quotation marks)"))
        super().construct_parser(parser)

    @classmethod
    def construct_pipeline(cls, args, pipeline):

        pipeline.add(
            construct_bids(
                name='construct_bids',
                input_names=pipeline.input_names))

        pipeline.add(
            bids_app(
                name='app',
                app_name=args.app,
                bids_dir=pipeline.construct_bids.lzout.bids_dir,
                analysis_level=args.analysis_level,
                ids=args.ids,
                flags=args.flags))

        pipeline.add(
            extract_bids(
                name='extract_bids',
                bids_dir=pipeline.app.lzout.bids_dir,
                outputs=pipeline.output_names))

        pipeline.set_output()

    @classmethod
    def app_name(cls, args):
        if args.container:
            name = args.container[1].split('/')[-1]
        else:
            name = args.entrypoint
        return name

    @classmethod
    def workflow_name(cls, args):
        return args.container.replace('/', '_')

    @classmethod
    def parse_frequency(cls, args):
        return 'session' if args.analysis_level == 'participant' else 'group'


    VAR_ARG = 'BIDS_PATH'

    VAR_DESC = f"""
        The {VAR_ARG} is the path the that the file/field should be
        located within the constructed BIDS dataset with the file extension
        and subject and session sub-dirs entities omitted, e.g:

            anat/T1w

        for Session 1 of Subject 1 would be placed at the path
            
            sub-01/ses-01/anat/sub-01_ses-01_T1w.nii.gz

        Field datatypes should also specify where they are stored in the
        corresponding JSON side-cars using JSON path syntax, e.g.

            anat/T1w$ImageOrientationPatientDICOM[1]

        will be stored as the second item in the
        'ImageOrientationPatientDICOM' array in the JSON side car at

            sub-01/ses-01/anat/sub-01_ses-01_T1w.json"""
