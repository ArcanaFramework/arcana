from itertools import zip_longest
import re
from typing import Sequence
from arcana2.core.data.selector import DataSelector
from arcana2.core.data.spec import DataSpec
from arcana2.exceptions import ArcanaUsageError
from arcana2.__about__ import __version__
from arcana2.tasks.bids import construct_bids, extract_bids, bids_app
from .base import BaseDatasetCmd
from arcana2.core.utils import resolve_class, resolve_data_format


sanitize_path_re = re.compile(r'[^a-zA-Z\d]')

def sanitize_path(path):
    return sanitize_path_re.sub(path, '_')

class BaseRunCmd(BaseDatasetCmd):
    """Abstract base class for RunCmds
    """

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
            metavar=(cls.VAR_ARG, 'PATH', 'FORMAT', 'ORDER', 'QUALITY',
                     'METADATA', 'FREQUENCY'),
            help=cls.INPUT_HELP.format(var_desc=cls.VAR_DESC))
        parser.add_argument(
            '--output', '-o', action='append', default=[], nargs=4,
            metavar=(cls.VAR_ARG, 'STORE_AT', 'FORMAT', 'DESC'),
            help=cls.OUTPUT_HELP.format(var_desc=cls.VAR_DESC))
        parser.add_argument(
            '--required_format', action='append', default=[], nargs=2,
            metavar=('INPUT', 'FORMAT'),
            help=("The file format the app requires the input in. Only needed "
                  "when it differs from the format provided in the input"))
        parser.add_argument(
            '--produced_format', action='append', default=[], nargs=2,
            metavar=('OUTPUT', 'FORMAT'),
            help=("The file format the app produces the output in. Only needed"
                  " when it differs from the format to be stored in the "
                  "dataset."))
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
        inputs = cls.parse_inputs(args)
        outputs = cls.parse_outputs(args)

        workflow = dataset.workflow(
            name=cls.workflow_name(args),
            inputs=inputs,
            outputs=outputs,
            frequency=cls.parse_frequency(args),
            required_formats=cls.parse_required_formats(args),
            produced_formats=cls.parse_produced_formats(args),
            ids=args.ids)

        cls.add_app_task(workflow, args)

        if not args.dry_run:
            workflow.run()

    @classmethod
    def parse_inputs(cls, args):
        """Parses input arguments into dictionary of DataSelectors

        Parameters
        ----------
        args : ArgumentParser.namespace
            The parsed arguments from a ArgumentParser.parse_args() method

        Returns
        -------
        Dict[str, DataSelector]
            A dictionary of the specified inputs with the data-selectors to 
            chose the relevant files/fields from the dataset

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
        frequency = cls.parse_frequency(args)
        data_structure = type(frequency)
        # Create file-group matchers
        inputs = {}
        defaults = (None, None, None, None, None, None, 'session')
        for i, inpt in enumerate(args.input):
            nargs = len(inpt)
            if nargs > 7:
                raise ArcanaUsageError(
                    f"Input {i} has too many input args, {nargs} instead "
                    f"of max 7 ({inpt})")
            (var, pattern, file_format, order, quality, metadata, freq) = [
                a if a != '*' else d
                for a, d in zip_longest(inpt, defaults, fillvalue='*')]
            if not var:
                raise ArcanaUsageError(
                    f"{cls.VAR_ARG} must be provided for input {i} ({inpt})")
            if not pattern:
                raise ArcanaUsageError(
                    f"Path must be provided for input {i} ({inpt})")
            if not file_format:
                raise ArcanaUsageError(
                    f"Datatype must be provided for input {i} ({inpt})")
            inputs[var] = DataSelector(
                path=pattern, data_format=resolve_data_format(file_format),
                frequency=data_structure[freq], order=order,
                metadata=metadata, is_regex=True,
                quality_threshold=quality)
        return inputs

    @classmethod
    def parse_outputs(cls, args):
        """Parses output arguments into dictionary of DataSpecs

        Parameters
        ----------
        args : ArgumentParser.namespace
            The parsed arguments from a ArgumentParser.parse_args() method

        Returns
        -------
        Dict[str, DataSelector]
            A dictionary of the specified outputs with the data-specs that
            specify where outputs are stored in the dataset
        """
        frequency = cls.parse_frequency(args)
        # Create outputs
        outputs = {}
        for output in args.output:
            var, store_at, data_format, desc = output
            outputs[var] = DataSpec(
                path=store_at,
                data_format=resolve_data_format(data_format),
                desc=desc,
                frequency=frequency)
        return outputs

    @classmethod
    def parse_required_formats(cls, args):
        required = {}
        for inpt, frmt in args.required_format:
            required[inpt] = resolve_data_format(frmt)
        return required

    @classmethod
    def parse_produced_formats(cls, args):
        produced = {}
        for inpt, frmt in args.produced_format:
            produced[inpt] = resolve_data_format(frmt)
        return produced

        # # Create field outputs
        # defaults = (str, 'session')
        # for i, inpt in enumerate(args.field_input):
        #     nargs = len(output)
        #     if nargs < 2:
        #         raise ArcanaUsageError(
        #             f"Field Input {i} requires at least 2 args, "
        #             f"found {nargs} ({inpt})")
        #     if nargs > 4:
        #         raise ArcanaUsageError(
        #             f"Field Input {i} has too many input args, {nargs} "
        #             f"instead of max 4 ({inpt})")
        #     path, name, data_format, freq = inpt + defaults[nargs - 2:]
        #     output_names[name] = path
        #     outputs[name] = FieldSpec(data_format=data_format,
        #                               frequency=data_structure[freq])


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

        DESC is a short description (remember to enclose it in quotes) of what
        the output is
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
    def add_app_task(cls, workflow, args, inputs, outputs):
        task_cls = resolve_class(args.app, prefixes=['pydra.tasks'])

        app_args = cls.parse_app_args(args, task_cls)
        for inpt in inputs:
            app_args[inpt] = getattr(workflow.source, inpt)

        workflow.add(task_cls(name='app', **app_args))

        for output in outputs:
            setattr(workflow.sink, output, getattr(workflow.app.lzout, output))

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
        for name, val in args.arg:
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

    PATH = f"""
        The {VAR_ARG} is the attribute in the Pydra interface to connect
        the input to.
    """

    @classmethod
    def parse_frequency(cls, args):
        return cls.parse_data_structure(args)[args.frequency]

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
            help=("The level at which the analysis is performed. One of (per) "
                  "dataset, group, subject, timepoint or session"))
        parser.add_argument(
            '--flags', '-f', default='',
            help=("Arbitrary flags to pass onto the BIDS app (enclose in "
                  "quotation marks)"))
        super().construct_parser(parser)

    @classmethod
    def add_app_task(cls, workflow, args, inputs, outputs):

        workflow.add(construct_bids(name='construct_bids',
                                    inputs=workflow.lzin.inputs))

        workflow.add(bids_app(name='app',
                              app_name=args.app,
                              bids_dir=workflow.construct_bids.lzout.bids_dir,
                              analysis_level=args.analysis_level,
                              ids=args.ids,
                              flags=args.flags))

        workflow.add(extract_bids(name='extract_bids',
                                  bids_dir=workflow.app.lzout.bids_dir,
                                  outputs=workflow.lzin.outputs))

        workflow.sink.outputs = workflow.extract_bids.lzout.outputs

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

    VAR_ARG = 'INTERFACE_NAME'

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
