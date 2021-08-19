from itertools import zip_longest
import re
from typing import Sequence
from pydra import ShellCommandTask, Workflow
from arcana2.data import (
    FileGroupSelector, FieldSelector, FileGroupSpec, FieldSpec)
from arcana2.data.repository.file_system_dir import single_dataset
from arcana2.data import file_format as ff
from arcana2.exceptions import ArcanaUsageError
from arcana2.__about__ import __version__
from arcana2.tasks.bids import construct_bids, extract_bids
from .base import BaseDatasetCmd
from .util import resolve_class


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
            '--dry_run', action='store_true', default=False,
            help=("Set up the workflow to test inputs but don't run the app"))



    @classmethod
    def run(cls, args):

        if args.ids is None:
            ids = None
        elif len(args.ids) == 1 and '/' in args.ids[0]:
            with open(args.ids[0]) as f:
                ids = f.read().split()
        else:
            ids = args.ids

        (inputs, outputs,
         input_names, output_names) = cls.parse_inputs_and_outputs(args)

        dataset = cls.get_dataset(args, inputs, outputs)

        frequency = cls.parse_frequency(args)

        workflow = Workflow(
            name=cls.app_name(args), input_spec=['ids'], ids=ids).split('ids')
        workflow.add(dataset.source_task(inputs=input_names,
                                         id=workflow.lzin.ids,
                                         frequency=frequency))

        app_outs = cls.add_app_task(workflow, args, input_names, output_names)
            
        workflow.add(dataset.sink_task(outputs=outputs,
                                       id=workflow.lzin.ids,
                                       **app_outs))

        if not args.dry_run:
            workflow.run()

    
    INPUT_HELP = """
        A file-group input to provide to the app that is matched by the 
        provided criteria.
        {path_desc}

        The criteria used to match the file-group (e.g. scan) in the
        repository follows the PATH arg in the following order:

            pattern   - regular expression (in Python syntax) of
                        file-group or field name
            format    - the name or extension of the file-format the
                        input is required in. Implicit conversions will
                        be attempted when required. The default is
                        'niftix_gz', which is the g-zipped NIfTI image file
                        + JSON side-car required for BIDS
            order     - the order of the scan in the session to select
                        if more than one match the other criteria. E.g.
                        an order of '2' with a pattern of '.*bold.*' could
                        match the second T1-weighted scan in the session
            quality   - the minimum usuable quality to be considered.
                        Can be one of 'usable', 'questionable' or
                        'unusable'
            header_vals  - semicolon-separated list of header_vals values
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


    FIELD_INPUT_HELP = """
        A field input to provide to the app.
        {path_desc}

        The DTYPE arg can be either 'float', 'int' or
        'string' (default) and defines the datatype the field
        will be transformed into. '[]' can be appended if the field
        is an array that is stored as a comma-separated list in
        the repository.
        
        The FREQUENCY arg specifies the frequency of the file-group
        within the dataset. It can be either 'dataset', 'group',
        'subject', 'timepoint' or 'session'. Typically only required for
        derivatives
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
        super().construct_parser(parser)
        parser.add_argument(
            '--app_arg', '-a', nargs=2, metavar=('NAME', 'VAL'),
            action='append', default=[],
            help=("Flag to pass to the app interface."))

    @classmethod
    def add_app_task(cls, workflow, args, input_paths, output_paths):
        task_cls = resolve_class(args.app, prefixes=['pydra.tasks'])
        app_args = cls.parse_app_args(args, task_cls)
        app_args.update((n, getattr(workflow.source.lzout, 'n'))
                        for n in input_paths)
        workflow.add(task_cls(name='app',
                              **app_args))
        return {n: getattr(workflow.app.lzout, n) for n in output_paths}


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

    PATH_DESC = """
        The NAME argument is the name of the input in the Pydra
        interface that wraps the app."""

    FIELD_PATH_DESC = """
        The NAME argument is the name of the input in the Pydra
        interface that wraps the app"""

    INPUT_ARG = "NAME"


    @classmethod
    def parse_frequency(cls, args):
        return args.frequency


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
        super().construct_parser(parser)
        parser.add_argument(
            '--flags', '-f', default='',
            help=("Arbitrary flags to pass onto the BIDS app (enclose in "
                  "quotation marks)"))

    @classmethod
    def parse_frequency(cls, args):
        if args.analysis_level == 'particpant':
            frequency = 'session'
        elif args.analysis_level == 'group':
            frequency = 'group'
        else:
            raise ArcanaUsageError(
                "Unrecognised analysis level '{}'".format(args.analysis_level))

    @classmethod
    def add_app_task(cls, workflow, args, input_paths, output_paths):
        workflow.add(construct_bids(name='construct_bids',
                     input_paths=input_paths))
        args = [args.analysis_level]
        if args.ids:
            args.append('--participant_label ' + ' '.join(args.ids))
        args.append(args.flags)
        workflow.add(ShellCommandTask(
            name='app',
            executable=args.entrypoint,
            args=' '.join(args),
            container_info=args.container))
        workflow.add(extract_bids(name='extract_bids',
                                  output_paths=output_paths))
        return {n: getattr(workflow.extract_bids.lzout, n)
                for n in output_paths}

    @classmethod
    def app_name(cls, args):
        if args.container:
            name = args.container[1].split('/')[-1]
        else:
            name = args.entrypoint
        return name

    INPUT_ARG = "PATH"

    PATH_DESC = """
        The PATH to place the input within the constructed BIDS dataset,
        with the file extension and subject and session sub-dirs
        entities omitted, e.g:

            anat/T1w

        for Session 1 of Subject 1 would be placed at the path
            
            sub-01/anat/ses-01/sub-01_ses-01_T1w.nii.gz
    """

    FIELD_PATH_DESC = """
        By default the PATH argument is taken to the name of the
        input in the Pydra interface of the app. If '--bids' flag
        is used, the PATH argument is taken to be the path to
        JSON file within the constructed BIDS dataset to place the
        field, omitting subect, session dirs and entities, and the
        '.json' extension, appended by the path to the field within
        the JSON using JSON path syntax path e.g.

            anat/T1w$ImageOrientationPatientDICOM[1]

        for Session 1 of Subject 1 would be placed in the second
        element of the "ImageOrientationPatientDICOM" array in
            
            sub-01/anat/ses-01/sub-01_ses-01_T1w.json

                    anat/T1w"""