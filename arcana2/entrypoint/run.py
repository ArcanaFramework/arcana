from itertools import zip_longest
import re
from argparse import ArgumentParser
from typing import Sequence
from pydra import ShellCommandTask, Workflow
from arcana2.data import (
    FileGroupMatcher, FieldMatcher, FileGroupSpec, FieldSpec)
from arcana2.data import file_format as ff
from arcana2.exceptions import ArcanaRequirementVersionsError, ArcanaUsageError
from arcana2.__about__ import __version__
from arcana2.tasks.bids import construct_bids, extract_bids
from .base import BaseRepoCmd
from .util import resolve_class


sanitize_path_re = re.compile(r'[^a-zA-Z\d]')

def sanitize_path(path):
    return sanitize_path_re.sub(path, '_')

class BaseRunCmd(BaseRepoCmd):
    """Abstract base class for RunCmds
    """

    @classmethod
    def construct_parser(cls, parser):
        parser.add_argument(
            'dataset_name',
            help=("Name of the dataset in the repository. For XNAT "
                  "repositories this is the project name, for file-system "
                  "repositories this is the path to the root directory"))
        super().construct_parser(parser)
        parser.add_argument(
            '--input', '-i', action='append', default=[], nargs='+',
            metavar=(cls.INPUT_ARG, 'PATTERN', 'FORMAT', 'ORDER', 'QUALITY',
                     'DICOM_TAGS', 'FREQUENCY'),
            help=cls.INPUT_HELP.format(path_desc=cls.PATH_DESC))
        parser.add_argument(
            '--field_input', action='append', default=[], nargs='+',
            metavar=(cls.INPUT_ARG, 'FIELD_NAME', 'DTYPE', 'FREQUENCY'),
            help=cls.FIELD_INPUT_HELP.format(path_desc=cls.FIELD_PATH_DESC))
        parser.add_argument(
            '--output', '-o', action='append', default=[], nargs=2,
            metavar=(cls.INPUT_ARG, 'OUTPUT_NAME', 'FORMAT', 'FREQUENCY'),
            help=("The outputs produced by the app to be stored in the "
                  "repository."))
        parser.add_argument(
            '--field_output', action='append', default=[], nargs=2,
            metavar=(cls.INPUT_ARG, 'OUTPUT_NAME', 'DTYPE', 'FREQUENCY'),
            help=("The field outputs produced by the app to be stored in the "
                  "repository"))
        parser.add_argument(
            '--ids', nargs='+', default=None, metavar='ID',
            help=("The IDs (group, subject, visit or session) to include in "
                  "the analysis. If a single value with a '/' is provided "
                  "then it is interpreted as a text file containing a list "
                  "of IDs"))
        parser.add_argument(
            '--container', nargs=2, default=None,
            metavar=('ENGINE', 'IMAGE'),
            help=("The container engine ('docker'|'singularity') and the image"
                  " to run the app in"))
        parser.add_argument(
            '--dry_run', action='store_true', default=False,
            help=("Set up the workflow to test inputs but don't run the app"))


    @classmethod
    def parse_inputs_and_outputs(cls, args):
        # Create file-group matchers
        inputs = {}
        input_paths = {}
        defaults = (None, None, ff.niftix_gz, None, None, None, 'session')
        for i, inpt in enumerate(args.input):
            nargs = len(inpt)
            if nargs > 7:
                raise ArcanaUsageError(
                    f"Input {i} has too many input args, {nargs} instead "
                    f"of max 7 ({inpt})")
            (path, pattern, format_name, order,
             quality, metadata, freq) = [
                a if a != '*' else d
                for a, d in zip_longest(inpt, defaults, fillvalue='*')]
            if not path:
                raise ArcanaUsageError(
                    f"Path must be provided to Input {i} ({inpt})")
            name = sanitize_path(path)
            input_paths[name] = path
            inputs[path] = FileGroupMatcher(
                pattern=pattern, format=ff.get_file_format(format_name),
                tree_level='per_' + freq, order=order, metadata=metadata,
                is_regex=True, acceptable_quality=quality)

        # Create field matchers
        defaults = (str, 'session')
        for i, inpt in enumerate(args.field_input):
            nargs = len(inpt)
            if len(inpt) < 2:
                raise ArcanaUsageError(
                    f"Output {i} requires at least 2 args, "
                    f"found {nargs} ({inpt})")
            if len(inpt) > 4:
                raise ArcanaUsageError(
                    f"Output {i} has too many input args, {nargs} "
                    f"instead of max 4 ({inpt})")
            path, field_name, dtype, freq = inpt + defaults[nargs - 2:]
            name = sanitize_path(path)
            input_paths[name] = path
            inputs[path] = FieldMatcher(pattern=field_name, dtype=dtype,
                                        tree_level='per_' + freq)

        outputs = {}
        output_paths = {}
        # Create outputs
        defaults = (ff.niftix_gz, 'session')
        for i, output in enumerate(args.field_output):
            nargs = len(output)
            if nargs < 2:
                raise ArcanaUsageError(
                    f"Field Output {i} requires at least 2 args, "
                    f"found {nargs} ({output})")
            if nargs> 4:
                raise ArcanaUsageError(
                    f"Field Output {i} has too many input args, {nargs} "
                    f"instead of max 4 ({output})")
            path, name, file_format, freq = inpt + defaults[nargs - 2:]
            output_paths[name] = path
            outputs[name] = FileGroupSpec(format=ff.get_format(file_format),
                                          tree_level='per_' + freq)

        
        # Create field outputs
        defaults = (str, 'session')
        for i, inpt in enumerate(args.field_input):
            nargs = len(output)
            if nargs < 2:
                raise ArcanaUsageError(
                    f"Field Input {i} requires at least 2 args, "
                    f"found {nargs} ({inpt})")
            if nargs > 4:
                raise ArcanaUsageError(
                    f"Field Input {i} has too many input args, {nargs} "
                    f"instead of max 4 ({inpt})")
            path, name, dtype, freq = inpt + defaults[nargs - 2:]
            output_paths[name] = path
            outputs[name] = FieldSpec(dtype=dtype, tree_level='per_' + freq)

        return inputs, outputs, input_paths, output_paths


    @classmethod
    def run(cls, args):

        if args.ids is None:
            ids = None
        elif len(args.ids) == 1 and '/' in args.ids[0]:
            with open(args.ids[0]) as f:
                ids = f.read().split()
        else:
            ids = args.ids

        repository = cls.init_repository(args.repository)

        (inputs, outputs,
         input_paths, output_paths) = cls.parse_inputs_and_outputs(args)

        tree_level = cls.parse_tree_level(args)

        workflow = Workflow(name=cls.app_name(args))
        workflow.add(repository.source(dataset_name=args.dataset_name,
                                       inputs=inputs,
                                       id=ids,
                                       tree_level=tree_level))

        app_outs = cls.add_app_task(workflow, args, input_paths, output_paths)
            
        workflow.add(repository.sink(dataset_name=args.dataset_name,
                                     outputs=outputs,
                                     tree_level=tree_level,
                                     id=workflow.source.lzout.id,
                                     **app_outs))

        if not args.dry_run:
            workflow.run()

    
    INPUT_HELP = """
        A file-group input to provide to the app that is matched by the 
        provided criteria.
        {path_desc}

        The criteria used to match the file-group (e.g. scan) in the
        repository follows the PATH arg in the following order:

            pattern    - regular expression (in Python syntax) of
                        file-group or field name
            format     - the name or extension of the file-format the
                        input is required in. Implicit conversions will
                        be attempted when required. The default is
                        'niftix_gz', which is the g-zipped NIfTI image file
                        + JSON side-car required for BIDS
            order      - the order of the scan in the session to select
                        if more than one match the other criteria. E.g.
                        an order of '2' with a pattern of '.*bold.*' could
                        match the second T1-weighted scan in the session
            quality    - the minimum usuable quality to be considered.
                        Can be one of 'usable', 'questionable' or
                        'unusable'
            metadata   - semicolon-separated list of metadata values
                        in NAME:VALUE form. For DICOM headers
                        NAME is the numeric values of the DICOM tag, e.g
                        (0008,0008) -> 00080008
            tree_level  - The tree_level of the file-group within the dataset.
                        Can be either 'dataset', 'group', 'subject', 'visit'
                        or 'session'. Typically only required for
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
        
        The FREQUENCY arg specifies the tree_level of the file-group
        within the dataset. It can be either 'dataset', 'group',
        'subject', 'visit' or 'session'. Typically only required for
        derivatives
    """


class RunAppCmd():

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
            '--tree_level', '-f', default='session',
            help=("The level at which the analysis is performed. One of (per) "
                  "dataset, group, subject, visit, group_visit or session"))        
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
    def parse_tree_level(cls, args):
        valid_frequencies = ('dataset', 'group', 'subject', 'visit',
                             'subject_visit', 'session')
        if args.tree_level not in valid_frequencies:
            raise ArcanaUsageError(
                f"Unrecognised tree_level '{args.tree_level}' "
                f"(valid: {valid_frequencies})")
        return 'per_' + args.tree_level

    @classmethod
    def app_name(cls, args):
        return args.app.split('.')[-1].lower()

    PATH_DESC = """
        The NAME argument is the name of the input in the Pydra
        interface that wraps the app."""

    FIELD_PATH_DESC = """
        The NAME argument is the name of the input in the Pydra
        interface that wraps the app"""


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
                  "dataset, group, subject, visit or session"))
        super().construct_parser(parser)
        parser.add_argument(
            '--flags', '-f', default='',
            help=("Arbitrary flags to pass onto the BIDS app (enclose in "
                  "quotation marks)"))

    @classmethod
    def parse_tree_level(cls, args):
        if args.analysis_level == 'particpant':
            tree_level = 'per_session'
        elif args.analysis_level == 'group':
            tree_level = 'per_group'
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


    PATH_DESC = """
        By default the PATH argument is taken to the name of the
        input in the Pydra interface used by the app. If '--bids'
        flag is used, the PATH argument is taken to be the path
        to place the input within the constructed BIDS dataset,
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