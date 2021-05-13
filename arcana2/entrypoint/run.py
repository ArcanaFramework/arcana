from itertools import zip_longest
import re
from argparse import ArgumentParser
from typing import Sequence, Dict
from pydra import ShellCommandTask, Workflow, mark
from arcana2.data import (
    FileGroupMatcher, FieldMatcher, FileGroupSpec, FieldSpec)
from arcana2.data import file_format as ff
from arcana2.exceptions import ArcanaRequirementVersionsError, ArcanaUsageError
from arcana2.__about__ import __version__
from .util import resolve_class, init_repository


sanitize_path_re = re.compile(r'[^a-zA-Z\d]')

def sanitize_path(path):
    return sanitize_path_re.sub(path, '_')


class RunAppCmd():

    desc = ("Runs an app against a dataset stored in a repository. Can either "
            "be a BIDS app (with '--bids' flag) or shell command wrapped in "
            "a Pydra interface")

    @classmethod
    def make_parser(cls, parser):
        parser.add_argument(
            'app',
            help=("Either the entrypoint of the BIDS app if '--bids' option is"
                  " provided or otherwise the path to a Pydra interface that "
                  "wraps the app convenience the 'pydra.tasks' prefix can be "
                  "omitted (e.g. fsl.preprocess.first.First)"))
        parser.add_argument(
            'dataset_name',
            help=("Name of the dataset in the repository. For XNAT "
                  "repositories this is the project name, for file-system "
                  "repositories this is the path to the root folder"))
        parser.add_argument(
            'analysis_level',
            help=("The level at which the analysis is performed. One of (per) "
                  "dataset, group, subject, visit or session"))        
        parser.add_argument(
            '--bids', default=False, action='store_true',
            help=("Treat the app as a BIDS app, i.e. add conversion/extraction"
                  "nodes to the pipeline to create BIDS app and pass the args "
                  "to it in the runscript /input /output analysis_level "
                  "format. Otherwise the app is treated as a path to a Pydra "
                  "interface"))
        parser.add_argument(
            '--input', '-i', action='append', default=[], nargs='+',
            metavar=('PATH', 'PATTERN', 'FORMAT', 'ORDER', 'QUALITY',
                     'DICOM_TAGS', 'FREQUENCY'),
            help=INPUT_HELP)
        parser.add_argument(
            '--field_input', action='append', default=[], nargs='+',
            metavar=('PATH', 'FIELD_NAME', 'DTYPE', 'FREQUENCY'),
            help=FIELD_INPUT_HELP)
        parser.add_argument(
            '--output', '-o', action='append', default=[], nargs=2,
            metavar=('PATH', 'OUTPUT_NAME', 'FORMAT', 'FREQUENCY'),
            help=("The outputs to produced by the app."))
        parser.add_argument(
            '--field_output', action='append', default=[], nargs=2,
            metavar=('PATH', 'OUTPUT_NAME', 'DTYPE', 'FREQUENCY'),
            help=("The field outputs to produced by the app."))
        parser.add_argument(
            '--container', nargs=2, default=None,
            metavar=('ENGINE', 'IMAGE'),
            help=("The container engine ('docker'|'singularity') and the image"
                  " to run the app in"))
        parser.add_argument(
            '--flag', '-f', action='append', default=[],
            help=("Flag to pass to the app. Will be inserted before the first"
                  " argument"))
        parser.add_argument(
            '--repository', '-r', nargs='+', default='file_system',
            metavar='ARG',
            help=("Specify the repository type and any optionsto be passed to "
                  "it. First argument "))
        parser.add_argument(
            '--dry_run', action='store_true', default=False,
            help=("Set up the workflow to test inputs but don't run the app"))

    @classmethod
    def run(cls, args):
        # Create file-group matchers
        matchers = {}
        input_paths = {}
        defaults = (None, None, ff.niftix_gz, None, None, None, 'session')
        for i, inpt in enumerate(args.input):
            nargs = len(inpt)
            if nargs > 7:
                raise ArcanaUsageError(
                    f"Input {i} has too many input args, {nargs} instead "
                    f"of max 7 ({inpt})")
            (path, pattern, format_name, order, quality, dicom_tags, freq) = [
                a if a != '*' else d
                for a, d in zip_longest(inpt, defaults, fillvalue='*')]
            if not path:
                raise ArcanaUsageError(
                    f"Path must be provided to Input {i} ({inpt})")
            name = sanitize_path(path)
            input_paths[name] = path
            matchers[path] = FileGroupMatcher(
                pattern=pattern, format=ff.get_file_format(format_name),
                frequency='per_' + freq, order=order, dicom_tags=dicom_tags,
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
            matchers[path] = FieldMatcher(pattern=field_name, dtype=dtype,
                                          frequency='per_' + freq)

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
                                          frequency='per_' + freq)

        
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
            outputs[name] = FieldSpec(dtype=dtype, frequency='per_' + freq)

        repository = init_repository(args.repository)

        app_kwargs = {}

        workflow = Workflow()
        workflow.add(repository.source(name='source',
                                       dataset_name=args.dataset_name,
                                       matchers=matchers))
        if args.bids:
            workflow.add(construct_bids(name='construct_bids',
                                       input_paths=input_paths))
            workflow.add(ShellCommandTask(
                name='app',
                executable=args.app,
                args=' '.join(f'--{f} {v}' for f, v in args.flag),
                container_info=args.container))
            workflow.add(extract_bids(name='extract_bids',
                                      output_paths=output_paths))
            sink_from = workflow.extract_bids
        else:
            workflow.add(resolve_class(args.app, prefixes=['pydra.tasks'])(
                name='app',
                **app_kwargs))
            sink_from = workflow.app
        workflow.add(repository.sink(args.dataset_name, outputs)(
            name='sink',
            path=workflow.construct_bids.lzout.path,
            **{n: getattr(sink_from.lzout, n) for n in outputs}))

        if not args.dry_run:
            workflow.run()


@mark.task
@mark.annotate({'path': str})
def construct_bids(input_paths: Dict[str, str]):
    pass


@mark.task
def extract_bids(path: str, output_paths: Dict[str, str]):
    pass


INPUT_HELP = """
        A file-group input to provide to the app that is matched by the 
        provided criteria.

        By default the PATH argument is taken to the name of the
        input in the Pydra interface used by the app. If '--bids'
        flag is used, the PATH argument is taken to be the path
        to place the input within the constructed BIDS dataset,
        with the file extension and subject and session sub-dirs
        entities omitted, e.g:

            anat/T1w

        for Session 1 of Subject 1 would be placed at the path
            
            sub-01/anat/ses-01/sub-01_ses-01_T1w.nii.gz

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
            dicom_tags - semicolon-separated list of DICOM attributes
                         in TAG:VALUE form, where the tag contains
                         just the numeric values, i.e. no punctuation
                         e.g. '00080008:ORIGINAL\\PRIMARY\\M_IR\M\\IR;
                         00101010:067Y'
            frequency  - The frequency of the file-group within the dataset.
                         Can be either 'dataset', 'group', 'subject', 'visit'
                         or 'session'. Typically only required for
                         derivatives

        Trailing args can be dropped if default, 

            e.g. --input in_file 't1_mprage.*'
            
        Preceding args that aren't required can be replaced by '*', 

            --input in_file.nii.gz 't1_mprage.*' * * questionable"""


FIELD_INPUT_HELP = """
        A field input to provide to the app.

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

                    anat/T1w

        The DTYPE arg can be either 'float', 'int' or
        'string' (default) and defines the datatype the field
        will be transformed into. '[]' can be appended if the field
        is an array that is stored as a comma-separated list in
        the repository.
        
        The FREQUENCY arg specifies the frequency of the file-group
        within the dataset. It can be either 'dataset', 'group',
        'subject', 'visit' or 'session'. Typically only required for
        derivatives
"""
