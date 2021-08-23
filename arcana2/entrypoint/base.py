
import os.path
import re
from itertools import zip_longest
from arcana2.exceptions import ArcanaUsageError
from arcana2.data.repository import FileSystemDir, Xnat, XnatCS
from arcana2.data import (
    FileGroupSelector, FieldSelector, FileGroupSpec, FieldSpec)
from arcana2.data import file_format as ff
import arcana2.data.frequency



sanitize_path_re = re.compile(r'[^a-zA-Z\d]')

def sanitize_path(path):
    return sanitize_path_re.sub(path, '_')


class BaseDatasetCmd():

    @classmethod
    def construct_parser(cls, parser):
        parser.add_argument(
            'dataset_name',
            help=("Name of the dataset in the repository. For XNAT "
                  "repositories this is the project name, for file-system "
                  "repositories this is the path to the root directory"))
        parser.add_argument(
            '--input', '-i', action='append', default=[], nargs='+',
            metavar=('NAME', 'PATTERN', 'FORMAT', 'ORDER', 'QUALITY',
                     'DICOM_TAGS', 'FREQUENCY'),
            help=cls.INPUT_HELP.format(path_desc=cls.PATH_DESC))
        parser.add_argument(
            '--field_input', action='append', default=[], nargs='+',
            metavar=('NAME', 'FIELD_NAME', 'DTYPE', 'FREQUENCY'),
            help=cls.FIELD_INPUT_HELP.format(path_desc=cls.FIELD_PATH_DESC))
        parser.add_argument(
            '--output', '-o', action='append', default=[], nargs=2,
            metavar=('NAME', 'PATH', 'FORMAT', 'FREQUENCY'),
            help=("The outputs produced by the app to be stored in the "
                  "repository. The first name is the output"))
        parser.add_argument(
            '--field_output', action='append', default=[], nargs=2,
            metavar=('NAME', 'PATH', 'DTYPE', 'FREQUENCY'),
            help=("The field outputs produced by the app to be stored in the "
                  "repository"))
        parser.add_argument(
            '--include_ids', nargs='+', default=None, metavar=('FREQ', 'ID'),
            action='append',
            help=("The IDs to include in the dataset. First value is the "
                  "frequency of the ID (e.g. 'group', 'subject', 'session') "
                  "followed by the IDs to be included in the dataset. "
                  "If the second arg contains '/' then it is interpreted as "
                  "the path to a text file containing a list of IDs"))
        parser.add_argument(
            '--exclude_ids', nargs='+', default=None, metavar=('FREQ', 'ID'),
            action='append',
            help=("The IDs to exclude from the dataset. First value is the "
                  "frequency of the ID (e.g. 'group', 'subject', 'session') "
                  "followed by the IDs to be included in the dataset. "
                  "If the second arg contains '/' then it is interpreted as "
                  "the path to a text file containing a list of IDs"))        
        parser.add_argument(
            '--repository', '-r', nargs='+', default=['file_system'],
            metavar='ARG',
            help=("Specify the repository type and any options to be passed to"
                  " it. The first argument is the type of repository, either "
                  "'file_system', 'xnat' or 'xnat_cs'. The remaining arguments"
                  " depend on the type of repository:\n"
                  "\tfile_system: BASE_DIR\n"
                  "\txnat: SERVER_URL, USERNAME, PASSWORD\n"
                  "\txnat_cs: SUBJECT TIMEPOINT\n"))
        parser.add_argument(
            '--dataset_structure', type=str, default='Clinical',
            help=("The enum that specifies the data frequencies present in "
                  "repository. Default to the `Clinical`, which "
                  "consists of the typical dataset>group>subject>session "
                  "data tree used in clinical trials/studies"))
        parser.add_argument(
            '--id_inference', nargs=2, metavar=('SOURCE', 'REGEX'),
            action='append',
            help="""Specifies how IDs of primary data frequencies that not explicitly
provided are inferred from the IDs that are. For example, given a set
of subject IDs that are a combination of the ID of the group that they belong
to and their member IDs (i.e. matched test/controls have same member ID), e.g.

    CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

the group ID can be extracted by providing the ID to source it from
(i.e. subject) and a regular expression (Python regex syntax)with a named
groups corresponding to the inferred IDs

    --id_inference subject '(?P<group>[A-Z]+)(?P<member>[0-9]+)'

""")

    @classmethod
    def get_dataset(cls, args):
        """Initialises a repository and then gets a dataset from it
        """
        repo_args = list(args.repository)
        repo_type = repo_args.pop(0)
        nargs = len(repo_args)
        if repo_type == 'file_system':
            repository = FileSystemDir()
        elif repo_type == 'xnat':
            if nargs < 1 or nargs > 3:
                raise ArcanaUsageError(
                    f"Incorrect number of arguments passed to an Xnat "
                    f"repository ({args}), at least 1 (SERVER) and no more "
                    f"than 3 are required (SERVER, USER, PASSWORD)")
            repository = Xnat(
                server=args[0],
                user=args[1] if nargs > 1 else None,
                password=args[2] if nargs > 2 else None)
        elif repo_type == 'xnat_cs':
            if nargs < 1 or nargs > 3:
                raise ArcanaUsageError(
                    f"Incorrect number of arguments passed to an Xnat "
                    f"repository ({args}), at least 1 (LEVEL) and no more "
                    f"than 3 are required (LEVEL, SUBJECT, VISIT)")
            repository = XnatCS(
                level=args[0],
                ids={'subject': args[1] if nargs > 1 else None,
                     'timepoint': args[2] if nargs > 2 else None})
        else:
            raise ArcanaUsageError(
                f"Unrecognised repository type provided as first argument "
                f"to '--repository' option ({repo_type})")

        dataset_structure = getattr(arcana2.data.frequency,
                                    args.dataset_structure)

        (inputs, outputs,
         input_names, output_names) = cls.parse_inputs_and_outputs(
             args, dataset_structure)

        if args.id_inference:
            id_inference = {t: (s, r) for t, s, r in args.ids_inference}
        else:
            id_inference = None


        def parse_ids(ids_args):
            parsed_ids = {}
            for iargs in ids_args:
                freq = dataset_structure[iargs.pop(0)]
                if len(iargs) == 1 and '/' in iargs[0]:
                    with open(args.ids[0]) as f:
                        ids = f.read().split()
                else:
                    ids = args.ids
                parsed_ids[freq] = ids
            return parsed_ids
        
        return (repository.dataset(args.dataset_name,
                                   selectors=inputs,
                                   derivatives=outputs,
                                   structure=dataset_structure,
                                   id_inference=id_inference,
                                   include_ids=parse_ids(args.include_ids),
                                   exclude_ids=parse_ids(args.exclude_ids)),
                input_names,
                output_names)

    

    @classmethod
    def parse_inputs_and_outputs(cls, args, dataset_structure):
        # Create file-group matchers
        inputs = {}
        input_names = {}
        defaults = (None, None, None, None, None, None, 'session')
        for i, inpt in enumerate(args.input):
            nargs = len(inpt)
            if nargs > 7:
                raise ArcanaUsageError(
                    f"Input {i} has too many input args, {nargs} instead "
                    f"of max 7 ({inpt})")
            (path, pattern, format_name, order,
             quality, header_vals, freq) = [
                a if a != '*' else d
                for a, d in zip_longest(inpt, defaults, fillvalue='*')]
            if not path:
                raise ArcanaUsageError(
                    f"Path must be provided to Input {i} ({inpt})")
            if not pattern:
                raise ArcanaUsageError(
                    f"Pattern must be provided to Input {i} ({inpt})")
            if not format_name:
                raise ArcanaUsageError(
                    f"Datatype must be provided to Input {i} ({inpt})")
            name = sanitize_path(path)
            input_names[name] = path
            inputs[path] = FileGroupSelector(
                name_path=pattern, format=getattr(ff, format_name),
                frequency=dataset_structure[freq], order=order,
                header_vals=header_vals, is_regex=True,
                acceptable_quality=quality)

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
            input_names[name] = path
            inputs[path] = FieldSelector(pattern=field_name, dtype=dtype,
                                         frequency=dataset_structure[freq])

        outputs = {}
        output_names = {}
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
            output_names[name] = path
            outputs[name] = FileGroupSpec(format=ff.get_format(file_format),
                                          frequency=dataset_structure[freq])

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
            output_names[name] = path
            outputs[name] = FieldSpec(dtype=dtype,
                                      frequency=dataset_structure[freq])

        return inputs, outputs, input_names, output_names

    
    
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