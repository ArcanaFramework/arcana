
import re
from arcana2.exceptions import ArcanaUsageError
from arcana2.repository.file_system import FileSystem
from arcana2.repository.xnat import Xnat
from arcana2.repository.xnat_cs import XnatCS
import arcana2.core.data.enum


class BaseDatasetCmd():

    @classmethod
    def construct_parser(cls, parser):
        parser.add_argument(
            'dataset_name',
            help=("Name of the dataset in the repository. For XNAT "
                  "repositories this is the project name, for file-system "
                  "repositories this is the path to the root directory"))    
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
            '--included', nargs=2, default=[], metavar=('FREQ', 'ID'),
            action='append',
            help=("The nodes to include in the dataset. First value is the "
                  "frequency of the ID (e.g. 'group', 'subject', 'session') "
                  "followed by the IDs to be included in the dataset. "
                  "If the second arg contains '/' then it is interpreted as "
                  "the path to a text file containing a list of IDs"))
        parser.add_argument(
            '--excluded', nargs=2, default=[], metavar=('FREQ', 'ID'),
            action='append',
            help=("The nodes to exclude from the dataset. First value is the "
                  "frequency of the ID (e.g. 'group', 'subject', 'session') "
                  "followed by the IDs to be included in the dataset. "
                  "If the second arg contains '/' then it is interpreted as "
                  "the path to a text file containing a list of IDs"))    
        parser.add_argument(
            '--data_structure', type=str, default='ClinicalTrial',
            help=("The enum that specifies the data structure of the dataset. "
                  "Defaults to `ClinicalTrial`, which "
                  "consists of the typical dataset>group>subject>session "
                  "data tree used in clinical trials/studies"))
        parser.add_argument(
            '--id_inference', nargs=2, metavar=('SOURCE', 'REGEX'),
            action='append',
            help="""Specifies how IDs of node frequencies that not explicitly
    provided are inferred from the IDs that are. For example, given a set
    of subject IDs that are a combination of the ID of the group that they belong
    to + their member IDs (i.e. matched test/controls have same member ID), e.g.

        CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

    the group ID can be extracted by providing the ID to source it from
    (i.e. subject) and a regular expression (in Python regex syntax:
    https://docs.python.org/3/library/re.html) with a named
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
            repository = FileSystem()
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

        if args.id_inference:
            id_inference = {t: (s, r) for t, s, r in args.ids_inference}
        else:
            id_inference = None

        data_structure = cls.parse_data_structure(args)

        def parse_ids(ids_args):
            parsed_ids = {}
            for iargs in ids_args:
                freq = data_structure[iargs.pop(0)]
                if len(iargs) == 1 and '/' in iargs[0]:
                    with open(args.ids[0]) as f:
                        ids = f.read().split()
                else:
                    ids = args.ids
                parsed_ids[freq] = ids
            return parsed_ids
        
        return repository.dataset(args.dataset_name,
                                  data_structure=data_structure,
                                  id_inference=id_inference,
                                  included=parse_ids(args.included),
                                  excluded=parse_ids(args.excluded))

    @classmethod
    def parse_data_structure(cls, args):
        return getattr(arcana2.core.data.enum, args.data_structure)
