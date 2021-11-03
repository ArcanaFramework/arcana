
import os
from importlib import import_module
from arcana2.data.spaces.clinical import Clinical
from arcana2.exceptions import ArcanaUsageError
from arcana2.data.repositories.file_system import FileSystem
from arcana2.data.repositories.xnat import Xnat
from arcana2.data.repositories.xnat.cs import XnatViaCS
from arcana2.core.entrypoint import BaseCmd


XNAT_CACHE_DIR = 'xnat-cache'

class BaseDatasetCmd(BaseCmd):

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
            '--dataspace', type=str, default='clinical.Clinical',
            help=("The enum that specifies the data dimensions of the dataset. "
                  "Defaults to `Clinical`, which "
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
        parser.add_argument(
            '--hierarchy', nargs='+', default=None,
            help=("The data frequencies that are present in the data tree. "
                  "For some repository types this is fixed (e.g. XNAT) but "
                  "for more flexible (e.g. FileSystem) the number of hierarchy "
                  "in the data tree, and what each layer corresponds to, "
                  "needs to specified. Defaults to all the hierarchy in the "
                  "data dimensions"))

    @classmethod
    def get_dataset(cls, args, work_dir):
        """Initialises a repository and then gets a dataset from it
        """

        dimensions = cls.parse_dataspace(args)
        hierarchy = [dimensions[l]
                     for l in args.hierarchy] if args.hierarchy else None
        
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
                server=repo_args[0],
                user=repo_args[1] if nargs > 1 else None,
                password=repo_args[2] if nargs > 2 else None,
                cache_dir=work_dir / XNAT_CACHE_DIR)
            hierarchy = [Clinical.subject, Clinical.session]
        elif repo_type == 'xnat_via_cs':
            if nargs < 1 or nargs > 7:
                raise ArcanaUsageError(
                    f"Incorrect number of arguments passed to an Xnat "
                    f"repository ({args}), at least 1 (FREQUENCY) and no more "
                    f"than 4 are required (FREQUENCY, NODE_ID, INPUT_MOUNT, OUTPUT_MOUNT)")
            repository = XnatViaCS(
                server=repo_args[0],
                user=repo_args[1],
                password=repo_args[2],
                cache_dir=work_dir / XNAT_CACHE_DIR,
                frequency=Clinical[repo_args[3]],
                node_id=repo_args[4] if len(repo_args) > 4 else None,
                input_mount=repo_args[5] if len(repo_args) > 5 else XnatViaCS.INPUT_MOUNT,
                output_mount=repo_args[6] if len(repo_args) > 6 else XnatViaCS.OUTPUT_MOUNT)
            hierarchy = [Clinical.subject, Clinical.session]
        else:
            raise ArcanaUsageError(
                f"Unrecognised repository type provided as first argument "
                f"to '--repository' option ({repo_type})")

        if args.id_inference:
            id_inference = {t: (s, r) for t, s, r in args.ids_inference}
        else:
            id_inference = None

        if hierarchy is None:
            hierarchy = [max(dimensions)]

        def parse_ids(ids_args):
            parsed_ids = {}
            for iargs in ids_args:
                freq = dimensions[iargs.pop(0)]
                if len(iargs) == 1 and '/' in iargs[0]:
                    with open(args.ids[0]) as f:
                        ids = f.read().split()
                else:
                    ids = args.ids
                parsed_ids[freq] = ids
            return parsed_ids
        
        return repository.dataset(args.dataset_name,
                                  hierarchy=hierarchy,
                                  id_inference=id_inference,
                                  included=parse_ids(args.included),
                                  excluded=parse_ids(args.excluded))

    @classmethod
    def parse_dataspace(cls, args):
        parts = args.dataspace.split('.')
        if len(parts) < 2:
            raise ArcanaUsageError(
                f"Value provided to '--dataspace' arg ({args.dataspace}) "
                "needs to include module, either relative to "
                "'arcana2.dataspaces' (e.g. clinical.Clinical) or an "
                "absolute path")
        module_path = '.'.join(parts[:-1])
        cls_name = parts[-1]
        try:
            module = import_module('arcana2.data.spaces.' + module_path)
        except ImportError:
            module = import_module(module_path)
        return getattr(module, cls_name)
