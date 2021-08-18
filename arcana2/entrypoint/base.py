
import typing as ty
import os.path
from arcana2.exceptions import ArcanaUsageError
from arcana2.data.repository import (
    FileSystemDir, Xnat, XnatCS, Repository)
import arcana2.data.frequency



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
            '--hierarchy', nargs='+', metavar='FREQ',
            default=['session'],
            help=("""The order data frequencies appear in the repository when
using file-system directory repositories. For example,

    hierarchy=['group', 'subject', 'session']

designates a directory structure with three layers, the top level
of sub-directories corresponding to the groups in the study
(e.g. "control" and "test"), the next layer corresponding to the
members in each group, and the final layer the time-points for each
session the subject was scanned.

Alternatively,

    hierarchy=['member', 'session']

Would specify a 2-level directory structure with the a directory in
the top layer for each matched members (i.e. test & control pairs) each
containing sub-directories for each subject in the match.

Note that binary string for each subsequent frequency in the hierarchy
should be a superset of the ones that come before it, e.g.

    (100, 110, 111), (010, 110, 111), or (001, 101, 111)"""))
        parser.add_argument(
            '--enum_class', type=str, default='Clinical',
            help=("The class the frequencies laid out in the data structure "
                  "will be instantiated as. Default to the `Clinical`, which "
                  "consists of the typical dataset>group>subject>session "
                  "data tree used in clinical trials/studies"))
        parser.add_argument(
            '--id_inference', nargs=3, metavar=('TARGET, SOURCE', 'REGEX'),
            action='append',
            help="""Specifies how IDs of primary data frequencies that not explicitly
provided are inferred from the IDs that are. For example, given a set
of subject IDs contain the ID of the group that they belong to in them

    CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

the group ID can be extracted by providing a dictionary with tuple
values containing the ID type of the ID to infer it from and a regex
that extracts the target ID from the provided ID (in the first group).

    id_inference={
        Clincal.group: (Clinical.subject, r'([a-zA-Z]+).*')}

Alternatively, a general function with signature `f(ids)` that returns
a dictionary with the mapped IDs can be provided instead.""")

    @classmethod
    def init_repository(cls, args):
        if args.id_inference:
            id_inference = {t: (s, r) for t, s, r in args.ids_inference}
        else:
            id_inference = None
        repo_args = list(args.repository)
        repo_type = repo_args.pop(0)
        nargs = len(repo_args)
        dataset_name = args.dataset_name
        if repo_type == 'file_system':
            # Assume that the dataset_name is the direct path to the
            # the dataset directory
            if not repo_args:
                base_dir = os.path.join(dataset_name, '..')
                dataset_name = os.path.basename(dataset_name)
            else:
                base_dir = repo_args[0]
            freq_enum = getattr(arcana2.data.frequency, args.enum_class)
            repository = FileSystemDir(base_dir=base_dir,
                                       hierarchy=args.hierarchy,
                                       frequency_enum=freq_enum,
                                       id_inference=id_inference)
        elif repo_type == 'xnat':
            if nargs < 1 or nargs > 3:
                raise ArcanaUsageError(
                    f"Incorrect number of arguments passed to an Xnat "
                    f"repository ({args}), at least 1 (SERVER) and no more "
                    f"than 3 are required (SERVER, USER, PASSWORD)")
            repository = Xnat(
                server=args[0],
                user=args[1] if nargs > 1 else None,
                password=args[2] if nargs > 2 else None,
                id_inference=id_inference)
        elif repo_type == 'xnat_cs':
            if nargs < 1 or nargs > 3:
                raise ArcanaUsageError(
                    f"Incorrect number of arguments passed to an Xnat "
                    f"repository ({args}), at least 1 (LEVEL) and no more "
                    f"than 3 are required (LEVEL, SUBJECT, VISIT)")
            repository = XnatCS(
                level=args[0],
                ids={'subject': args[1] if nargs > 1 else None,
                     'timepoint': args[2] if nargs > 2 else None},
                id_inference=id_inference)
        else:
            raise ArcanaUsageError(
                f"Unrecognised repository type provided as first argument "
                f"to '--repository' option ({repo_type})")
        return repository, dataset_name