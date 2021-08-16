
import typing as ty
from arcana2.exceptions import ArcanaUsageError
from arcana2.data.repository import FileSystemDir, Xnat, XnatCS, Repository



class BaseRepoCmd():

    @classmethod
    def construct_parser(cls, parser):
        parser.add_argument(
            '--repository', '-r', nargs='+', default='file_system',
            metavar='ARG',
            help=("Specify the repository type and any optionsto be passed to "
                  "it. First argument "))

    @classmethod
    def init_repository(cls, args: ty.Sequence[ty.Any]) -> Repository:
        try:
            repo_type = args.pop(0)
        except IndexError:
            raise ArcanaUsageError(
                f"Repository type not provided to '--repository' option")
        nargs = len(args)
        if repo_type == 'file_system':
            repository = FileSystemDir(base_dir=args[0], frequencies=args[1:])
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
        return repository