
import os
from importlib import import_module
import click
from arcana.data.dimensions.medicalimaging import ClinicalTrial
from arcana.exceptions import ArcanaUsageError
from arcana.data.stores.file_system import FileSystem
from arcana.data.stores.xnat import Xnat
from arcana.data.stores.xnat.cs import XnatViaCS
from arcana.core.cli import cli


XNAT_CACHE_DIR = 'xnat-cache'

@click.group()
def dataset():
    pass


@dataset.command(help=("""Define the tree structure and IDs to include in a
dataset. Where possible, the definition file is saved inside the dataset for
use by multiple users, if not possible it is stored in the ~/.arcana directory.

store
    The name data store the dataset is located in (see 'arcana store' command).
    For basic file system directories use 'file', and bids-formatted directories
    use 'bids'.
id
    ID of the dataset in the data store. For XNAT repositories this is the
    project ID, for file-system directories this is the path to the rdirectory
hierarchy
    The data frequencies that are present in the data tree. "
    For some store types this is fixed (e.g. XNAT-> subject > session) but "
    for more flexible (e.g. FileSystem), which dimension each layer of
    sub-directories corresponds to can be arbitrarily specified. dimensions
    "
"""))
@click.argument('id')
@click.argument('hierarchy', nargs=-1)
@click.option(
    '--included', nargs=2, default=[], metavar='<freq-id>',
    multiple=True,
    help=("The nodes to include in the dataset. First value is the "
           "frequency of the ID (e.g. 'group', 'subject', 'session') "
           "followed by the IDs to be included in the dataset. "
           "If the second arg contains '/' then it is interpreted as "
           "the path to a text file containing a list of IDs"))
@click.option(
    '--excluded', nargs=2, default=[], metavar='<freq-id>',
    multiple=True,
    help=("The nodes to exclude from the dataset. First value is the "
          "frequency of the ID (e.g. 'group', 'subject', 'session') "
          "followed by the IDs to be included in the dataset. "
          "If the second arg contains '/' then it is interpreted as "
          "the path to a text file containing a list of IDs"))
@click.option(
    '--store', '-s', type=str,
    help=("The nickname of the store (as added by `arcana store add ...`) the "
          "dataset is stored in"))
@click.option(
    '--dimensions', type=str, default='medicalimaging.ClinicalTrial',
    help=("The enum that specifies the data dimensions of the dataset. "
          "Defaults to `ClinicalTrial`, which "
          "consists of the typical dataset>group>subject>session "
          "data tree used in medicalimaging trials/studies"))
@click.option(
    '--id_inference', nargs=2, metavar='<source-regex>',
    multiple=True,
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
def define(id, hierarchy, included, excluded, store, dimensions, id_inference,
           name):
    raise NotImplementedError

    dimensions = cls.parse_dimensions(args)
    hierarchy = [dimensions[l]
                    for l in args.hierarchy] if args.hierarchy else None
    
    repo_args = list(args.store)
    repo_type = repo_args.pop(0)
    nargs = len(repo_args)
    if repo_type == 'file_system':
        store = FileSystem()
    elif repo_type == 'xnat':
        if nargs < 1 or nargs > 3:
            raise ArcanaUsageError(
                f"Incorrect number of arguments passed to an Xnat "
                f"store ({args}), at least 1 (SERVER) and no more "
                f"than 3 are required (SERVER, USER, PASSWORD)")
        optional_args = ['user', 'password']
        store = Xnat(
            server=repo_args[0],
            cache_dir=work_dir / XNAT_CACHE_DIR,
            **{k: v for k, v in zip(optional_args, repo_args[1:])})
        hierarchy = [ClinicalTrial.subject, ClinicalTrial.session]
    elif repo_type == 'xnat_via_cs':
        if nargs < 1 or nargs > 7:
            raise ArcanaUsageError(
                f"Incorrect number of arguments passed to an Xnat "
                f"store ({args}), at least 1 (FREQUENCY) is required "
                "and no more than 7 are permitted (FREQUENCY, NODE_ID, "
                "SERVER, USER, PASSWORD, INPUT_MOUNT, OUTPUT_MOUNT)")
        optional_args = ['node_id', 'server', 'user', 'password',
                            'input_mount', 'output_mount']
        store = XnatViaCS(
            cache_dir=work_dir / XNAT_CACHE_DIR,
            frequency=ClinicalTrial[repo_args[0]],
            **{k: v for k, v in zip(optional_args, repo_args[1:])})
        hierarchy = [ClinicalTrial.subject, ClinicalTrial.session]
    else:
        raise ArcanaUsageError(
            f"Unrecognised store type provided as first argument "
            f"to '--store' option ({repo_type})")

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
    
    return store.dataset(args.dataset_name,
                                hierarchy=hierarchy,
                                id_inference=id_inference,
                                included=parse_ids(args.included),
                                excluded=parse_ids(args.excluded))

@dataset.command(help="""
Renames a data store saved in the stores.yml to a new name

old_name
    The current name of the store
new_name
    The new name for the store""")
@click.argument('old_name')
@click.argument('new_name')
def rename(old_name, new_name):
    raise NotImplementedError


def optional_args(names, args):
    kwargs = {}