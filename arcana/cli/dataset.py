
import click
from arcana.core.cli import cli
from arcana.core.data.set import Dataset
from arcana.core.data.store import DataStore
from arcana.core.utils import resolve_class


XNAT_CACHE_DIR = 'xnat-cache'

@cli.group()
def dataset():
    pass


@dataset.command(name='define',
                 help=("""Define the tree structure and IDs to include in a
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
    '--space', type=str, default=None,
    help=("The \"space\" of the dataset, defines the dimensions along the ids "
          "of node can vary"))
@click.option(
    '--include', nargs=2, default=[], metavar='<freq-id>',
    multiple=True,
    help=("The nodes to include in the dataset. First value is the "
           "frequency of the ID (e.g. 'group', 'subject', 'session') "
           "followed by the IDs to be included in the dataset. "
           "If the second arg contains '/' then it is interpreted as "
           "the path to a text file containing a list of IDs"))
@click.option(
    '--exclude', nargs=2, default=[], metavar='<freq-id>',
    multiple=True,
    help=("The nodes to exclude from the dataset. First value is the "
          "frequency of the ID (e.g. 'group', 'subject', 'session') "
          "followed by the IDs to be included in the dataset. "
          "If the second arg contains '/' then it is interpreted as "
          "the path to a text file containing a list of IDs"))
@click.option(
    '--space', default='medimage:Clinical',
    help=("The enum that specifies the data dimensions of the dataset. "
          "Defaults to `Clinical`, which "
          "consists of the typical dataset>group>subject>session "
          "data tree used in medimage trials/studies"))
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
def define(id, hierarchy, include, exclude, space, id_inference):

    store_name, id, name = Dataset.parse_id_str(id)

    if not hierarchy:
        hierarchy = None

    store = DataStore.load(store_name)

    if space:
        space = resolve_class(space, ['arcana.data.spaces'])
    
    dataset = store.new_dataset(
        id,
        hierarchy=hierarchy,
        space=space,
        id_inference=id_inference,
        include=include,
        exclude=exclude)

    dataset.save(name)

@dataset.command(help="""
Renames a data store saved in the stores.yml to a new name

dataset_path
    The current name of the store
new_name
    The new name for the store""")
@click.argument('dataset_path')
@click.argument('new_name')
def copy(dataset_path, new_name):
    dataset = Dataset.load(dataset_path)
    dataset.save(new_name)


def optional_args(names, args):
    kwargs = {}


@dataset.command(name='add-source', help="""Adds a source column to a dataset. A source column
selects comparable items along a dimension of the dataset to serve as
an input to pipelines and analyses.

Arguments
---------
dataset_path
    The path to the dataset including store and dataset name (where
    applicable), e.g. central-xnat//MYXNATPROJECT:pass_t1w_qc
name
    The name the source will be referenced by
format
    The data type of the column. Can be a field (int|float|str|bool),
    field array (list[int|float|str|bool]) or "file-group"
    (file, file+header/side-cars or directory)
""")
@click.argument('dataset_path')
@click.argument('name')
@click.argument('format')
@click.option(
    '--frequency', '-f', metavar='<dimension>',
    help=("The frequency that items appear in the dataset (e.g. per "
          "'session', 'subject', 'timepoint', 'group', 'dataset' for "
          "medimage:Clinical data dimensions"),
    show_default="highest")
@click.option(
    '--path', '-p',
    help=("Path to item in the dataset. If 'regex' option is provided it will "
          "be treated as a regular-expression (in Python syntax)"))
@click.option(
    '--order', type=int,
    help=("If multiple items match the remaining criteria within a session, "
          "select the <order>th of the matching items"))
@click.option(
    '--quality', '-q', default='usable',
    help=("For data stores that enable flagging of data quality, "
          "this option can filter out poor quality scans"))
@click.option(
    '--regex/--no-regex', 'is_regex', default=True,
    help=("Whether the 'path' option should be treated as a regular expression "
          "or not"))
@click.option(
    '--header', '-h', nargs=2, metavar='<key-val>',
    help=("Match on specific header value. This option is only valid for "
          "select formats that the implement the 'header_val()' method "
          "(e.g. medimage:dicom)."))
def add_source(dataset_path, name, format, frequency, path, order,
               quality, is_regex, header):
    dataset = Dataset.load(dataset_path)
    dataset.add_source(
        name=name,
        path=path,
        format=resolve_class(format, prefixes=['arcana.data.formats']),
        frequency=frequency,
        quality_threshold=quality,
        order=order,
        header_vals=dict(header),
        is_regex=is_regex)
    dataset.save()


@dataset.command(name='add-sink', help="""Adds a sink column to a dataset. A sink column
specifies how data should be writen into the dataset.

Arguments
---------
dataset_path
    The path to the dataset including store and dataset name (where
    applicable), e.g. central-xnat//MYXNATPROJECT:pass_t1w_qc
name
    The name the source will be referenced by
format
    The data type of the column. Can be a field (int|float|str|bool),
    field array (list[int|float|str|bool]) or "file-group"
    (file, file+header/side-cars or directory)
""")
@click.argument('dataset_path')
@click.argument('name')
@click.argument('format')
@click.option(
    '--frequency', '-f', metavar='<dimension>',
    help=("The frequency that items appear in the dataset (e.g. per "
          "'session', 'subject', 'timepoint', 'group', 'dataset' for "
          "medimage:Clinical data dimensions"),
    show_default="highest")
@click.option(
    '--path', '-p',
    help=("Path to item in the dataset. If 'regex' option is provided it will "
          "be treated as a regular-expression (in Python syntax)"))
@click.option(
    '--salience', '-s',
    help=("The salience of the column, i.e. whether it will show up on "
          "'arcana derive menu'"))
def add_sink(dataset_path, name, format, frequency, path, salience):
    dataset = Dataset.load(dataset_path)
    dataset.add_sink(
        name=name,
        path=path,
        format=resolve_class(format, prefixes=['arcana.data.formats']),
        frequency=frequency,
        salience=salience)
    dataset.save()


@dataset.command(
    name="missing-items",
    help="""Finds the IDs of nodes that are missing a valid entry for an item in
the column.

Arguments
---------
dataset_path
    The path to the dataset including store and dataset name (where
    applicable), e.g. central-xnat//MYXNATPROJECT:pass_t1w_qc
name
    The name of the column to check
""")
@click.argument('dataset_path')
@click.argument('name')
def missing_items(name):
    raise NotImplementedError

    