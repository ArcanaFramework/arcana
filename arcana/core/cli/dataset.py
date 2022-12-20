from __future__ import annotations
import click
from .base import cli
from arcana.core.data.set import Dataset
from arcana.core.data.store import DataStore
from arcana.core.data.space import DataSpace
from arcana.core.data.type.base import DataType
from arcana.core.utils.serialize import ClassResolver


@cli.group()
def dataset():
    pass


@dataset.command(
    name="define",
    help=(
        """Define the tree structure and IDs to include in a
dataset. Where possible, the definition file is saved inside the dataset for
use by multiple users, if not possible it is stored in the ~/.arcana directory.

ID_STR string containing the nick-name of the store, the ID of the dataset
(e.g. XNAT project ID or file-system directory) and the dataset's name in the
format <store-nickname>//<dataset-id>[@<dataset-name>]

HIERARCHY the data frequencies that are present in the data tree. For some
store types this is fixed (e.g. XNAT-> subject > session) but for more flexible
(e.g. FileSystem), which dimension each layer of sub-directories corresponds to
can be arbitrarily specified. dimensions"""
    ),
)
@click.argument("id_str")
@click.argument("hierarchy", nargs=-1)
@click.option(
    "--space",
    type=str,
    default=None,
    help=(
        'The "space" of the dataset, defines the dimensions along the ids '
        "of row can vary"
    ),
)
@click.option(
    "--include",
    nargs=2,
    default=(),
    metavar="<freq-id>",
    multiple=True,
    type=str,
    help=(
        "The rows to include in the dataset. First value is the "
        "row-frequency of the ID (e.g. 'group', 'subject', 'session') "
        "followed by the IDs to be included in the dataset. "
        "If the second arg contains '/' then it is interpreted as "
        "the path to a text file containing a list of IDs"
    ),
)
@click.option(
    "--exclude",
    nargs=2,
    default=(),
    metavar="<freq-id>",
    multiple=True,
    type=str,
    help=(
        "The rows to exclude from the dataset. First value is the "
        "row-frequency of the ID (e.g. 'group', 'subject', 'session') "
        "followed by the IDs to be included in the dataset. "
        "If the second arg contains '/' then it is interpreted as "
        "the path to a text file containing a list of IDs"
    ),
)
@click.option(
    "--space",
    default="medimage:Clinical",
    help=(
        "The enum that specifies the data dimensions of the dataset. "
        "Defaults to `Clinical`, which "
        "consists of the typical dataset>group>subject>session "
        "data tree used in medimage trials/studies"
    ),
)
@click.option(
    "--id-inference",
    nargs=2,
    metavar="<source-regex>",
    multiple=True,
    help="""Specifies how IDs of row frequencies that not explicitly
provided are inferred from the IDs that are. For example, given a set
of subject IDs that are a combination of the ID of the group that they belong
to + their member IDs (i.e. matched test/controls have same member ID), e.g.

CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

the group ID can be extracted by providing the ID to source it from
(i.e. subject) and a regular expression (in Python regex syntax:
https://docs.python.org/3/library/re.html) with a named
groups corresponding to the inferred IDs

--id-inference subject '(?P<group>[A-Z]+)(?P<member>[0-9]+)'

""",
)
def define(id_str, hierarchy, include, exclude, space, id_inference):

    store_name, id, name = Dataset.parse_id_str(id_str)

    if not hierarchy:
        hierarchy = None

    store = DataStore.load(store_name)

    if space:
        space = ClassResolver(DataSpace)(space)

    dataset = store.new_dataset(
        id,
        hierarchy=hierarchy,
        space=space,
        id_inference=id_inference,
        include=include,
        exclude=exclude,
    )

    dataset.save(name)


@dataset.command(
    help="""
Renames a data store saved in the stores.yaml to a new name

dataset_path
    The current name of the store
new_name
    The new name for the store"""
)
@click.argument("dataset_path")
@click.argument("new_name")
def copy(dataset_path, new_name):
    dataset = Dataset.load(dataset_path)
    dataset.save(new_name)


# def optional_args(names, args):
#     kwargs = {}


@dataset.command(
    name="add-source",
    help="""Adds a source column to a dataset. A source column
selects comparable items along a dimension of the dataset to serve as
an input to pipelines and analyses.

DATASET_PATH: The path to the dataset including store and dataset name
(where applicable), e.g. central-xnat//MYXNATPROJECT:pass_t1w_qc

NAME: The name the source will be referenced by

FORMAT: The data type of the column. Can be a field (int|float|str|bool),
field array (list[int|float|str|bool]) or
"file-group" (file, file+header/side-cars or directory)
""",
)
@click.argument("dataset_path")
@click.argument("name")
@click.argument("datatype")
@click.option(
    "--row-frequency",
    "-f",
    metavar="<dimension>",
    help=(
        "The row-frequency that items appear in the dataset (e.g. per "
        "'session', 'subject', 'timepoint', 'group', 'dataset' for "
        "medimage:Clinical data dimensions"
    ),
    show_default="highest",
)
@click.option(
    "--path",
    "-p",
    help=(
        "Path to item in the dataset. If 'regex' option is provided it will "
        "be treated as a regular-expression (in Python syntax)"
    ),
)
@click.option(
    "--order",
    type=int,
    help=(
        "If multiple items match the remaining criteria within a session, "
        "select the <order>th of the matching items"
    ),
)
@click.option(
    "--quality",
    "-q",
    default="usable",
    help=(
        "For data stores that enable flagging of data quality, "
        "this option can filter out poor quality scans"
    ),
)
@click.option(
    "--regex/--no-regex",
    "is_regex",
    default=True,
    help=(
        "Whether the 'path' option should be treated as a regular expression " "or not"
    ),
)
@click.option(
    "--header",
    "-h",
    nargs=2,
    metavar="<key-val>",
    multiple=True,
    help=(
        "Match on specific header value. This option is only valid for "
        "select formats that the implement the 'header_val()' method "
        "(e.g. medimage:dicom)."
    ),
)
def add_source(
    dataset_path, name, datatype, row_frequency, path, order, quality, is_regex, header
):
    dataset = Dataset.load(dataset_path)
    dataset.add_source(
        name=name,
        path=path,
        datatype=ClassResolver(DataType)(datatype),
        row_frequency=row_frequency,
        quality_threshold=quality,
        order=order,
        header_vals=dict(header),
        is_regex=is_regex,
    )
    dataset.save()


@dataset.command(
    name="add-sink",
    help="""Adds a sink column to a dataset. A sink column
specifies how data should be written into the dataset.

Arguments
---------
dataset_path
    The path to the dataset including store and dataset name (where
    applicable), e.g. central-xnat//MYXNATPROJECT:pass_t1w_qc
name
    The name the source will be referenced by
datatype
    The data type of the column. Can be a field (int|float|str|bool),
    field array (list[int|float|str|bool]) or "file-group"
    (file, file+header/side-cars or directory)
""",
)
@click.argument("dataset_path")
@click.argument("name")
@click.argument("datatype")
@click.option(
    "--row-frequency",
    "-f",
    metavar="<dimension>",
    help=(
        "The row-frequency that items appear in the dataset (e.g. per "
        "'session', 'subject', 'timepoint', 'group', 'dataset' for "
        "medimage:Clinical data dimensions"
    ),
    show_default="highest",
)
@click.option(
    "--path",
    "-p",
    help=(
        "Path to item in the dataset. If 'regex' option is provided it will "
        "be treated as a regular-expression (in Python syntax)"
    ),
)
@click.option(
    "--salience",
    "-s",
    help=(
        "The salience of the column, i.e. whether it will show up on "
        "'arcana derive menu'"
    ),
)
def add_sink(dataset_path, name, datatype, row_frequency, path, salience):
    dataset = Dataset.load(dataset_path)
    dataset.add_sink(
        name=name,
        path=path,
        datatype=ClassResolver(DataType)(datatype),
        row_frequency=row_frequency,
        salience=salience,
    )
    dataset.save()


@dataset.command(
    name="missing-items",
    help="""Finds the IDs of rows that are missing a valid entry for an item in
the column.

Arguments
---------
dataset_path
    The path to the dataset including store and dataset name (where
    applicable), e.g. central-xnat//MYXNATPROJECT:pass_t1w_qc
name
    The name of the column to check
""",
)
@click.argument("dataset_path")
@click.argument("name")
def missing_items(name):
    raise NotImplementedError
