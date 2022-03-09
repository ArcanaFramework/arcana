import click
from arcana.core.cli import cli
from arcana.core.enum import DataQuality


@cli.group()
def column():
    pass


@column.command(name='add-source', help="""Adds a source column to a dataset. A source column
selects comparable items along a dimension of the dataset to serve as
an input to pipelines and analyses.

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
""")
@click.argument('dataset_path')
@click.argument('column_name')
@click.argument('datatype')
@click.option(
    '--frequency', '-f', metavar='<dimension>',
    help=("The frequency that items appear in the dataset (e.g. per "
          "'session', 'subject', 'timepoint', 'group', 'dataset' for "
          "medicalimaging:ClinicalTrial data dimensions"),
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
          "select datatypes that the implement the 'header_val()' method "
          "(e.g. medicalimaging:dicom)."))
def add_source(dataset_path, column_name, datatype, frequency, path, order,
               quality, is_regex, header):
    raise NotImplementedError


@column.command(name='add-sink', help="""Adds a sink column to a dataset. A sink column
specifies how data should be writen into the dataset.

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
""")
@click.argument('dataset_path')
@click.argument('name')
@click.argument('datatype')
@click.option(
    '--frequency', '-f', metavar='<dimension>',
    help=("The frequency that items appear in the dataset (e.g. per "
          "'session', 'subject', 'timepoint', 'group', 'dataset' for "
          "medicalimaging:ClinicalTrial data dimensions"),
    show_default="highest")
@click.option(
    '--path', '-p',
    help=("Path to item in the dataset. If 'regex' option is provided it will "
          "be treated as a regular-expression (in Python syntax)"))
def add_sink(dataset_path, name, datatype, frequency, path):
    raise NotImplementedError


@column.command(help="""Removes an existing source column from a dataset

Arguments
---------
name
    The name of the source column
""")
def remove(name):
    # TODO: Should check to see if column has been used in workflows
    raise NotImplementedError


@column.command(help="""Finds the IDs of nodes that are missing a valid entry for the
column.

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
def missing(name):
    raise NotImplementedError

