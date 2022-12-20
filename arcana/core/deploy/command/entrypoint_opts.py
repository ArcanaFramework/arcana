from __future__ import annotations
from pathlib import Path
import click
from click_option_group import optgroup
from arcana.core.deploy.image import App


def data_columns(func):

    return _apply_options(
        func,
        [
            optgroup.group(
                "Data columns",
                help=(
                    "Configure data columns to be added to the dataset before "
                    "the application and execution of the pipeline"
                ),
            ),
            optgroup.option(
                "--input",
                "-i",
                "input_values",
                nargs=2,
                default=(),
                metavar="<col-name> <match-criteria>",
                multiple=True,
                type=str,
                help=("The match criteria to pass to the column"),
            ),
            optgroup.option(
                "--output",
                "-o",
                "output_values",
                nargs=2,
                default=(),
                metavar="<col-name> <output-path>",
                multiple=True,
                type=str,
                help=("The path in which to store the output of the pipeline"),
            ),
        ],
    )


def parameterisation(func):
    return _apply_options(
        func,
        [
            optgroup.group(
                "Parameterisation",
                help=("Parameterisation of the pipeline and related options "),
            ),
            optgroup.option(
                "--parameter",
                "-p",
                "parameter_values",
                nargs=2,
                default=(),
                metavar="<name> <value>",
                multiple=True,
                type=str,
                help=("sets a parameter of the workflow"),
            ),
            optgroup.option(
                "--dataset-name",
                type=str,
                default=None,
                help=(
                    "The name of the dataset. Will be created if not present. Dataset names "
                    "are used to separate different analyses performed on the same data "
                    "into different namespaces"
                ),
            ),
            optgroup.option(
                "--overwrite/--no-overwrite",
                type=bool,
                help=(
                    "Whether to overwrite a saved pipeline with the same name, and its "
                    "parameterisation, if present"
                ),
            ),
        ],
    )


def execution(func):
    return _apply_options(
        func,
        [
            optgroup.group(
                "Execution", help=("Control over how the pipelines are executed")
            ),
            optgroup.option(
                "--work",
                "-w",
                "work_dir",
                default=None,
                help=(
                    "The location of the directory where the working files "
                    "created during the pipeline execution will be stored"
                ),
            ),
            optgroup.option(
                "--export-work",
                default=None,
                type=click.Path(exists=False, path_type=Path),
                help=(
                    "Export the work directory to another location after the task/workflow "
                    "exits (used for post-hoc investigation of completed workflows in "
                    "situations where the scratch space is inaccessible after the "
                    "workflow exits"
                ),
            ),
            optgroup.option(
                "--plugin",
                default="cf",
                help=("The Pydra plugin with which to process the task/workflow"),
            ),
            optgroup.option(
                "--loglevel",
                type=str,
                default="info",
                help=("The level of detail logging information is presented"),
            ),
            optgroup.option(
                "--ids",
                default=None,
                type=str,
                help=(
                    "List of IDs to restrict the pipeline execution to (i.e. don't execute "
                    "over the whole dataset)"
                ),
            ),
            optgroup.option(
                "--single-row",
                type=str,
                default=None,
                help=(
                    "Restrict the dataset created to a single row to avoid reduce start up "
                    "times and avoid ssues with unrelated rows that aren't being processed. "
                    "Comma-separated list of IDs for each layer of the hierarchy, e.g. "
                    "--single-row mysubject,mysession"
                ),
            ),
        ],
    )


def dataset_config(func):
    return _apply_options(
        func,
        [
            optgroup.group(
                "Dataset configuration",
                help=(
                    "Provide additional information to control how the dataset is created"
                ),
            ),
            optgroup.option(
                "--dataset-hierarchy",
                type=str,
                default=None,
                help=(
                    "Comma-separated hierarchy of the dataset "
                    "(see http://arcana.readthedocs.io/data_model.html"
                ),
            ),
        ],
    )


def debugging(func):

    return _apply_options(
        func,
        [
            optgroup.group(
                "Debugging options",
                help=(
                    "Options that can be used to help debug the deployment framework"
                ),
            ),
            optgroup.option(
                "--raise-errors/--catch-errors",
                type=bool,
                default=False,
                help="raise exceptions instead of capturing them to suppress call stack",
            ),
            optgroup.option(
                "--keep-running-on-errors/--exit-on-errors",
                type=bool,
                default=False,
                help=(
                    "Keep the the pipeline running in infinite loop on error (will need "
                    "to be manually killed). Can be useful in situations where the "
                    "enclosing container will be removed on completion and you need to "
                    "be able to 'exec' into the container to debug."
                ),
            ),
            optgroup.option(
                "--spec-path",
                type=click.Path(exists=True, path_type=Path),
                default=Path(App.SPEC_PATH),
                help=(
                    "Used to specify a different path to the spec path from the one that is written "
                    "to in the image (typically used in debugging/testing)"
                ),
            ),
        ],
    )


def _apply_options(func, options: list[click.Option]):

    for opt in reversed(options):
        func = opt(func)

    return func
