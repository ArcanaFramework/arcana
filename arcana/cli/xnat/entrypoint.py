import click
from arcana.core.deploy.command import entrypoint_opts
from arcana.deploy.xnat import XnatCSImage
from .base import xnat_group


@xnat_group.command(
    name="cs-entrypoint",
    help="""Loads/creates a dataset, then applies and launches a pipeline
in a single command. To be used within the command configuration of an XNAT
Container Service ready Docker image.

DATASET_ID_STR string containing the nickname of the data store, the ID of the
dataset (e.g. XNAT project ID or file-system directory) and the dataset's name
in the format <STORE-NICKNAME>//<DATASET-ID>:<DATASET-NAME>

""",
)
@click.argument("dataset_id_str")
@entrypoint_opts.data_columns
@entrypoint_opts.parameterisation
@entrypoint_opts.execution
@entrypoint_opts.debugging
@entrypoint_opts.dataset_config
def cs_image_entrypoint(
    dataset_id_str,
    spec_path,
    **kwargs,
):

    image_spec = XnatCSImage.load(spec_path=spec_path)

    image_spec.command.run(
        dataset_id_str,
        **kwargs,
    )
