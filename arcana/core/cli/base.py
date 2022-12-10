import click
from arcana import __version__


# Define the base CLI entrypoint
@click.group()
@click.version_option(version=__version__)
def cli():
    pass
