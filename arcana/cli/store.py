import click
from arcana.core.utils import resolve_class
from arcana.core.data.store import DataStore
from arcana.core.cli import cli


@cli.group()
def store():
    pass


@store.command("""Adds a new store to the Arcana configuration
    Usage
    ----
    name
        The name given to the store for reference in other commands
    type
        The storage class and the module it is defined in, e.g.
        `arcana.data.store.xnat:Xnat`
    location
        The location of the store, e.g. server address
""")
@click.argument('name')
@click.argument('type')
@click.argument('address')
def add(name, type, address):
    store_cls = resolve_class(type)
    store = store_cls(address)
    DataStore.save(name, store)


@store.command(help="""
Renames a data store saved in the stores.yml to a new name

old_name
    The current name of the store
new_name
    The new name for the store""")
@click.argument('old_name')
@click.argument('new_name')
def rename(old_name, new_name):
    DataStore.save(new_name,  DataStore.load(old_name))


@store.command()
def remove(name):
    DataStore.remove(name)
