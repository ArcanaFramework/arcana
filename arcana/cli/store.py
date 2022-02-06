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
    store = store_cls.factory(name, address)
    store.save()


@store.command()
def rename(old_name, new_name):
    store = DataStore.load(old_name)
    DataStore.remove(store)
    store.name = new_name
    store.save()


@store.command()
def remove(name):
    pass