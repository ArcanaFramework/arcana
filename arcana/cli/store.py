import click
from arcana.core.utils import resolve_class
from arcana.core.data.store import DataStore
from arcana.core.cli import cli
from arcana.core.utils import get_home_dir


@cli.group()
def store():
    pass


@store.command("""Saves the details for a new data store in the configuration
file ('~/.arcana/stores.yml').

Arguments
---------
nickname
    The name given to the store for reference in other commands
type
    The storage class and the module it is defined in, e.g.
    `arcana.data.store.xnat:Xnat`
location
    The location of the store, e.g. server address
""")
@click.argument('nickname')
@click.argument('type')
@click.argument('location')
@click.argument('varargs', nargs=-1)
@click.option(
    '--cache', '-c', default=None,
    help="The location of a cache dir to download local copies of remote data")
def add(nickname, type, location, varargs, cache):
    if cache is None:
        cache = get_home_dir() / 'cache' / nickname
    store_cls = resolve_class(type)
    store = store_cls(location, *varargs, cache_dir=cache)
    DataStore.save(nickname, store)


@store.command(help="""
Gives a data store saved in the config file ('~/.arcana/stores.yml') a new
nickname

Arguments
---------
old_nickname
    The current name of the store
new_nickname
    The new name for the store""")
@click.argument('old_nickname')
@click.argument('new_nickname')
def rename(old_nickname, new_nickname):
    DataStore.save(new_nickname,  DataStore.load(old_nickname))


@store.command("""Remove a saved data store from the config file

Arguments
---------
nickname
    The nickname the store was given when its details were saved""")
def remove(nickname):
    DataStore.remove(nickname)
