from operator import itemgetter
import click
from arcana.core.data.store import DataStore
from arcana.core.utils.serialize import ClassResolver
from arcana.core.utils.misc import get_home_dir
from .base import cli


@cli.group()
def store():
    pass


@store.command(
    help="""Saves the details for a new data store in the configuration
file ('~/.arcana/stores.yaml').

Arguments
---------
nickname
    The name given to the store for reference in other commands
type
    The storage class and the module it is defined in, e.g.
    `arcana.data.store.xnat:Xnat`
location
    The location of the store, e.g. server address
*varargs
    Parameters that are specific to the 'type' of storage class to be added
"""
)
@click.argument("nickname")
@click.argument("type")
@click.argument("location")
@click.argument("varargs", nargs=-1)
@click.option(
    "--user", "-u", default=None, help="The username to use to connect to the store"
)
@click.option(
    "--password",
    "-p",
    prompt=True,
    hide_input=True,
    help="The password to use to connect to the store",
)
@click.option(
    "--cache",
    "-c",
    default=None,
    help="The location of a cache dir to download local copies of remote data",
)
def add(nickname, type, location, varargs, cache, user, password):
    if cache is None:
        cache = get_home_dir() / "cache" / nickname
        cache.mkdir(parents=True, exist_ok=True)
    store_cls = ClassResolver(DataStore)(type)
    store = store_cls(location, *varargs, cache_dir=cache, user=user, password=password)
    store.save(nickname)


@store.command(
    help="""
Gives a data store saved in the config file ('~/.arcana/stores.yaml') a new
nickname.

Arguments

OLD_NICKNAME The current name of the store.
NEW_NICKNAME The new name for the store."""
)
@click.argument("old_nickname")
@click.argument("new_nickname")
def rename(old_nickname, new_nickname):
    DataStore.load(old_nickname).save(new_nickname)
    DataStore.remove(old_nickname)


@store.command(
    help="""Remove a saved data store from the config file

nickname
    The nickname the store was given when its details were saved"""
)
@click.argument("nickname")
def remove(nickname):
    DataStore.remove(nickname)


@store.command(
    help="""Refreshes credentials saved for the given store
(typically a token that expires)

nickname
    Nickname given to the store to refresh the credentials of"""
)
@click.argument("nickanme")
@click.option(
    "--user", "-u", default=None, help="The username to use to connect to the store"
)
@click.option(
    "--password",
    "-p",
    prompt=True,
    hide_input=True,
    help="The password to use to connect to the store",
)
def refresh(nickname, user, password):
    store = DataStore.load(nickname)
    if user is not None:
        store.user = user
    store.password = password
    store.save()
    DataStore.remove(nickname)
    store.save(nickname)


@store.command(help="""List available stores that have been saved""")
def ls():
    click.echo("Built-in stores\n---------------")
    for name, store in sorted(DataStore.singletons().items(), key=itemgetter(0)):
        click.echo(f"{name} - {ClassResolver.tostr(store, strip_prefix=False)}")
    click.echo("\nSaved stores\n-------------")
    for name, entry in DataStore.load_saved_entries().items():
        store_class = entry.pop("class")
        click.echo(f"{name} - {store_class[1:-1]}")
        for key, val in sorted(entry.items(), key=itemgetter(0)):
            click.echo(f"    {key}: {val}")
