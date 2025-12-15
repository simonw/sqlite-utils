import sqlite3

import click
from pluggy import HookimplMarker
from pluggy import HookspecMarker

hookspec = HookspecMarker("sqlite_utils")
hookimpl = HookimplMarker("sqlite_utils")


@hookspec
def register_commands(cli: click.Group) -> None:
    """Register additional CLI commands, e.g. 'sqlite-utils mycommand ...'"""


@hookspec
def prepare_connection(conn: sqlite3.Connection) -> None:
    """Modify SQLite connection in some way e.g. register custom SQL functions"""
