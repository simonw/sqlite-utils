from pluggy import HookimplMarker
from pluggy import HookspecMarker

hookspec = HookspecMarker("sqlite_utils")
hookimpl = HookimplMarker("sqlite_utils")


@hookspec
def register_commands(cli):
    """Register additional CLI commands, e.g. 'sqlite-utils mycommand ...'"""


@hookspec
def prepare_connection(conn):
    """Modify SQLite connection in some way e.g. register custom SQL functions"""
