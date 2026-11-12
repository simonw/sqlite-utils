import click
import sqlite_utils


@sqlite_utils.hookimpl
def register_commands(cli):
    @cli.command()
    def standalone_plugin_test():
        click.echo("Bundled plugin works")
