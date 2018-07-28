import click


@click.command()
def cli(*args):
    click.echo(repr(args))
