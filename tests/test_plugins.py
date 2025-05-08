from click.testing import CliRunner
import click
import importlib
import pytest
from sqlite_utils import cli, Database, hookimpl, plugins


def _supports_pragma_function_list():
    db = Database(memory=True)
    try:
        db.execute("select * from pragma_function_list()")
    except Exception:
        return False
    return True


def test_register_commands():
    importlib.reload(cli)
    assert plugins.get_plugins() == []

    class HelloWorldPlugin:
        __name__ = "HelloWorldPlugin"

        @hookimpl
        def register_commands(self, cli):
            @cli.command(name="hello-world")
            def hello_world():
                "Print hello world"
                click.echo("Hello world!")

    try:
        plugins.pm.register(HelloWorldPlugin(), name="HelloWorldPlugin")
        importlib.reload(cli)

        assert plugins.get_plugins() == [
            {"name": "HelloWorldPlugin", "hooks": ["register_commands"]}
        ]

        runner = CliRunner()
        result = runner.invoke(cli.cli, ["hello-world"])
        assert result.exit_code == 0
        assert result.output == "Hello world!\n"

    finally:
        plugins.pm.unregister(name="HelloWorldPlugin")
        importlib.reload(cli)
        assert plugins.get_plugins() == []


@pytest.mark.skipif(
    not _supports_pragma_function_list(),
    reason="Needs SQLite version that supports pragma_function_list()",
)
def test_prepare_connection():
    importlib.reload(cli)
    assert plugins.get_plugins() == []

    class HelloFunctionPlugin:
        __name__ = "HelloFunctionPlugin"

        @hookimpl
        def prepare_connection(self, conn):
            conn.create_function("hello", 1, lambda name: f"Hello, {name}!")

    db = Database(memory=True)

    def _functions(db):
        return [
            row[0]
            for row in db.execute(
                "select distinct name from pragma_function_list() order by 1"
            ).fetchall()
        ]

    assert "hello" not in _functions(db)

    try:
        plugins.pm.register(HelloFunctionPlugin(), name="HelloFunctionPlugin")

        assert plugins.get_plugins() == [
            {"name": "HelloFunctionPlugin", "hooks": ["prepare_connection"]}
        ]

        db = Database(memory=True)
        assert "hello" in _functions(db)
        result = db.execute('select hello("world")').fetchone()[0]
        assert result == "Hello, world!"

        # Test execute_plugins=False
        db2 = Database(memory=True, execute_plugins=False)
        assert "hello" not in _functions(db2)

    finally:
        plugins.pm.unregister(name="HelloFunctionPlugin")
        assert plugins.get_plugins() == []
