from click.testing import CliRunner
from sqlite_utils import cli, recipes
from pathlib import Path
import pytest
import re

docs_path = Path(__file__).parent.parent / "docs"
commands_re = re.compile(r"(?:\$ |    )sqlite-utils (\S+)")
recipes_re = re.compile(r"r\.(\w+)\(")


@pytest.fixture(scope="session")
def documented_commands():
    rst = ""
    for doc in ("cli.rst", "plugins.rst"):
        rst += (docs_path / doc).read_text()
    return {
        command
        for command in commands_re.findall(rst)
        if "." not in command and ":" not in command
    }


@pytest.fixture(scope="session")
def documented_recipes():
    rst = (docs_path / "cli.rst").read_text()
    return set(recipes_re.findall(rst))


@pytest.mark.parametrize("command", cli.cli.commands.keys())
def test_commands_are_documented(documented_commands, command):
    assert command in documented_commands


@pytest.mark.parametrize("command", cli.cli.commands.values())
def test_commands_have_help(command):
    assert command.help, "{} is missing its help".format(command)


def test_convert_help():
    result = CliRunner().invoke(cli.cli, ["convert", "--help"])
    assert result.exit_code == 0
    for expected in (
        "r.jsonsplit(value, ",
        "r.parsedate(value, ",
        "r.parsedatetime(value, ",
    ):
        assert expected in result.output


@pytest.mark.parametrize(
    "recipe",
    [
        n
        for n in dir(recipes)
        if not n.startswith("_")
        and n not in ("json", "parser")
        and callable(getattr(recipes, n))
    ],
)
def test_recipes_are_documented(documented_recipes, recipe):
    assert recipe in documented_recipes
