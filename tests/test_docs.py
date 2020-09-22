from sqlite_utils import cli
from pathlib import Path
import pytest
import re

docs_path = Path(__file__).parent.parent / "docs"
commands_re = re.compile(r"(?:\$ |    )sqlite-utils (\S+) ")


@pytest.fixture(scope="session")
def documented_commands():
    rst = (docs_path / "cli.rst").read_text()
    return {
        command
        for command in commands_re.findall(rst)
        if "." not in command and ":" not in command
    }


@pytest.mark.parametrize("command", cli.cli.commands.keys())
def test_commands_are_documented(documented_commands, command):
    assert command in documented_commands


@pytest.mark.parametrize("command", cli.cli.commands.values())
def test_commands_have_docstrings(command):
    assert command.__doc__, "{} is missing a docstring".format(command)
