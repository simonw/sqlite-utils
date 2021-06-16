from sqlite_utils import cli, Database
from click.testing import CliRunner
import pytest


def test_memory_basic():
    result = CliRunner().invoke(cli.cli, ["memory", "select 1 + 1"])
    assert result.output.strip() == '[{"1 + 1": 2}]'


@pytest.mark.parametrize("sql_from", ("test", "t", "t1"))
@pytest.mark.parametrize("use_stdin", (True, False))
def test_memory_csv(tmpdir, sql_from, use_stdin):
    content = "id,name\n1,Cleo\n2,Bants"
    input = None
    if use_stdin:
        input = content
        csv_path = "-"
        if sql_from == "test":
            sql_from = "stdin"
    else:
        csv_path = str(tmpdir / "test.csv")
        open(csv_path, "w").write(content)
    result = CliRunner().invoke(
        cli.cli,
        ["memory", csv_path, "select * from {}".format(sql_from), "--nl"],
        input=input,
    )
    assert (
        result.output.strip()
        == '{"id": "1", "name": "Cleo"}\n{"id": "2", "name": "Bants"}'
    )
