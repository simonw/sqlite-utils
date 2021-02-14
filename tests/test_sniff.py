from sqlite_utils import cli, Database
from click.testing import CliRunner
import pathlib
import pytest

sniff_dir = pathlib.Path(__file__).parent / "sniff"


@pytest.mark.parametrize("filepath", sniff_dir.glob("example*"))
def test_sniff(tmpdir, filepath):
    db_path = str(tmpdir / "test.db")
    runner = CliRunner()
    result = runner.invoke(
        cli.cli,
        ["insert", db_path, "creatures", str(filepath), "--sniff"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.stdout
    db = Database(db_path)
    assert list(db["creatures"].rows) == [
        {"id": "1", "species": "dog", "name": "Cleo", "age": "5"},
        {"id": "2", "species": "dog", "name": "Pancakes", "age": "4"},
        {"id": "3", "species": "cat", "name": "Mozie", "age": "8"},
        {"id": "4", "species": "spider", "name": "Daisy, the tarantula", "age": "6"},
    ]
