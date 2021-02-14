from sqlite_utils import cli, Database
from click.testing import CliRunner
import os
import pathlib


def test_insert_files():
    runner = CliRunner()
    with runner.isolated_filesystem():
        tmpdir = pathlib.Path(".")
        db_path = str(tmpdir / "files.db")
        (tmpdir / "one.txt").write_text("This is file one", "utf-8")
        (tmpdir / "two.txt").write_text("Two is shorter", "utf-8")
        (tmpdir / "nested").mkdir()
        (tmpdir / "nested" / "three.txt").write_text("Three is nested", "utf-8")
        coltypes = (
            "name",
            "path",
            "fullpath",
            "sha256",
            "md5",
            "mode",
            "content",
            "mtime",
            "ctime",
            "mtime_int",
            "ctime_int",
            "mtime_iso",
            "ctime_iso",
            "size",
        )
        cols = []
        for coltype in coltypes:
            cols += ["-c", "{}:{}".format(coltype, coltype)]
        result = runner.invoke(
            cli.cli,
            ["insert-files", db_path, "files", str(tmpdir)] + cols + ["--pk", "path"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.stdout
        db = Database(db_path)
        rows_by_path = {r["path"]: r for r in db["files"].rows}
        one, two, three = (
            rows_by_path["one.txt"],
            rows_by_path["two.txt"],
            rows_by_path[os.path.join("nested", "three.txt")],
        )
        assert {
            "content": b"This is file one",
            "md5": "556dfb57fce9ca301f914e2273adf354",
            "name": "one.txt",
            "path": "one.txt",
            "sha256": "e34138f26b5f7368f298b4e736fea0aad87ddec69fbd04dc183b20f4d844bad5",
            "size": 16,
        }.items() <= one.items()
        assert {
            "content": b"Two is shorter",
            "md5": "f86f067b083af1911043eb215e74ac70",
            "name": "two.txt",
            "path": "two.txt",
            "sha256": "9368988ed16d4a2da0af9db9b686d385b942cb3ffd4e013f43aed2ec041183d9",
            "size": 14,
        }.items() <= two.items()
        assert {
            "content": b"Three is nested",
            "md5": "12580f341781f5a5b589164d3cd39523",
            "name": "three.txt",
            "path": os.path.join("nested", "three.txt"),
            "sha256": "6dd45aaaaa6b9f96af19363a92c8fca5d34791d3c35c44eb19468a6a862cc8cd",
            "size": 15,
        }.items() <= three.items()
        # Assert the other int/str/float columns exist and are of the right types
        expected_types = {
            "ctime": float,
            "ctime_int": int,
            "ctime_iso": str,
            "mtime": float,
            "mtime_int": int,
            "mtime_iso": str,
            "mode": int,
            "fullpath": str,
        }
        for colname, expected_type in expected_types.items():
            for row in (one, two, three):
                assert isinstance(row[colname], expected_type)


def test_insert_files_stdin():
    runner = CliRunner()
    with runner.isolated_filesystem():
        tmpdir = pathlib.Path(".")
        db_path = str(tmpdir / "files.db")
        result = runner.invoke(
            cli.cli,
            ["insert-files", db_path, "files", "-", "--name", "stdin-name"],
            catch_exceptions=False,
            input="hello world",
        )
        assert result.exit_code == 0, result.stdout
        db = Database(db_path)
        row = list(db["files"].rows)[0]
        assert {"path": "stdin-name", "content": b"hello world", "size": 11} == row
