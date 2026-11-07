import os
import pathlib
import sys
import zlib
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

from sqlite_utils import Database, cli


@pytest.mark.parametrize("silent", (False, True))
@pytest.mark.parametrize(
    "pk_args,expected_pks",
    (
        (["--pk", "path"], ["path"]),
        (["--pk", "path", "--pk", "name"], ["path", "name"]),
    ),
)
def test_insert_files(silent, pk_args, expected_pks):
    runner = CliRunner()
    with runner.isolated_filesystem():
        tmpdir = pathlib.Path(".")
        db_path = str(tmpdir / "files.db")
        (tmpdir / "one.txt").write_text("This is file one", "utf-8")
        (tmpdir / "two.txt").write_text("Two is shorter", "utf-8")
        (tmpdir / "nested").mkdir()
        (tmpdir / "nested" / "three.zz.txt").write_text("Three is nested", "utf-8")
        coltypes = (
            "name",
            "path",
            "fullpath",
            "sha256",
            "md5",
            "mode",
            "content",
            "content_text",
            "mtime",
            "ctime",
            "mtime_int",
            "ctime_int",
            "mtime_iso",
            "ctime_iso",
            "size",
            "suffix",
            "stem",
        )
        cols = []
        for coltype in coltypes:
            cols += ["-c", f"{coltype}:{coltype}"]
        result = runner.invoke(
            cli.cli,
            ["insert-files", db_path, "files", str(tmpdir)]
            + cols
            + pk_args
            + (["--silent"] if silent else []),
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.stdout
        db = Database(db_path)
        rows_by_path = {r["path"]: r for r in db.table("files").rows}
        one, two, three = (
            rows_by_path["one.txt"],
            rows_by_path["two.txt"],
            rows_by_path[os.path.join("nested", "three.zz.txt")],
        )
        assert {
            "content": b"This is file one",
            "content_text": "This is file one",
            "md5": "556dfb57fce9ca301f914e2273adf354",
            "name": "one.txt",
            "path": "one.txt",
            "sha256": "e34138f26b5f7368f298b4e736fea0aad87ddec69fbd04dc183b20f4d844bad5",
            "size": 16,
            "stem": "one",
            "suffix": ".txt",
        }.items() <= one.items()
        assert {
            "content": b"Two is shorter",
            "content_text": "Two is shorter",
            "md5": "f86f067b083af1911043eb215e74ac70",
            "name": "two.txt",
            "path": "two.txt",
            "sha256": "9368988ed16d4a2da0af9db9b686d385b942cb3ffd4e013f43aed2ec041183d9",
            "size": 14,
            "stem": "two",
            "suffix": ".txt",
        }.items() <= two.items()
        assert {
            "content": b"Three is nested",
            "content_text": "Three is nested",
            "md5": "12580f341781f5a5b589164d3cd39523",
            "name": "three.zz.txt",
            "path": os.path.join("nested", "three.zz.txt"),
            "sha256": "6dd45aaaaa6b9f96af19363a92c8fca5d34791d3c35c44eb19468a6a862cc8cd",
            "size": 15,
            "stem": "three.zz",
            "suffix": ".txt",
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
            "content": bytes,
            "content_text": str,
            "stem": str,
            "suffix": str,
        }
        for colname, expected_type in expected_types.items():
            for row in (one, two, three):
                assert isinstance(row[colname], expected_type)
        assert set(db.table("files").pks) == set(expected_pks)


@pytest.mark.parametrize(
    "use_text,encoding,input,expected",
    (
        (False, None, "hello world", b"hello world"),
        (True, None, "hello world", "hello world"),
        (False, None, b"S\xe3o Paulo", b"S\xe3o Paulo"),
        (True, "latin-1", b"S\xe3o Paulo", "S\xe3o Paulo"),
    ),
)
def test_insert_files_stdin(use_text, encoding, input, expected):
    runner = CliRunner()
    with runner.isolated_filesystem():
        tmpdir = pathlib.Path(".")
        db_path = str(tmpdir / "files.db")
        args = ["insert-files", db_path, "files", "-", "--name", "stdin-name"]
        if use_text:
            args += ["--text"]
        if encoding is not None:
            args += ["--encoding", encoding]
        result = runner.invoke(
            cli.cli,
            args,
            catch_exceptions=False,
            input=input,
        )
        assert result.exit_code == 0, result.stdout
        db = Database(db_path)
        row = next(iter(db.table("files").rows))
        key = "content"
        if use_text:
            key = "content_text"
        assert {"path": "stdin-name", key: expected}.items() <= row.items()


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="Windows has a different way of handling default encodings",
)
def test_insert_files_bad_text_encoding_error():
    runner = CliRunner()
    with runner.isolated_filesystem():
        tmpdir = pathlib.Path(".")
        latin = tmpdir / "latin.txt"
        latin.write_bytes(b"S\xe3o Paulo")
        db_path = str(tmpdir / "files.db")
        result = runner.invoke(
            cli.cli,
            ["insert-files", db_path, "files", str(latin), "--text"],
            catch_exceptions=False,
        )
        assert result.exit_code == 1, result.output
        assert result.output.strip().startswith(
            f"Error: Could not read file '{latin.resolve()!s}' as text"
        )


@pytest.mark.parametrize("stdin", [False, True], ids=["file", "stdin"])
@pytest.mark.parametrize("content", [b"", b"\x00\xff" * 1000], ids=["empty", "binary"])
def test_insert_files_convert_compress(tmp_path, stdin, content):
    source = tmp_path / "input.bin"
    source.write_bytes(content)
    db_path = str(tmp_path / "files.db")
    result = CliRunner().invoke(
        cli.cli,
        [
            "insert-files",
            db_path,
            "files",
            "-" if stdin else str(source),
            "--name",
            "renamed.bin",
            "-c",
            "name:name",
            "-c",
            "data:content",
            "-c",
            "sz:size",
            "--pk",
            "name",
            "--import",
            "zlib",
            "--convert",
            'row["data"] = zlib.compress(row["data"])',
        ],
        input=content if stdin else None,
    )
    assert result.exit_code == 0, result.output
    row = next(Database(db_path).table("files").rows)
    assert row["name"] == "renamed.bin"
    assert row["sz"] == len(content)
    assert zlib.decompress(row["data"]) == content


@pytest.mark.parametrize(
    "code",
    [
        '{**row, "content_text": row["content_text"].upper()}',
        'row["content_text"] = row["content_text"].upper()',
        'def convert(row):\n    row["content_text"] = row["content_text"].upper()\n    return row',
    ],
)
def test_insert_files_convert_text(tmp_path, code):
    source = tmp_path / "text.txt"
    source.write_bytes(b"S\xe3o Paulo")
    db_path = str(tmp_path / "files.db")
    result = CliRunner().invoke(
        cli.cli,
        [
            "insert-files",
            db_path,
            "files",
            str(source),
            "--text",
            "--encoding",
            "latin-1",
            "--convert",
            code,
        ],
    )
    assert result.exit_code == 0, result.output
    row = next(Database(db_path).table("files").rows)
    assert row["content_text"] == "SÃO PAULO"
    assert row["size"] == 9


def test_insert_files_convert_multiple_imports(tmp_path):
    result = CliRunner().invoke(
        cli.cli,
        [
            "insert-files",
            str(tmp_path / "files.db"),
            "files",
            "-",
            "--import",
            "hashlib",
            "--import",
            "json",
            "--convert",
            '{"digest": hashlib.sha256(row["content"]).hexdigest(), "size": json.dumps(row["size"])}',
            "--pk",
            "digest",
        ],
        input=b"hello",
    )
    assert result.exit_code == 0, result.output
    row = next(Database(tmp_path / "files.db").table("files").rows)
    assert row == {
        "digest": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        "size": "5",
    }


@pytest.mark.parametrize("mode", ["--replace", "--upsert"])
def test_insert_files_convert_existing_rows(tmp_path, mode):
    db_path = str(tmp_path / "files.db")
    db = Database(db_path)
    db.table("files").insert({"path": "input", "content": b"old"}, pk="path")
    result = CliRunner().invoke(
        cli.cli,
        [
            "insert-files",
            db_path,
            "files",
            "-",
            "--name",
            "input",
            "--alter",
            mode,
            "--convert",
            'row["content"] = row["content"].upper()',
        ],
        input=b"new",
    )
    assert result.exit_code == 0, result.output
    assert list(db.table("files").rows) == [
        {"path": "input", "content": b"NEW", "size": 3}
    ]


def test_insert_files_convert_across_batches(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    for i in range(105):
        (source / f"{i}.txt").write_bytes(b"hello")
    converter = Mock(side_effect=lambda row: {**row, "content": row["content"].upper()})
    compiler = Mock(return_value=converter)
    monkeypatch.setattr(cli, "_compile_code", compiler)
    db_path = str(tmp_path / "files.db")
    result = CliRunner().invoke(
        cli.cli,
        ["insert-files", db_path, "files", str(source), "--convert", "conversion"],
    )
    assert result.exit_code == 0, result.output
    compiler.assert_called_once_with("conversion", (), variable="row")
    assert converter.call_count == 105
    rows = list(Database(db_path).table("files").rows)
    assert len(rows) == 105
    assert all(row["content"] == b"HELLO" for row in rows)
    assert {row["path"] for row in rows} == {
        str(source / f"{i}.txt") for i in range(105)
    }


@pytest.mark.parametrize("fail_at", [2, 102], ids=["first-batch", "second-batch"])
def test_insert_files_convert_failure_during_import(tmp_path, fail_at):
    sources = [tmp_path / f"{i}.txt" for i in range(105)]
    for source in sources:
        source.write_bytes(b"hello")
    db_path = str(tmp_path / "files.db")
    db = Database(db_path)
    db.table("files").insert(
        {"path": "existing", "content": b"keep", "size": 4}, pk="path"
    )
    code = (
        f'if row["path"] == {str(sources[fail_at])!r}:\n'
        '    raise ValueError("broken conversion")\n'
        'row["content"] = row["content"].upper()'
    )
    result = CliRunner().invoke(
        cli.cli,
        ["insert-files", db_path, "files", *map(str, sources), "--convert", code],
    )
    assert result.exit_code == 1, result.output
    assert (
        f"Error converting file '{sources[fail_at]}': broken conversion"
        in result.output
    )
    assert "Traceback" not in result.output
    rows = list(db.table("files").rows)
    assert rows[0] == {"path": "existing", "content": b"keep", "size": 4}
    # Completed batches remain, but the failing batch is never inserted.
    expected_sources = sources[:100] if fail_at >= 100 else []
    assert {row["path"] for row in rows[1:]} == set(map(str, expected_sources))
    assert all(row["content"] == b"HELLO" for row in rows[1:])


@pytest.mark.parametrize(
    "code,message",
    [
        ("{}", "--convert produced an empty row"),
        ("row.clear()", "--convert produced an empty row"),
        ("{1: 'hello'}", "--convert produced a row with non-string column names"),
        ("row[1] = 'hello'", "--convert produced a row with non-string column names"),
    ],
    ids=["empty-return", "empty-in-place", "key-return", "key-in-place"],
)
def test_insert_files_convert_invalid_row(tmp_path, code, message):
    result = CliRunner().invoke(
        cli.cli,
        [
            "insert-files",
            str(tmp_path / "files.db"),
            "files",
            "-",
            "--convert",
            code,
        ],
        input=b"hello",
    )
    assert result.exit_code == 1, result.output
    assert f"Error converting file '-': {message}" in result.output
    assert "Traceback" not in result.output


@pytest.mark.parametrize(
    "code,imports,message",
    [
        ("return (", [], "Could not compile code"),
        (
            "row",
            ["--import", "nonexistent_sqlite_utils_test_module"],
            "No module named",
        ),
        ('row["missing"]', [], "Error converting file '-'"),
        ('raise ValueError("broken conversion")', [], "broken conversion"),
        ("42", [], "--convert must return a dict or None"),
        ("[]", [], "--convert must return a dict or None"),
    ],
)
def test_insert_files_convert_error(tmp_path, code, imports, message):
    result = CliRunner().invoke(
        cli.cli,
        [
            "insert-files",
            str(tmp_path / "files.db"),
            "files",
            "-",
            "--convert",
            code,
            *imports,
        ],
        input=b"hello",
    )
    assert result.exit_code == 1, result.output
    assert message in result.output
    assert "Traceback" not in result.output


@pytest.mark.parametrize(
    "stdin,code",
    [
        (True, '{"content": row["content"], "size": row["size"]}'),
        (False, 'del row["path"]'),
    ],
    ids=["replacement-stdin", "in-place-file"],
)
def test_insert_files_convert_missing_default_pk(tmp_path, stdin, code):
    source = tmp_path / "first.txt"
    source.write_bytes(b"hello")
    file_arg = "-" if stdin else str(source)
    db_path = str(tmp_path / "files.db")
    runner = CliRunner()
    result = runner.invoke(
        cli.cli,
        [
            "insert-files",
            db_path,
            "files",
            file_arg,
            "--name",
            "first.txt",
            "--convert",
            code,
        ],
        input=b"hello" if stdin else None,
    )
    assert result.exit_code == 1, result.output
    assert (
        f"Error converting file '{file_arg}': "
        "--convert produced a row missing primary key column 'path'"
    ) in result.output
    assert "Include this column in the converted row" in result.output
    assert "Traceback" not in result.output
    with Database(db_path) as db:
        assert not db.table("files").exists()

    # A rejected first row must not leave an integer path column behind.
    result = runner.invoke(
        cli.cli,
        ["insert-files", db_path, "files", "-", "--name", "second.txt"],
        input=b"world",
    )
    assert result.exit_code == 0, result.output
    with Database(db_path) as db:
        assert list(db.table("files").rows) == [
            {"path": "second.txt", "content": b"world", "size": 5}
        ]
        assert db.table("files").columns_dict["path"] is str


@pytest.mark.parametrize(
    "pks,code,missing",
    [
        (["name"], '{"content": row["content"], "size": row["size"]}', "name"),
        (["name", "size"], '{"name": row["name"], "content": row["content"]}', "size"),
    ],
    ids=["custom", "composite"],
)
def test_insert_files_convert_missing_selected_pk(tmp_path, pks, code, missing):
    db_path = str(tmp_path / "files.db")
    pk_args = [arg for pk in pks for arg in ("--pk", pk)]
    result = CliRunner().invoke(
        cli.cli,
        [
            "insert-files",
            db_path,
            "files",
            "-",
            "-c",
            "name:name",
            "-c",
            "content:content",
            "-c",
            "size:size",
            *pk_args,
            "--convert",
            code,
        ],
        input=b"hello",
    )
    assert result.exit_code == 1, result.output
    assert f"missing primary key column '{missing}'" in result.output
    assert "Traceback" not in result.output
    with Database(db_path) as db:
        assert not db.table("files").exists()


def test_insert_files_convert_preserves_pk_across_imports(tmp_path):
    db_path = str(tmp_path / "files.db")
    args = [
        "insert-files",
        db_path,
        "files",
        "-",
        "-c",
        "name:name",
        "-c",
        "content:content",
        "-c",
        "size:size",
        "--pk",
        "NAME",
    ]
    runner = CliRunner()
    result = runner.invoke(
        cli.cli,
        [
            *args,
            "--name",
            "first.txt",
            "--convert",
            '{**row, "content": row["content"].upper()}',
        ],
        input=b"hello",
    )
    assert result.exit_code == 0, result.output
    result = runner.invoke(cli.cli, [*args, "--name", "second.txt"], input=b"world")
    assert result.exit_code == 0, result.output
    with Database(db_path) as db:
        assert db.table("files").pks == ["name"]
        assert db.table("files").columns_dict["name"] is str
        assert list(db.table("files").rows) == [
            {"name": "first.txt", "content": b"HELLO", "size": 5},
            {"name": "second.txt", "content": b"world", "size": 5},
        ]


def test_insert_files_convert_without_selected_pk(tmp_path):
    db_path = str(tmp_path / "files.db")
    args = [
        "insert-files",
        db_path,
        "files",
        "-",
        "-c",
        "content:content",
        "-c",
        "size:size",
    ]
    runner = CliRunner()
    result = runner.invoke(
        cli.cli,
        [
            *args,
            "--convert",
            '{"content": row["content"].upper(), "size": row["size"]}',
        ],
        input=b"hello",
    )
    assert result.exit_code == 0, result.output
    result = runner.invoke(cli.cli, args, input=b"world")
    assert result.exit_code == 0, result.output
    with Database(db_path) as db:
        assert db.table("files").use_rowid
        assert list(db.query("select rowid, content, size from files")) == [
            {"rowid": 1, "content": b"HELLO", "size": 5},
            {"rowid": 2, "content": b"world", "size": 5},
        ]


def test_insert_files_convert_missing_pk_leaves_existing_rows_unchanged(tmp_path):
    db_path = str(tmp_path / "files.db")
    original = {"path": "existing.txt", "content": b"keep", "size": 4}
    with Database(db_path) as db:
        db.table("files").insert(original, pk="path")
    result = CliRunner().invoke(
        cli.cli,
        ["insert-files", db_path, "files", "-", "--convert", 'del row["path"]'],
        input=b"hello",
    )
    assert result.exit_code == 1, result.output
    assert "missing primary key column 'path'" in result.output
    with Database(db_path) as db:
        assert list(db.table("files").rows) == [original]
