from sqlite_utils import cli, Database
from click.testing import CliRunner
import json
import pytest


def test_insert_simple(tmpdir):
    json_path = str(tmpdir / "dog.json")
    db_path = str(tmpdir / "dogs.db")
    open(json_path, "w").write(json.dumps({"name": "Cleo", "age": 4}))
    result = CliRunner().invoke(cli.cli, ["insert", db_path, "dogs", json_path])
    assert 0 == result.exit_code
    assert [{"age": 4, "name": "Cleo"}] == list(
        Database(db_path).query("select * from dogs")
    )
    db = Database(db_path)
    assert ["dogs"] == db.table_names()
    assert [] == db["dogs"].indexes


def test_insert_from_stdin(tmpdir):
    db_path = str(tmpdir / "dogs.db")
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "dogs", "-"],
        input=json.dumps({"name": "Cleo", "age": 4}),
    )
    assert 0 == result.exit_code
    assert [{"age": 4, "name": "Cleo"}] == list(
        Database(db_path).query("select * from dogs")
    )


def test_insert_invalid_json_error(tmpdir):
    db_path = str(tmpdir / "dogs.db")
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "dogs", "-"],
        input="name,age\nCleo,4",
    )
    assert result.exit_code == 1
    assert (
        result.output
        == "Error: Invalid JSON - use --csv for CSV or --tsv for TSV files\n"
    )


def test_insert_json_flatten(tmpdir):
    db_path = str(tmpdir / "flat.db")
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "items", "-", "--flatten"],
        input=json.dumps({"nested": {"data": 4}}),
    )
    assert result.exit_code == 0
    assert list(Database(db_path).query("select * from items")) == [{"nested_data": 4}]


def test_insert_json_flatten_nl(tmpdir):
    db_path = str(tmpdir / "flat.db")
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "items", "-", "--flatten", "--nl"],
        input="\n".join(
            json.dumps(item)
            for item in [{"nested": {"data": 4}}, {"nested": {"other": 3}}]
        ),
    )
    assert result.exit_code == 0
    assert list(Database(db_path).query("select * from items")) == [
        {"nested_data": 4, "nested_other": None},
        {"nested_data": None, "nested_other": 3},
    ]


def test_insert_with_primary_key(db_path, tmpdir):
    json_path = str(tmpdir / "dog.json")
    open(json_path, "w").write(json.dumps({"id": 1, "name": "Cleo", "age": 4}))
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id"]
    )
    assert 0 == result.exit_code
    assert [{"id": 1, "age": 4, "name": "Cleo"}] == list(
        Database(db_path).query("select * from dogs")
    )
    db = Database(db_path)
    assert ["id"] == db["dogs"].pks


def test_insert_multiple_with_primary_key(db_path, tmpdir):
    json_path = str(tmpdir / "dogs.json")
    dogs = [{"id": i, "name": "Cleo {}".format(i), "age": i + 3} for i in range(1, 21)]
    open(json_path, "w").write(json.dumps(dogs))
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id"]
    )
    assert 0 == result.exit_code
    db = Database(db_path)
    assert dogs == list(db.query("select * from dogs order by id"))
    assert ["id"] == db["dogs"].pks


def test_insert_multiple_with_compound_primary_key(db_path, tmpdir):
    json_path = str(tmpdir / "dogs.json")
    dogs = [
        {"breed": "mixed", "id": i, "name": "Cleo {}".format(i), "age": i + 3}
        for i in range(1, 21)
    ]
    open(json_path, "w").write(json.dumps(dogs))
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id", "--pk", "breed"]
    )
    assert 0 == result.exit_code
    db = Database(db_path)
    assert dogs == list(db.query("select * from dogs order by breed, id"))
    assert {"breed", "id"} == set(db["dogs"].pks)
    assert (
        "CREATE TABLE [dogs] (\n"
        "   [breed] TEXT,\n"
        "   [id] INTEGER,\n"
        "   [name] TEXT,\n"
        "   [age] INTEGER,\n"
        "   PRIMARY KEY ([id], [breed])\n"
        ")"
    ) == db["dogs"].schema


def test_insert_not_null_default(db_path, tmpdir):
    json_path = str(tmpdir / "dogs.json")
    dogs = [
        {"id": i, "name": "Cleo {}".format(i), "age": i + 3, "score": 10}
        for i in range(1, 21)
    ]
    open(json_path, "w").write(json.dumps(dogs))
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "dogs", json_path, "--pk", "id"]
        + ["--not-null", "name", "--not-null", "age"]
        + ["--default", "score", "5", "--default", "age", "1"],
    )
    assert 0 == result.exit_code
    db = Database(db_path)
    assert (
        "CREATE TABLE [dogs] (\n"
        "   [id] INTEGER PRIMARY KEY,\n"
        "   [name] TEXT NOT NULL,\n"
        "   [age] INTEGER NOT NULL DEFAULT '1',\n"
        "   [score] INTEGER DEFAULT '5'\n)"
    ) == db["dogs"].schema


def test_insert_binary_base64(db_path):
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "files", "-"],
        input=r'{"content": {"$base64": true, "encoded": "aGVsbG8="}}',
    )
    assert 0 == result.exit_code, result.output
    db = Database(db_path)
    actual = list(db.query("select content from files"))
    assert actual == [{"content": b"hello"}]


def test_insert_newline_delimited(db_path):
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "from_json_nl", "-", "--nl"],
        input='{"foo": "bar", "n": 1}\n{"foo": "baz", "n": 2}',
    )
    assert 0 == result.exit_code, result.output
    db = Database(db_path)
    assert [
        {"foo": "bar", "n": 1},
        {"foo": "baz", "n": 2},
    ] == list(db.query("select foo, n from from_json_nl"))


def test_insert_ignore(db_path, tmpdir):
    db = Database(db_path)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    json_path = str(tmpdir / "dogs.json")
    open(json_path, "w").write(json.dumps([{"id": 1, "name": "Bailey"}]))
    # Should raise error without --ignore
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id"]
    )
    assert 0 != result.exit_code, result.output
    # If we use --ignore it should run OK
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id", "--ignore"]
    )
    assert 0 == result.exit_code, result.output
    # ... but it should actually have no effect
    assert [{"id": 1, "name": "Cleo"}] == list(db.query("select * from dogs"))


@pytest.mark.parametrize(
    "content,options",
    [
        ("foo\tbar\tbaz\n1\t2\tcat,dog", ["--tsv"]),
        ('foo,bar,baz\n1,2,"cat,dog"', ["--csv"]),
        ('foo;bar;baz\n1;2;"cat,dog"', ["--csv", "--delimiter", ";"]),
        # --delimiter implies --csv:
        ('foo;bar;baz\n1;2;"cat,dog"', ["--delimiter", ";"]),
        ("foo,bar,baz\n1,2,|cat,dog|", ["--csv", "--quotechar", "|"]),
        ("foo,bar,baz\n1,2,|cat,dog|", ["--quotechar", "|"]),
    ],
)
def test_insert_csv_tsv(content, options, db_path, tmpdir):
    db = Database(db_path)
    file_path = str(tmpdir / "insert.csv-tsv")
    open(file_path, "w").write(content)
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "data", file_path] + options,
        catch_exceptions=False,
    )
    assert 0 == result.exit_code
    assert [{"foo": "1", "bar": "2", "baz": "cat,dog"}] == list(db["data"].rows)


@pytest.mark.parametrize(
    "options",
    (
        ["--tsv", "--nl"],
        ["--tsv", "--csv"],
        ["--csv", "--nl"],
        ["--csv", "--nl", "--tsv"],
    ),
)
def test_only_allow_one_of_nl_tsv_csv(options, db_path, tmpdir):
    file_path = str(tmpdir / "insert.csv-tsv")
    open(file_path, "w").write("foo")
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "data", file_path] + options
    )
    assert 0 != result.exit_code
    assert "Error: Use just one of --nl, --csv or --tsv" == result.output.strip()


def test_insert_replace(db_path, tmpdir):
    test_insert_multiple_with_primary_key(db_path, tmpdir)
    json_path = str(tmpdir / "insert-replace.json")
    db = Database(db_path)
    assert 20 == db["dogs"].count
    insert_replace_dogs = [
        {"id": 1, "name": "Insert replaced 1", "age": 4},
        {"id": 2, "name": "Insert replaced 2", "age": 4},
        {"id": 21, "name": "Fresh insert 21", "age": 6},
    ]
    open(json_path, "w").write(json.dumps(insert_replace_dogs))
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id", "--replace"]
    )
    assert 0 == result.exit_code, result.output
    assert 21 == db["dogs"].count
    assert (
        list(db.query("select * from dogs where id in (1, 2, 21) order by id"))
        == insert_replace_dogs
    )


def test_insert_truncate(db_path):
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "from_json_nl", "-", "--nl", "--batch-size=1"],
        input='{"foo": "bar", "n": 1}\n{"foo": "baz", "n": 2}',
    )
    assert 0 == result.exit_code, result.output
    db = Database(db_path)
    assert [
        {"foo": "bar", "n": 1},
        {"foo": "baz", "n": 2},
    ] == list(db.query("select foo, n from from_json_nl"))
    # Truncate and insert new rows
    result = CliRunner().invoke(
        cli.cli,
        [
            "insert",
            db_path,
            "from_json_nl",
            "-",
            "--nl",
            "--truncate",
            "--batch-size=1",
        ],
        input='{"foo": "bam", "n": 3}\n{"foo": "bat", "n": 4}',
    )
    assert 0 == result.exit_code, result.output
    assert [
        {"foo": "bam", "n": 3},
        {"foo": "bat", "n": 4},
    ] == list(db.query("select foo, n from from_json_nl"))


def test_insert_alter(db_path, tmpdir):
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "from_json_nl", "-", "--nl"],
        input='{"foo": "bar", "n": 1}\n{"foo": "baz", "n": 2}',
    )
    assert 0 == result.exit_code, result.output
    # Should get an error with incorrect shaped additional data
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "from_json_nl", "-", "--nl"],
        input='{"foo": "bar", "baz": 5}',
    )
    assert 0 != result.exit_code, result.output
    # If we run it again with --alter it should work correctly
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "from_json_nl", "-", "--nl", "--alter"],
        input='{"foo": "bar", "baz": 5}',
    )
    assert 0 == result.exit_code, result.output
    # Soundness check the database itself
    db = Database(db_path)
    assert {"foo": str, "n": int, "baz": int} == db["from_json_nl"].columns_dict
    assert [
        {"foo": "bar", "n": 1, "baz": None},
        {"foo": "baz", "n": 2, "baz": None},
        {"foo": "bar", "baz": 5, "n": None},
    ] == list(db.query("select foo, n, baz from from_json_nl"))


def test_insert_lines(db_path):
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "from_lines", "-", "--lines"],
        input='First line\nSecond line\n{"foo": "baz"}',
    )
    assert 0 == result.exit_code, result.output
    db = Database(db_path)
    assert [
        {"line": "First line"},
        {"line": "Second line"},
        {"line": '{"foo": "baz"}'},
    ] == list(db.query("select line from from_lines"))


def test_insert_text(db_path):
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "from_text", "-", "--text"],
        input='First line\nSecond line\n{"foo": "baz"}',
    )
    assert 0 == result.exit_code, result.output
    db = Database(db_path)
    assert [{"text": 'First line\nSecond line\n{"foo": "baz"}'}] == list(
        db.query("select text from from_text")
    )


@pytest.mark.parametrize(
    "options,input",
    (
        ([], '[{"id": "1", "name": "Bob"}, {"id": "2", "name": "Cat"}]'),
        (["--csv"], "id,name\n1,Bob\n2,Cat"),
        (["--nl"], '{"id": "1", "name": "Bob"}\n{"id": "2", "name": "Cat"}'),
    ),
)
def test_insert_convert_json_csv_jsonnl(db_path, options, input):
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "rows", "-", "--convert", '{**row, **{"extra": 1}}']
        + options,
        input=input,
    )
    assert result.exit_code == 0, result.output
    db = Database(db_path)
    rows = list(db.query("select id, name, extra from rows"))
    assert rows == [
        {"id": "1", "name": "Bob", "extra": 1},
        {"id": "2", "name": "Cat", "extra": 1},
    ]


def test_insert_convert_text(db_path):
    result = CliRunner().invoke(
        cli.cli,
        [
            "insert",
            db_path,
            "text",
            "-",
            "--text",
            "--convert",
            '{"text": text.upper()}',
        ],
        input="This is text\nwill be upper now",
    )
    assert result.exit_code == 0, result.output
    db = Database(db_path)
    rows = list(db.query("select [text] from [text]"))
    assert rows == [{"text": "THIS IS TEXT\nWILL BE UPPER NOW"}]


def test_insert_convert_text_returning_iterator(db_path):
    result = CliRunner().invoke(
        cli.cli,
        [
            "insert",
            db_path,
            "text",
            "-",
            "--text",
            "--convert",
            '({"word": w} for w in text.split())',
        ],
        input="A bunch of words",
    )
    assert result.exit_code == 0, result.output
    db = Database(db_path)
    rows = list(db.query("select [word] from [text]"))
    assert rows == [{"word": "A"}, {"word": "bunch"}, {"word": "of"}, {"word": "words"}]


def test_insert_convert_lines(db_path):
    result = CliRunner().invoke(
        cli.cli,
        [
            "insert",
            db_path,
            "all",
            "-",
            "--lines",
            "--convert",
            '{"line": line.upper()}',
        ],
        input="This is text\nwill be upper now",
    )
    assert result.exit_code == 0, result.output
    db = Database(db_path)
    rows = list(db.query("select [line] from [all]"))
    assert rows == [{"line": "THIS IS TEXT"}, {"line": "WILL BE UPPER NOW"}]
