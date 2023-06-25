import json

import pytest
from click.testing import CliRunner

from sqlite_utils import Database, cli


def test_memory_basic():
    result = CliRunner().invoke(cli.cli, ["memory", "select 1 + 1"])
    assert result.exit_code == 0
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
        with open(csv_path, "w") as fp:
            fp.write(content)
    result = CliRunner().invoke(
        cli.cli,
        ["memory", csv_path, "select * from {}".format(sql_from), "--nl"],
        input=input,
    )
    assert result.exit_code == 0
    assert (
        result.output.strip() == '{"id": 1, "name": "Cleo"}\n{"id": 2, "name": "Bants"}'
    )


@pytest.mark.parametrize("use_stdin", (True, False))
def test_memory_tsv(tmpdir, use_stdin):
    data = "id\tname\n1\tCleo\n2\tBants"
    if use_stdin:
        input = data
        path = "stdin:tsv"
        sql_from = "stdin"
    else:
        input = None
        path = str(tmpdir / "chickens.tsv")
        with open(path, "w") as fp:
            fp.write(data)
        path = path + ":tsv"
        sql_from = "chickens"
    result = CliRunner().invoke(
        cli.cli,
        ["memory", path, "select * from {}".format(sql_from)],
        input=input,
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.strip()) == [
        {"id": 1, "name": "Cleo"},
        {"id": 2, "name": "Bants"},
    ]


@pytest.mark.parametrize("use_stdin", (True, False))
def test_memory_json(tmpdir, use_stdin):
    data = '[{"name": "Bants"}, {"name": "Dori", "age": 1, "nested": {"nest": 1}}]'
    if use_stdin:
        input = data
        path = "stdin:json"
        sql_from = "stdin"
    else:
        input = None
        path = str(tmpdir / "chickens.json")
        with open(path, "w") as fp:
            fp.write(data)
        path = path + ":json"
        sql_from = "chickens"
    result = CliRunner().invoke(
        cli.cli,
        ["memory", path, "select * from {}".format(sql_from)],
        input=input,
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.strip()) == [
        {"name": "Bants", "age": None, "nested": None},
        {"name": "Dori", "age": 1, "nested": '{"nest": 1}'},
    ]


@pytest.mark.parametrize("use_stdin", (True, False))
def test_memory_json_nl(tmpdir, use_stdin):
    data = '{"name": "Bants"}\n\n{"name": "Dori"}'
    if use_stdin:
        input = data
        path = "stdin:nl"
        sql_from = "stdin"
    else:
        input = None
        path = str(tmpdir / "chickens.json")
        with open(path, "w") as fp:
            fp.write(data)
        path = path + ":nl"
        sql_from = "chickens"
    result = CliRunner().invoke(
        cli.cli,
        ["memory", path, "select * from {}".format(sql_from)],
        input=input,
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.strip()) == [
        {"name": "Bants"},
        {"name": "Dori"},
    ]


@pytest.mark.parametrize("use_stdin", (True, False))
def test_memory_csv_encoding(tmpdir, use_stdin):
    latin1_csv = (
        b"date,name,latitude,longitude\n" b"2020-03-04,S\xe3o Paulo,-23.561,-46.645\n"
    )
    input = None
    if use_stdin:
        input = latin1_csv
        csv_path = "-"
        sql_from = "stdin"
    else:
        csv_path = str(tmpdir / "test.csv")
        with open(csv_path, "wb") as fp:
            fp.write(latin1_csv)
        sql_from = "test"
    # Without --encoding should error:
    assert (
        CliRunner()
        .invoke(
            cli.cli,
            ["memory", csv_path, "select * from {}".format(sql_from), "--nl"],
            input=input,
        )
        .exit_code
        == 1
    )
    # With --encoding should work:
    result = CliRunner().invoke(
        cli.cli,
        ["memory", "-", "select * from stdin", "--encoding", "latin-1", "--nl"],
        input=latin1_csv,
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.strip()) == {
        "date": "2020-03-04",
        "name": "São Paulo",
        "latitude": -23.561,
        "longitude": -46.645,
    }


@pytest.mark.parametrize("extra_args", ([], ["select 1"]))
def test_memory_dump(extra_args):
    result = CliRunner().invoke(
        cli.cli,
        ["memory", "-"] + extra_args + ["--dump"],
        input="id,name\n1,Cleo\n2,Bants",
    )
    assert result.exit_code == 0
    expected = (
        "BEGIN TRANSACTION;\n"
        'CREATE TABLE IF NOT EXISTS "stdin" (\n'
        "   [id] INTEGER,\n"
        "   [name] TEXT\n"
        ");\n"
        "INSERT INTO \"stdin\" VALUES(1,'Cleo');\n"
        "INSERT INTO \"stdin\" VALUES(2,'Bants');\n"
        "CREATE VIEW t1 AS select * from [stdin];\n"
        "CREATE VIEW t AS select * from [stdin];\n"
        "COMMIT;"
    )
    # Using sqlite-dump it won't have IF NOT EXISTS
    expected_alternative = expected.replace("IF NOT EXISTS ", "")
    assert result.output.strip() in (expected, expected_alternative)


@pytest.mark.parametrize("extra_args", ([], ["select 1"]))
def test_memory_schema(extra_args):
    result = CliRunner().invoke(
        cli.cli,
        ["memory", "-"] + extra_args + ["--schema"],
        input="id,name\n1,Cleo\n2,Bants",
    )
    assert result.exit_code == 0
    assert result.output.strip() == (
        'CREATE TABLE "stdin" (\n'
        "   [id] INTEGER,\n"
        "   [name] TEXT\n"
        ");\n"
        "CREATE VIEW t1 AS select * from [stdin];\n"
        "CREATE VIEW t AS select * from [stdin];"
    )


@pytest.mark.parametrize("extra_args", ([], ["select 1"]))
def test_memory_save(tmpdir, extra_args):
    save_to = str(tmpdir / "save.db")
    result = CliRunner().invoke(
        cli.cli,
        ["memory", "-"] + extra_args + ["--save", save_to],
        input="id,name\n1,Cleo\n2,Bants",
    )
    assert result.exit_code == 0
    db = Database(save_to)
    assert list(db["stdin"].rows) == [
        {"id": 1, "name": "Cleo"},
        {"id": 2, "name": "Bants"},
    ]


@pytest.mark.parametrize("option", ("-n", "--no-detect-types"))
def test_memory_no_detect_types(option):
    result = CliRunner().invoke(
        cli.cli,
        ["memory", "-", "select * from stdin"] + [option],
        input="id,name,weight\n1,Cleo,45.5\n2,Bants,3.5",
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.strip()) == [
        {"id": "1", "name": "Cleo", "weight": "45.5"},
        {"id": "2", "name": "Bants", "weight": "3.5"},
    ]


def test_memory_flatten():
    result = CliRunner().invoke(
        cli.cli,
        ["memory", "-", "select * from stdin", "--flatten"],
        input=json.dumps(
            {
                "httpRequest": {
                    "latency": "0.112114537s",
                    "requestMethod": "GET",
                },
                "insertId": "6111722f000b5b4c4d4071e2",
            }
        ),
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.strip()) == [
        {
            "httpRequest_latency": "0.112114537s",
            "httpRequest_requestMethod": "GET",
            "insertId": "6111722f000b5b4c4d4071e2",
        }
    ]


def test_memory_analyze():
    result = CliRunner().invoke(
        cli.cli,
        ["memory", "-", "--analyze"],
        input="id,name\n1,Cleo\n2,Bants",
    )
    assert result.exit_code == 0
    assert result.output == (
        "stdin.id: (1/2)\n\n"
        "  Total rows: 2\n"
        "  Null rows: 0\n"
        "  Blank rows: 0\n\n"
        "  Distinct values: 2\n\n"
        "stdin.name: (2/2)\n\n"
        "  Total rows: 2\n"
        "  Null rows: 0\n"
        "  Blank rows: 0\n\n"
        "  Distinct values: 2\n\n"
    )


def test_memory_two_files_with_same_stem(tmpdir):
    (tmpdir / "one").mkdir()
    (tmpdir / "two").mkdir()
    one = tmpdir / "one" / "data.csv"
    two = tmpdir / "two" / "data.csv"
    one.write_text("id,name\n1,Cleo\n2,Bants", encoding="utf-8")
    two.write_text("id,name\n3,Blue\n4,Lila", encoding="utf-8")
    result = CliRunner().invoke(cli.cli, ["memory", str(one), str(two), "", "--schema"])
    assert result.exit_code == 0
    assert result.output == (
        'CREATE TABLE "data" (\n'
        "   [id] INTEGER,\n"
        "   [name] TEXT\n"
        ");\n"
        "CREATE VIEW t1 AS select * from [data];\n"
        "CREATE VIEW t AS select * from [data];\n"
        'CREATE TABLE "data_2" (\n'
        "   [id] INTEGER,\n"
        "   [name] TEXT\n"
        ");\n"
        "CREATE VIEW t2 AS select * from [data_2];\n"
    )


def test_memory_functions():
    result = CliRunner().invoke(
        cli.cli,
        ["memory", "select hello()", "--functions", "hello = lambda: 'Hello'"],
    )
    assert result.exit_code == 0
    assert result.output.strip() == '[{"hello()": "Hello"}]'
