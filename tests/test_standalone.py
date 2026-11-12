import importlib.metadata
import json
import os
import subprocess
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def standalone_executable():
    executable = os.environ.get("SQLITE_UTILS_EXECUTABLE")
    if not executable:
        pytest.skip("Set SQLITE_UTILS_EXECUTABLE to a built standalone executable")
    path = Path(executable).resolve()
    assert path.is_file(), f"Standalone executable does not exist: {path}"
    return path


@pytest.fixture
def run_standalone(standalone_executable, tmp_path):
    empty_path = tmp_path / "empty-path"
    empty_path.mkdir()
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("PYTHON", "VIRTUAL_ENV", "CONDA"))
    }
    env["PATH"] = str(empty_path)

    def run(*args):
        return subprocess.run(
            [str(standalone_executable), *map(str, args)],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    return run


def test_standalone_help_and_version(run_standalone):
    help_result = run_standalone("--help")
    assert help_result.returncode == 0, help_result.stderr
    assert "create-table" in help_result.stdout
    assert "insert" in help_result.stdout
    assert "query" in help_result.stdout

    version_result = run_standalone("--version")
    assert version_result.returncode == 0, version_result.stderr
    assert version_result.stdout.strip().endswith(
        f", version {importlib.metadata.version('sqlite-utils')}"
    )


def test_standalone_import_query_export_and_fts(run_standalone, tmp_path):
    database = tmp_path / "people.db"
    json_file = tmp_path / "people.json"
    json_file.write_text(
        json.dumps([{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]),
        encoding="utf-8",
    )
    csv_file = tmp_path / "cities.csv"
    csv_file.write_text("id,city\n1,London\n2,New York\n", encoding="utf-8")

    created = run_standalone(
        "create-table",
        database,
        "people",
        "id",
        "integer",
        "name",
        "text",
        "--pk",
        "id",
    )
    assert created.returncode == 0, created.stderr

    imported_json = run_standalone(
        "insert", database, "people", json_file, "--pk", "id"
    )
    assert imported_json.returncode == 0, imported_json.stderr

    imported_csv = run_standalone("insert", database, "cities", csv_file, "--csv")
    assert imported_csv.returncode == 0, imported_csv.stderr

    query = run_standalone("query", database, "select id, name from people order by id")
    assert query.returncode == 0, query.stderr
    assert json.loads(query.stdout) == [
        {"id": 1, "name": "Ada"},
        {"id": 2, "name": "Grace"},
    ]

    csv_export = run_standalone(
        "query", database, "select city from cities order by id", "--csv"
    )
    assert csv_export.returncode == 0, csv_export.stderr
    assert csv_export.stdout == "city\nLondon\nNew York\n"

    enabled_fts = run_standalone("enable-fts", database, "people", "name")
    assert enabled_fts.returncode == 0, enabled_fts.stderr
    fts_query = run_standalone(
        "query", database, "select count(*) as count from people_fts"
    )
    assert fts_query.returncode == 0, fts_query.stderr
    assert json.loads(fts_query.stdout) == [{"count": 2}]


def test_standalone_invalid_arguments_have_click_error(run_standalone):
    result = run_standalone("query", "--not-a-real-option")
    assert result.returncode == 2
    assert "No such option" in result.stderr


def test_standalone_missing_arguments_have_click_error(run_standalone):
    result = run_standalone("create-table")
    assert result.returncode == 2
    assert "Missing argument" in result.stderr


@pytest.mark.parametrize(
    "command",
    [("install", "example-plugin"), ("uninstall", "example-plugin")],
)
def test_standalone_package_management_has_helpful_error(run_standalone, command):
    result = run_standalone(*command)
    assert result.returncode == 1
    assert "not supported by the standalone executable" in result.stderr
    assert "Traceback" not in result.stderr


def test_standalone_plugin_listing(run_standalone):
    result = run_standalone("plugins")
    assert result.returncode == 0, result.stderr
    plugins = json.loads(result.stdout)
    expected_plugin = os.environ.get("SQLITE_UTILS_STANDALONE_TEST_PLUGIN")
    if expected_plugin:
        assert any(plugin["name"] == expected_plugin for plugin in plugins)
    else:
        assert plugins == []


def test_standalone_loads_plugin_bundled_at_build_time(run_standalone):
    if not os.environ.get("SQLITE_UTILS_STANDALONE_TEST_PLUGIN"):
        pytest.skip("Test plugin was not installed when the executable was built")
    result = run_standalone("standalone-plugin-test")
    assert result.returncode == 0, result.stderr
    assert result.stdout == "Bundled plugin works\n"
