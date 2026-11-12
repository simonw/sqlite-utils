from PyInstaller.utils.hooks import can_import_module

hiddenimports = [
    module
    for module in ("numpy", "pandas", "sqlite_dump", "pysqlite3", "pysqlite3.dbapi2")
    if can_import_module(module)
]
