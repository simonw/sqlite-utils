from PyInstaller.utils.hooks import collect_entry_point

datas, hiddenimports = collect_entry_point("sqlite_utils")  # codespell:ignore datas
