import platform
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"


def main() -> None:
    system = platform.system().lower()
    machine = platform.machine().lower()
    architectures = {
        "amd64": "x86_64",
        "aarch64": "arm64" if system == "darwin" else "aarch64",
        "arm64": "arm64",
        "x86_64": "x86_64",
    }
    if system not in {"darwin", "linux", "windows"}:
        raise SystemExit(f"Unsupported operating system: {system}")
    try:
        architecture = architectures[machine]
    except KeyError:
        raise SystemExit(f"Unsupported architecture: {machine}") from None

    if DIST.exists():
        shutil.rmtree(DIST)
    DIST.mkdir(parents=True)

    subprocess.run(
        [
            sys.executable,
            "-m",
            "PyInstaller",
            "--clean",
            "--noconfirm",
            "--onefile",
            "--name",
            "sqlite-utils",
            "--copy-metadata",
            "sqlite-utils",
            "--additional-hooks-dir",
            str(ROOT / "packaging" / "pyinstaller" / "hooks"),
            "--distpath",
            str(DIST),
            "--workpath",
            str(ROOT / "build" / "pyinstaller"),
            "--specpath",
            str(ROOT / "build" / "pyinstaller"),
            str(ROOT / "packaging" / "standalone.py"),
        ],
        cwd=ROOT,
        check=True,
    )

    executable_os = {"darwin": "macos"}.get(system, system)
    extension = ".exe" if system == "windows" else ""
    executable = DIST / f"sqlite-utils{extension}"
    asset = DIST / f"sqlite-utils-{executable_os}-{architecture}{extension}"
    asset.parent.mkdir(exist_ok=True)
    shutil.move(str(executable), str(asset))
    print(f"Built {asset}")


if __name__ == "__main__":
    main()
