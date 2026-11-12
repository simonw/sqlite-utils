.. _installation:

==============
 Installation
==============

``sqlite-utils`` is tested on Linux, macOS and Windows.

.. _installation_homebrew:

Using Homebrew
==============

The :ref:`sqlite-utils command-line tool <cli>` can be installed on macOS using Homebrew::

    brew install sqlite-utils

If you have it installed and want to upgrade to the most recent release, you can run::

    brew upgrade sqlite-utils

Then run ``sqlite-utils --version`` to confirm the installed version.

.. _installation_pip:

Using pip
=========

The `sqlite-utils package <https://pypi.org/project/sqlite-utils/>`__ on PyPI includes both the :ref:`sqlite_utils Python library <python_api>` and the ``sqlite-utils`` command-line tool. You can install them using ``pip`` like so::

    pip install sqlite-utils

.. _installation_pipx:

Using pipx
==========

`pipx <https://pypi.org/project/pipx/>`__ is a tool for installing Python command-line applications in their own isolated environments. You can use ``pipx`` to install the ``sqlite-utils`` command-line tool like this::

    pipx install sqlite-utils

.. _installation_standalone:

Standalone executables
======================

Standalone executables for Linux, macOS and Windows are attached to each
`GitHub release <https://github.com/simonw/sqlite-utils/releases>`__. They
include Python and the required ``sqlite-utils`` dependencies, so Python does
not need to be installed separately. Download the executable for your operating
system and processor architecture, then run it directly. Release assets are
named ``sqlite-utils-linux-x86_64``, ``sqlite-utils-macos-arm64`` and
``sqlite-utils-windows-x86_64.exe``. On macOS or Linux you may need to make the
downloaded file executable first::

    chmod +x sqlite-utils-*

The Linux executable is built on Ubuntu 22.04 and requires a compatible glibc
runtime. Executables are specific to their operating system and CPU architecture.
The SQLite library is the one bundled with the Python runtime used to build the
executable; available SQLite features therefore depend on that build. SQLite
extensions such as SpatiaLite are not included and must be supplied separately
as compatible native libraries.

The executable includes plugins installed in the build environment. Since it
does not use a system Python installation, plugins installed after downloading
the executable are not discovered. The ``install`` and ``uninstall`` commands
are therefore unavailable in the executable. To use additional plugins, install
them before building your own executable as described below. Other optional
integrations such as ``numpy``, ``pandas``, ``sqlite_dump`` and ``pysqlite3`` are
included when installed in the build environment. Modules imported dynamically
by custom Python code may need an explicit PyInstaller hook.

Building a standalone executable
--------------------------------

Builds must be performed on the operating system and architecture for which the
executable is intended; PyInstaller does not cross-compile. From a checkout of
the ``sqlite-utils`` source, install the executable build dependencies and run
the build script::

    pip install . --group executable
    python packaging/build.py

The platform-specific executable will be written to ``dist/``. To include
plugins, install them into the same Python environment before running the build
script. PyInstaller collects ``sqlite_utils`` entry points and their modules
from that environment.

.. _installation_sqlite3_alternatives:

Alternatives to sqlite3
=======================

By default, ``sqlite-utils`` uses the ``sqlite3`` package bundled with the Python standard library.

Depending on your operating system, this may come with some limitations.

On some platforms the ability to load additional extensions (via ``conn.load_extension(...)`` or ``--load-extension=/path/to/extension``) may be disabled.

You may also see the error ``sqlite3.OperationalError: table sqlite_master may not be modified`` when trying to alter an existing table.

You can work around these limitations by installing the `pysqlite3 <https://pypi.org/project/pysqlite3/>`__ package, which provides a drop-in replacement for the standard library ``sqlite3`` module but with a recent version of SQLite and full support for loading extensions.

To install ``pysqlite3`` run the following:

.. code-block:: bash

    sqlite-utils install pysqlite3

``pysqlite3`` does not provide an implementation of the ``.iterdump()`` method. To use that method (see :ref:`python_api_itedump`) or the ``sqlite-utils dump`` command you should also install the ``sqlite-dump`` package:

.. code-block:: bash

    sqlite-utils install sqlite-dump

.. _installation_completion:

Setting up shell completion
===========================

You can configure shell tab completion for the ``sqlite-utils`` command using these commands.

For ``bash``:

.. code-block:: bash

    eval "$(_SQLITE_UTILS_COMPLETE=bash_source sqlite-utils)"

For ``zsh``:

.. code-block:: zsh

    eval "$(_SQLITE_UTILS_COMPLETE=zsh_source sqlite-utils)"

Add this code to ``~/.zshrc`` or ``~/.bashrc`` to automatically run it when you start a new shell.

See `the Click documentation <https://click.palletsprojects.com/en/8.1.x/shell-completion/>`__ for more details.
