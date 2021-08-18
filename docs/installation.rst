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
