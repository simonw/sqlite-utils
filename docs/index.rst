=======================
 sqlite-utils |version|
=======================

|PyPI| |Changelog| |CI| |License| |discord|

.. |PyPI| image:: https://img.shields.io/pypi/v/sqlite-utils.svg
   :target: https://pypi.org/project/sqlite-utils/
.. |Changelog| image:: https://img.shields.io/github/v/release/simonw/sqlite-utils?include_prereleases&label=changelog
   :target: https://sqlite-utils.datasette.io/en/stable/changelog.html
.. |CI| image:: https://github.com/simonw/sqlite-utils/workflows/Test/badge.svg
   :target: https://github.com/simonw/sqlite-utils/actions
.. |License| image:: https://img.shields.io/badge/license-Apache%202.0-blue.svg
   :target: https://github.com/simonw/sqlite-utils/blob/main/LICENSE
.. |discord| image:: https://img.shields.io/discord/823971286308356157?label=discord
   :target: https://discord.gg/Ass7bCAMDw

*CLI tool and Python library for manipulating SQLite databases*

This library and command-line utility helps create SQLite databases from an existing collection of data.

Most of the functionality is available as either a Python API or through the ``sqlite-utils`` command-line tool.

sqlite-utils is not intended to be a full ORM: the focus is utility helpers to make creating the initial database and populating it with data as productive as possible.

It is designed as a useful complement to `Datasette <https://datasette.io/>`_.

`Cleaning data with sqlite-utils and Datasette <https://datasette.io/tutorials/clean-data>`_ provides a tutorial introduction (and accompanying ten minute video) about using this tool.

Contents
--------

.. toctree::
   :maxdepth: 3

   installation
   cli
   python-api
   plugins
   reference
   cli-reference
   contributing
   changelog
