=======================
 sqlite-utils |version|
=======================

|PyPI| |Changelog| |CI| |License|

.. |PyPI| image:: https://img.shields.io/pypi/v/sqlite-utils.svg
   :target: https://pypi.org/project/sqlite-utils/
.. |Changelog| image:: https://img.shields.io/github/v/release/simonw/sqlite-utils?include_prereleases&label=changelog
   :target: https://sqlite-utils.readthedocs.io/en/stable/changelog.html
.. |CI| image:: https://github.com/simonw/sqlite-utils/workflows/Test/badge.svg
   :target: https://github.com/simonw/sqlite-utils/actions
.. |License| image:: https://img.shields.io/badge/license-Apache%202.0-blue.svg
   :target: https://github.com/simonw/sqlite-utils/blob/main/LICENSE

*Python utility functions for manipulating SQLite databases*

This library and command-line utility helps create SQLite databases from an existing collection of data.

Most of the functionality is available as either a Python API or through the ``sqlite-utils`` command-line tool.

sqlite-utils is not intended to be a full ORM: the focus is utility helpers to make creating the initial database and populating it with data as productive as possible.

It is designed as a useful complement to `Datasette <https://github.com/simonw/datasette>`_.

Contents
--------

.. toctree::
   :maxdepth: 3

   cli
   python-api
   changelog

Take a look at `this script <https://github.com/simonw/russian-ira-facebook-ads-datasette/blob/master/fetch_and_build_russian_ads.py>`_ for an example of this library in action.
