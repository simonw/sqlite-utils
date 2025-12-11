.. _contributing:

==============
 Contributing
==============

Development of ``sqlite-utils`` takes place in the `sqlite-utils GitHub repository <https://github.com/simonw/sqlite-utils>`__.

All improvements to the software should start with an issue. Read `How I build a feature <https://simonwillison.net/2022/Jan/12/how-i-build-a-feature/>`__ for a detailed description of the recommended process for building bug fixes or enhancements.

.. _contributing_checkout:

Obtaining the code
==================

To work on this library locally, first checkout the code::

    git clone git@github.com:simonw/sqlite-utils
    cd sqlite-utils

Use ``uv run`` to run the development version of the tool::

    uv run sqlite-utils --help

.. _contributing_tests:

Running the tests
=================

Use ``uv run`` to run the tests::

    uv run pytest

.. _contributing_docs:

Building the documentation
==========================

To build the documentation run this command::

    uv run make livehtml --directory docs

This will start a server on port 8000 that will serve the documentation and live-reload any time you make an edit to a ``.rst`` file.

The `cog <https://github.com/nedbat/cog>`__ tool is used to maintain portions of the documentation. You can run it like so::

    uv run cog -r docs/*.rst

.. _contributing_linting:

Linting and formatting
======================

``sqlite-utils`` uses `Black <https://black.readthedocs.io/>`__ for code formatting, and `flake8 <https://flake8.pycqa.org/>`__ and `mypy <https://mypy.readthedocs.io/>`__ for linting and type checking::

    uv run black .

Linting tools can be run like this::

    uv run flake8
    uv run mypy sqlite_utils

All three of these tools are run by our CI mechanism against every commit and pull request.

.. _contributing_just:

Using Just
==========

If you install `Just <https://github.com/casey/just>`__ you can use it to manage your local development environment.

To run all of the tests and linters::

    just

To run tests, or run a specific test module or test by name::

    just test # All tests
    just test tests/test_cli_memory.py # Just this module
    just test -k test_memory_no_detect_types # Just this test

To run just the linters::

    just lint

To apply Black to your code::

    just black

To update documentation using Cog::

    just cog

To run the live documentation server (this will run Cog first)::

    just docs

And to list all available commands::

    just -l

.. _release_process:

Release process
===============

Releases are performed using tags. When a new release is published on GitHub, a `GitHub Actions workflow <https://github.com/simonw/sqlite-utils/blob/main/.github/workflows/publish.yml>`__ will perform the following:

* Run the unit tests against all supported Python versions. If the tests pass...
* Build a wheel bundle of the underlying Python source code
* Push that new wheel up to PyPI: https://pypi.org/project/sqlite-utils/

To deploy new releases you will need to have push access to the GitHub repository.

``sqlite-utils`` follows `Semantic Versioning <https://semver.org/>`__::

    major.minor.patch

We increment ``major`` for backwards-incompatible releases.

We increment ``minor`` for new features.

We increment ``patch`` for bugfix releass.

To release a new version, first create a commit that updates the version number in ``pyproject.toml`` and the :ref:`the changelog <changelog>` with highlights of the new version. An example `commit can be seen here <https://github.com/simonw/sqlite-utils/commit/b491f22d817836829965516983a3f4c3c72c05fc>`__::

    # Update changelog
    git commit -m " Release 3.29

    Refs #423, #458, #467, #469, #470, #471, #472, #475" -a
    git push

Referencing the issues that are part of the release in the commit message ensures the name of the release shows up on those issue pages, e.g. `here <https://github.com/simonw/sqlite-utils/issues/458#ref-commit-b491f22>`__.

You can generate the list of issue references for a specific release by copying and pasting text from the release notes or GitHub changes-since-last-release view into this `Extract issue numbers from pasted text <https://observablehq.com/@simonw/extract-issue-numbers-from-pasted-text>`__ tool.

To create the tag for the release, create `a new release <https://github.com/simonw/sqlite-utils/releases/new>`__ on GitHub matching the new version number. You can convert the release notes to Markdown by copying and pasting the rendered HTML into this `Paste to Markdown tool <https://euangoddard.github.io/clipboard2markdown/>`__.
