.. _contributing:

==============
 Contributing
==============

Development of ``sqlite-utils`` takes place in the `sqlite-utils GitHub repository <https://github.com/simonw/sqlite-utils>`__.

All improvements to the software should start with an issue. Read `How I build a feature <https://simonwillison.net/2022/Jan/12/how-i-build-a-feature/>`__ for a detailed description of the recommended process for building bug fixes or enhancements.

.. _contributing_checkout:

Obtaining the code
==================

To work on this library locally, first checkout the code. Then create a new virtual environment::

    git clone git@github.com:simonw/sqlite-utils
    cd sqlite-utils
    python3 -mvenv venv
    source venv/bin/activate

Or if you are using ``pipenv``::

    pipenv shell

Within the virtual environment running ``sqlite-utils`` should run your locally editable version of the tool. You can use ``which sqlite-utils`` to confirm that you are running the version that lives in your virtual environment.

.. _contributing_tests:

Running the tests
=================

To install the dependencies and test dependencies::

    pip install -e '.[test]'

To run the tests::

    pytest

.. _contributing_docs:

Building the documentation
==========================

To build the documentation, first install the documentation dependencies::

    pip install -e '.[docs]'

Then run ``make livehtml`` from the ``docs/`` directory to start a server on port 8000 that will serve the documentation and live-reload any time you make an edit to a ``.rst`` file::

    cd docs
    make livehtml

The `cog <https://github.com/nedbat/cog>`__ tool is used to maintain portions of the documentation. You can run it like so::

    cog -r docs/*.rst

.. _contributing_linting:

Linting and formatting
======================

``sqlite-utils`` uses `Black <https://black.readthedocs.io/>`__ for code formatting, and `flake8 <https://flake8.pycqa.org/>`__ and `mypy <https://mypy.readthedocs.io/>`__ for linting and type checking.

Black is installed as part of ``pip install -e '.[test]'`` - you can then format your code by running it in the root of the project::

    black .

To install ``mypy`` and ``flake8`` run the following::

    pip install -e '.[flake8,mypy]'

Both commands can then be run in the root of the project like this::

    flake8
    mypy sqlite_utils

All three of these tools are run by our CI mechanism against every commit and pull request.
