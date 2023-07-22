.. _plugins:

=========
 Plugins
=========

``sqlite-utils`` supports plugins, which can be used to add extra features to the software.

Plugins can add new commands, for example ``sqlite-utils some-command ...``

Plugins can be installed using the ``sqlite-utils install`` command:

.. code-block:: bash

    sqlite-utils install sqlite-utils-name-of-plugin

You can see a JSON list of plugins that have been installed by running this:

.. code-block:: bash

    sqlite-utils plugins

Plugin hooks such as :ref:`plugins_hooks_prepare_connection` affect each instance of the ``Database`` class. You can opt-out of these plugins by creating that class instance like so:

.. code-block:: python

    db = Database(memory=True, execute_plugins=False)

.. _plugins_building:

Building a plugin
-----------------

Plugins are created in a directory named after the plugin. To create a "hello world" plugin, first create a ``hello-world`` directory:

.. code-block:: bash

    mkdir hello-world
    cd hello-world

In that folder create two files. The first is a ``pyproject.toml`` file describing the plugin:

.. code-block:: toml

    [project]
    name = "sqlite-utils-hello-world"
    version = "0.1"

    [project.entry-points.sqlite_utils]
    hello_world = "sqlite_utils_hello_world"

The ```[project.entry-points.sqlite_utils]`` section tells ``sqlite-tils`` which module to load when executing the plugin.

Then create ``sqlite_utils_hello_world.py`` with the following content:

.. code-block:: python

    import click
    import sqlite_utils

    @sqlite_utils.hookimpl
    def register_commands(cli):
        @cli.command()
        def hello_world():
            "Say hello world"
            click.echo("Hello world!")

Install the plugin in "editable" mode - so you can make changes to the code and have them picked up instantly by ``sqlite-utils`` - like this:

.. code-block:: bash

    sqlite-utils install -e .

Or pass the path to your plugin directory:

.. code-block:: bash

    sqlite-utils install -e `/dev/sqlite-utils-hello-world

Now, running this should execute your new command:

.. code-block:: bash

    sqlite-utils hello-world

Your command will also be listed in the output of ``sqlite-utils --help``.

See the `LLM plugin documentation <https://llm.datasette.io/en/stable/plugins/tutorial-model-plugin.html#distributing-your-plugin>`__ for tips on distributing your plugin.

.. _plugins_hooks:

Plugin hooks
------------

Plugin hooks allow ``sqlite-utils`` to be customized.

.. _plugins_hooks_register_commands:

register_commands(cli)
~~~~~~~~~~~~~~~~~~~~~~

This hook can be used to register additional commands with the ``sqlite-utils`` CLI. It is called with the ``cli`` object, which is a ``click.Group`` instance.

Example implementation:

.. code-block:: python

    import click
    import sqlite_utils

    @sqlite_utils.hookimpl
    def register_commands(cli):
        @cli.command()
        def hello_world():
            "Say hello world"
            click.echo("Hello world!")

.. _plugins_hooks_prepare_connection:

prepare_connection(conn)
~~~~~~~~~~~~~~~~~~~~~~~~

This hook is called when a new SQLite database connection is created. You can
use it to `register custom SQL functions <https://docs.python.org/2/library/sqlite3.html#sqlite3.Connection.create_function>`_,
aggregates and collations. For example:

.. code-block:: python

    import sqlite_utils

    @sqlite_utils.hookimpl
    def prepare_connection(conn):
        conn.create_function(
            "hello", 1, lambda name: f"Hello, {name}!"
        )

This registers a SQL function called ``hello`` which takes a single
argument and can be called like this:

.. code-block:: sql

    select hello("world"); -- "Hello, world!"
