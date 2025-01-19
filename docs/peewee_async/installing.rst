Installing
====================

Install latest version from PyPI.

For PostgreSQL aiopg backend:

.. code-block:: console

    pip install peewee-async[postgresql]

For PostgreSQL psycopg3 backend:

.. code-block:: console

    pip install peewee-async[psycopg]

For MySQL:

.. code-block:: console

    pip install peewee-async[mysql]

Installing and developing
+++++++++++++++++++++++++

Clone source code from GitHub:

.. code-block:: console

    git clone https://github.com/05bit/peewee-async.git
    cd peewee-async

Install dependencies using pip:

.. code-block:: console

    pip install -e .[develop]


Or using `poetry`_:

.. _poetry: https://python-poetry.org/docs/

.. code-block:: console

    poetry install -E develop


Running tests
+++++++++++++

* Clone source code from GitHub as shown above
* Run docker environment with PostgreSQL database for testing

.. code-block:: console

    docker-compose up -d

Then run tests:

.. code-block:: console

    pytest -s -v

Report bugs and discuss
+++++++++++++++++++++++

You are welcome to add discussion topics or bug reports to `tracker on GitHub`_!

.. _tracker on GitHub: https://github.com/05bit/peewee-async/issues
