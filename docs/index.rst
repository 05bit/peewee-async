.. peewee-async documentation master file, created by
   sphinx-quickstart on Sat Oct 11 20:12:24 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

peewee-async
============

**peewee-async** is a library providing asynchronous interface powered by `asyncio`_ for `peewee`_ ORM.

.. _peewee: https://github.com/coleifer/peewee
.. _asyncio: https://docs.python.org/3/library/asyncio.html


* Works on Python 3.8+
* Has support for PostgreSQL via `aiopg`
* Has support for MySQL via `aiomysql`
* Asynchronous analogues of peewee sync methods with prefix **aio_**
* Drop-in replacement for sync code, sync will remain sync
* Basic operations are supported
* Transactions support is present

The source code is hosted on `GitHub`_.

.. _GitHub: https://github.com/05bit/peewee-async

Quickstart
----------

.. literalinclude:: ./samples/quickstart.py


Install
-------

Install latest version from PyPI.

For PostgreSQL:

.. code-block:: console

    pip install peewee-async[postgresql]

For MySQL:

.. code-block:: console

    pip install peewee-async[mysql]

Install from sources
++++++++++++++++++++

.. code-block:: console

    git clone https://github.com/05bit/peewee-async.git
    cd peewee-async
    pip install .

Running tests
+++++++++++++

Prepare environment for tests:

* Clone source code from GitHub as shown above
* Run docker environment with PostgreSQL database for testing

Then run tests:

.. code-block:: console

    pytest -s -v

Report bugs and discuss
-----------------------

You are welcome to add discussion topics or bug reports to `tracker on GitHub`_!

.. _tracker on GitHub: https://github.com/05bit/peewee-async/issues

Contents
--------

.. toctree::
   :maxdepth: 2

   peewee_async/api
   peewee_async/examples

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

