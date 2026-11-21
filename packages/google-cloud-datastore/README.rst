Python Client for Cloud Datastore
=================================

|stable| |pypi| |versions|

`Cloud Datastore`_: Accesses the schemaless NoSQL database to provide fully managed, robust,
scalable storage for your application.

- `Client Library Documentation`_
- `Product Documentation`_

.. |stable| image:: https://img.shields.io/badge/support-stable-gold.svg
   :target: https://github.com/googleapis/google-cloud-python/blob/main/README.rst#stability-levels
.. |pypi| image:: https://img.shields.io/pypi/v/google-cloud-datastore.svg
   :target: https://pypi.org/project/google-cloud-datastore/
.. |versions| image:: https://img.shields.io/pypi/pyversions/google-cloud-datastore.svg
   :target: https://pypi.org/project/google-cloud-datastore/
.. _Cloud Datastore: https://docs.cloud.google.com/datastore/docs
.. _Client Library Documentation: https://docs.cloud.google.com/python/docs/reference/datastore/latest/summary_overview
.. _Product Documentation: https://docs.cloud.google.com/datastore/docs

Choosing the Right Package
--------------------------

- Use ``google.cloud.datastore`` (the handwritten veneer in this package) for
  idiomatic synchronous entities, keys, queries, aggregations, batches, and
  transactions with Cloud Datastore or Firestore in Datastore mode.
- Use ``google.cloud.datastore_v1`` (the auto-generated GAPIC client bundled in
  this same distribution) when you need ``asyncio`` via
  ``DatastoreAsyncClient`` or GAPIC-only features such as GQL queries, vector
  search, server-side transforms, or ``previous_transaction`` retry priority.
  Do not mix types between ``google.cloud.datastore`` and
  ``google.cloud.datastore_v1`` (they share class names such as ``Entity``,
  ``Key``, ``Query``, and ``PropertyFilter``).
- Use ``google-cloud-ndb`` when migrating App Engine NDB applications.
- Use ``google-cloud-firestore`` for Firestore in Native mode.

Quick Start
-----------

In order to use this library, you first need to go through the following steps:

1. `Select or create a Cloud Platform project.`_
2. `Enable billing for your project.`_
3. `Enable the Cloud Datastore.`_
4. `Set up Authentication.`_

.. _Select or create a Cloud Platform project.: https://console.cloud.google.com/project
.. _Enable billing for your project.: https://docs.cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project
.. _Enable the Cloud Datastore.: https://console.cloud.google.com/flows/enableapi?apiid=datastore.googleapis.com
.. _Set up Authentication.: https://docs.cloud.google.com/docs/authentication/client-libraries

Installation
~~~~~~~~~~~~

Install this library in a virtual environment using `venv`_. `venv`_ is a tool that
creates isolated Python environments. These isolated environments can have separate
versions of Python packages, which allows you to isolate one project's dependencies
from the dependencies of other projects.

With `venv`_, it's possible to install this library without needing system
install permissions, and without clashing with the installed system
dependencies.

.. _`venv`: https://docs.python.org/3/library/venv.html

Example Usage
~~~~~~~~~~~~~

.. code-block:: python

    from google.cloud import datastore

    # Reuse a single Client instance across requests.
    client = datastore.Client()

    key = client.key("Task", "sample_task")
    task = datastore.Entity(key=key)
    task["description"] = "Buy milk"
    client.put(task)

    fetched = client.get(key)
    print(fetched["description"])


Code samples and snippets
~~~~~~~~~~~~~~~~~~~~~~~~~

Auto-generated ``google.cloud.datastore_v1`` and
``google.cloud.datastore_admin_v1`` GAPIC snippets live in the `samples/`_
folder.

.. _samples/: https://github.com/googleapis/google-cloud-python/tree/main/packages/google-cloud-datastore/samples


Supported Python Versions
^^^^^^^^^^^^^^^^^^^^^^^^^
Our client libraries are compatible with all current `active`_ and `maintenance`_ versions of
Python.

Python >= 3.10, including 3.15

.. _active: https://devguide.python.org/versions/#supported-versions
.. _maintenance: https://devguide.python.org/versions/#supported-versions

Unsupported Python Versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Python <= 3.9


If you are using an `end-of-life`_
version of Python, we recommend that you update as soon as possible to an actively supported version.

.. _end-of-life: https://devguide.python.org/versions/#unsupported-versions

Mac/Linux
^^^^^^^^^

.. code-block:: console

    python3 -m venv <your-env>
    source <your-env>/bin/activate
    pip install google-cloud-datastore


Windows
^^^^^^^

.. code-block:: console

    py -m venv <your-env>
    .\<your-env>\Scripts\activate
    pip install google-cloud-datastore

Next Steps
~~~~~~~~~~

-  Read the `Client Library Documentation`_ for Cloud Datastore
   to see other available methods on the client.
-  Read the `Cloud Datastore Product documentation`_ to learn
   more about the product and see How-to Guides.
-  View this `README`_ to see the full list of Cloud
   APIs that we cover.

.. _Cloud Datastore Product documentation: https://docs.cloud.google.com/datastore/docs
.. _README: https://github.com/googleapis/google-cloud-python/blob/main/README.rst

Logging
-------

This library uses the standard Python :code:`logging` functionality to log some RPC events that could be of interest for debugging and monitoring purposes.
Note the following:

#. Logs may contain sensitive information. Take care to **restrict access to the logs** if they are saved, whether it be on local storage or on Google Cloud Logging.
#. Google may refine the occurrence, level, and content of various log messages in this library without flagging such changes as breaking. **Do not depend on immutability of the logging events**.
#. By default, the logging events from this library are not handled. You must **explicitly configure log handling** using one of the mechanisms below.

Simple, environment-based configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To enable logging for this library without any changes in your code, set the :code:`GOOGLE_SDK_PYTHON_LOGGING_SCOPE` environment variable to a valid Google
logging scope. This configures handling of logging events (at level :code:`logging.DEBUG` or higher) from this library in a default manner, emitting the logged
messages in a structured format. It does not currently allow customizing the logging levels captured nor the handlers, formatters, etc. used for any logging
event.

A logging scope is a period-separated namespace that begins with :code:`google`, identifying the Python module or package to log.

- Valid logging scopes: :code:`google`, :code:`google.cloud.datastore`, :code:`google.cloud.datastore_v1`, :code:`google.api`, :code:`google.auth`, etc.
- Invalid logging scopes: :code:`foo`, :code:`123`, etc.

**NOTE**: If the logging scope is invalid, the library does not set up any logging handlers.

Environment-Based Examples
^^^^^^^^^^^^^^^^^^^^^^^^^^

- Enabling the default handler for all Google-based loggers

.. code-block:: console

    export GOOGLE_SDK_PYTHON_LOGGING_SCOPE=google

- Enabling the default handler for Cloud Datastore (use :code:`google.cloud.datastore_v1` for underlying GAPIC RPC logs):

.. code-block:: console

    export GOOGLE_SDK_PYTHON_LOGGING_SCOPE=google.cloud.datastore


Advanced, code-based configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also configure a valid logging scope using Python's standard `logging` mechanism.

Code-Based Examples
^^^^^^^^^^^^^^^^^^^

- Configuring a handler for all Google-based loggers

.. code-block:: python

    import logging

    from google.cloud import datastore

    base_logger = logging.getLogger("google")
    base_logger.addHandler(logging.StreamHandler())
    base_logger.setLevel(logging.DEBUG)

- Configuring a handler for Cloud Datastore (or :code:`google.cloud.datastore_v1` for underlying GAPIC RPC logs):

.. code-block:: python

    import logging

    from google.cloud import datastore

    base_logger = logging.getLogger("google.cloud.datastore")
    base_logger.addHandler(logging.StreamHandler())
    base_logger.setLevel(logging.DEBUG)

Logging details
~~~~~~~~~~~~~~~

#. Regardless of which of the mechanisms above you use to configure logging for this library, by default logging events are not propagated up to the root
   logger from the `google`-level logger. If you need the events to be propagated to the root logger, you must explicitly set
   :code:`logging.getLogger("google").propagate = True` in your code.
#. You can mix the different logging configurations above for different Google modules. For example, you may want use a code-based logging configuration for
   one library, but decide you need to also set up environment-based logging configuration for another library.

   #. If you attempt to use both code-based and environment-based configuration for the same module, the environment-based configuration will be ineffectual
      if the code -based configuration gets applied first.

#. The Google-specific logging configurations (default handlers for environment-based configuration; not propagating logging events to the root logger) get
   executed the first time *any* client library is instantiated in your application, and only if the affected loggers have not been previously configured.
   (This is the reason for 2.i. above.)
