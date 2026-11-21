Creating a Client
~~~~~~~~~~~~~~~~~

A project is the top-level container in the ``BigQuery`` API:  it is tied
closely to billing, and can provide default access control across all its
datasets.  If no ``project`` is passed to the client container, the library
attempts to infer a project using the environment (including explicit
environment variables, GAE, and GCE).

To override the project inferred from the environment, pass an explicit
``project`` to the :class:`~google.cloud.bigquery.client.Client` constructor,
or to either of the alternative ``classmethod`` factories:

.. code-block:: python

   from google.cloud import bigquery
   client = bigquery.Client(project='PROJECT_ID')


Project IAM Permissions
^^^^^^^^^^^^^^^^^^^^^^^

Each project has IAM access controls granting roles to principals. Project-level
IAM policies cannot be managed through the BigQuery API. Manage them using the
Google Cloud console or the Resource Manager API. See `Access control with IAM
<https://docs.cloud.google.com/bigquery/docs/access-control>`_ for details.
