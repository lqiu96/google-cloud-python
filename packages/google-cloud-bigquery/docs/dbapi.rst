DB-API Reference
~~~~~~~~~~~~~~~~

.. automodule:: google.cloud.bigquery.dbapi
  :members:
  :show-inheritance:


DB-API Query-Parameter Syntax
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The BigQuery DB-API uses the ``pyformat`` `parameter style
<https://peps.python.org/pep-0249/#paramstyle>`_ for both unnamed/positional
parameters (``%s``, translated to BigQuery ``?``) and named parameters
(``%(name)s``, translated to BigQuery ``@`name` ``).

An example of a query using unnamed parameters::

  insert into people (name, income) values (%s, %s)

and using named parameters::

  insert into people (name, income) values (%(name)s, %(income)s)

Providing explicit type information
-----------------------------------

BigQuery requires type information for parameters. The BigQuery
DB-API can usually determine parameter types for parameters based on
provided values. Sometimes, however, types cannot be determined (for
example when ``None`` or an empty sequence ``[]`` is passed) or are determined
incorrectly (for example when passing a floating-point value to a numeric
column). Passing ``None`` or an empty sequence without an explicit type raises
:class:`~google.cloud.bigquery.dbapi.ProgrammingError`. Passing a ``dict``
without an explicit ``:struct<...>`` type raises :class:`NotImplementedError`.

The BigQuery DB-API provides an extended parameter syntax. For named
parameters, a BigQuery type is provided after the name separated by a
colon, as in::

  insert into people (name, income) values (%(name:string)s, %(income:numeric)s)

For unnamed parameters, use the named syntax with a type, but no
name, as in::

  insert into people (name, income) values (%(:string)s, %(:numeric)s)

Providing type information is the *only* way to pass ``STRUCT`` data::

  cursor.execute(
    "insert into points (point) values (%(:struct<x float64, y float64>)s)",
    [{"x": 10, "y": 20}],
    )

PEP 249 Notes and Limitations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Transactions**: BigQuery DB-API does not manage multi-statement client
  transactions. :meth:`~google.cloud.bigquery.dbapi.Connection.commit` is a
  no-op (raising :class:`~google.cloud.bigquery.dbapi.ProgrammingError` if the
  connection is closed), and ``Connection`` does not implement ``rollback()``.
- **Context managers**: Neither :class:`~google.cloud.bigquery.dbapi.Connection`
  nor :class:`~google.cloud.bigquery.dbapi.Cursor` supports the ``with``
  statement. Call ``close()`` explicitly when finished.
- **Thread safety**: ``threadsafety = 2``. Threads may share the module and
  ``Connection`` instances, but not ``Cursor`` instances.
- **Array size**: ``Cursor.arraysize``
  defaults to ``None`` (rather than ``1``) and is passed as ``page_size`` when
  fetching query results. When ``arraysize`` is ``None``,
  :meth:`~google.cloud.bigquery.dbapi.Cursor.fetchmany` defaults to fetching 1
  row.
- **Percent-sign escaping**: ``%%`` in SQL strings is unescaped to ``%`` even
  when ``parameters`` is ``None`` or empty.
- **Row type**: ``fetchone()``, ``fetchmany()``, and ``fetchall()`` return
  :class:`~google.cloud.bigquery.table.Row` objects rather than plain Python
  tuples.
- **Batch execution**: :meth:`~google.cloud.bigquery.dbapi.Cursor.executemany`
  executes each parameter set sequentially as a separate query and does not
  accept ``job_config``.
- **Job inspection**: Accessing
  :attr:`~google.cloud.bigquery.dbapi.Cursor.query_job` issues a live
  ``client.get_job()`` API request each time and returns ``None`` when a
  stateless query did not create a job.
- **Error wrapping**: :meth:`~google.cloud.bigquery.dbapi.Cursor.execute` wraps
  :class:`~google.cloud.exceptions.GoogleCloudError` in
  :class:`~google.cloud.bigquery.dbapi.DatabaseError`. Fetch methods
  (``fetchone()``, ``fetchmany()``, ``fetchall()``) do not wrap API errors and
  raise underlying exceptions directly, such as
  :class:`~google.api_core.exceptions.PermissionDenied` when
  ``prefer_bqstorage_client=True`` and the caller lacks
  ``bigquery.readsessions.create`` permission.
