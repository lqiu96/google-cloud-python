# Copyright 2014 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Create / interact with Google Cloud Datastore transactions."""

from google.protobuf import timestamp_pb2

from google.cloud.datastore.batch import Batch
from google.cloud.datastore.helpers import set_database_id_to_request
from google.cloud.datastore_v1.types import TransactionOptions


def _make_retry_timeout_kwargs(retry, timeout):
    """Helper: make optional retry / timeout kwargs dict."""
    kwargs = {}

    if retry is not None:
        kwargs["retry"] = retry

    if timeout is not None:
        kwargs["timeout"] = timeout

    return kwargs


class Transaction(Batch):
    """An abstraction representing datastore Transactions.

    Transactions can be used to build up a bulk mutation and ensure all
    or none succeed (transactionally).

    .. warning::

        Datastore transactions do NOT automatically retry on contention
        (:class:`~google.api_core.exceptions.Aborted` in gRPC transport or
        :class:`~google.api_core.exceptions.Conflict` in HTTP transport),
        unlike ``google-cloud-firestore`` ``@firestore.transactional`` or
        ``google-cloud-ndb`` ``@ndb.transactional``. Wrap idempotent
        read-modify-write logic in an application retry loop with exponential
        backoff, creating a fresh ``client.transaction()`` for each attempt:

        .. code-block:: python

            import time
            from google.api_core import exceptions

            for attempt in range(5):
                try:
                    with client.transaction():
                        entity = client.get(key)
                        entity["count"] += 1
                        client.put(entity)
                    break
                except (exceptions.Aborted, exceptions.Conflict):
                    if attempt == 4:
                        raise
                    time.sleep(0.1 * (2**attempt))

    .. note::

        - Entering ``with client.transaction():`` calls :meth:`begin` (unless
          ``begin_later=True``) and pushes the transaction onto the client's
          thread-local stack. Inside the block, ``client.get()``,
          ``client.get_multi()``, ``client.put()``, ``client.put_multi()``,
          ``client.delete()``, ``client.delete_multi()``, ``query.fetch()``,
          and ``aggregation_query.fetch()`` attach automatically, commit on
          normal exit, and roll back on exceptions.
        - Managing ``txn.begin()`` manually without ``with`` does NOT push the
          transaction onto the thread-local stack. In that mode,
          ``client.put()`` and ``client.delete()`` execute immediately outside
          the transaction unless you call ``txn.put()`` and ``txn.delete()``,
          reads require passing ``transaction=txn`` explicitly, and
          ``query.fetch()`` or ``aggregation_query.fetch()`` cannot attach to
          the transaction.
        - Avoid nesting ``with client.batch():`` inside
          ``with client.transaction():``. Pushing a ``Batch`` hides
          ``client.current_transaction`` (making it ``None``), so
          ``client.get()`` and ``query.fetch()`` silently run outside the
          transaction while ``client.put()`` and ``client.delete()`` target the
          non-transactional batch. Nesting two ``with client.transaction():``
          blocks creates independent server transactions and risks
          self-contention.
        - Queries and aggregation queries are lazy and read
          ``client.current_transaction`` only when pages are fetched. Exhaust
          iterators inside the ``with`` block rather than iterating after
          exiting it.
        - Writes are buffered client-side, serialized at ``put()`` time, and
          sent only on :meth:`commit`. Mutating an entity after ``put()`` does
          not change what is written, and buffered writes are not visible to
          reads within the same transaction.
        - ``client.put()`` and ``client.delete()`` ignore ``retry`` and
          ``timeout`` keyword arguments when called inside
          ``with client.transaction():``. Context manager exit calls
          :meth:`commit` with default arguments, so call
          ``txn.commit(retry=..., timeout=...)`` explicitly if you need custom
          commit retry or timeout settings.
        - :meth:`delete` (inherited from
          :class:`~google.cloud.datastore.batch.Batch`) requires a complete
          :class:`~google.cloud.datastore.key.Key` and does not accept
          :class:`~google.cloud.datastore.entity.Entity` instances.
        - Datastore enforces a maximum of 500 entity mutations per commit.
          ``begin_transaction``, ``commit``, and ``rollback`` have no default
          retry policy configured in gRPC or HTTP transport. See
          https://docs.cloud.google.com/datastore/docs/concepts/transactions
          and https://docs.cloud.google.com/datastore/docs/concepts/errors.

    For example, the following snippet of code will put the two ``save``
    operations (either ``insert`` or ``upsert``) into the same
    mutation, and execute those within a transaction:

    .. testsetup:: txn

        import uuid

        from google.cloud import datastore

        unique = str(uuid.uuid4())[0:8]
        client = datastore.Client(namespace='ns{}'.format(unique))

    .. doctest:: txn

        >>> entity1 = datastore.Entity(client.key('EntityKind', 1234))
        >>> entity2 = datastore.Entity(client.key('EntityKind', 2345))
        >>> with client.transaction():
        ...     client.put_multi([entity1, entity2])

    Because it derives from :class:`~google.cloud.datastore.batch.Batch`,
    :class:`Transaction` also provides :meth:`put` and :meth:`delete` methods:

    .. doctest:: txn

       >>> with client.transaction() as xact:
       ...     xact.put(entity1)
       ...     xact.delete(entity2.key)

    By default, the transaction is rolled back if the transaction block
    exits with an error:

    .. doctest:: txn

        >>> def do_some_work():
        ...    return
        >>> class SomeException(Exception):
        ...    pass
        >>> with client.transaction():
        ...     do_some_work()
        ...     raise SomeException  # rolls back
        Traceback (most recent call last):
          ...
        SomeException

    If the transaction block exits without an exception, it will commit
    by default.

    .. warning::

        Inside a transaction, automatically assigned IDs for
        entities will not be available at save time!  That means, if you
        try:

        .. doctest:: txn

            >>> with client.transaction():
            ...     thing1 = datastore.Entity(key=client.key('Thing'))
            ...     client.put(thing1)

       ``thing1`` won't have a complete key until the transaction is
       committed.

       Once you exit the transaction (or call :meth:`commit`), the
       automatically generated ID will be assigned to the entity:

       .. doctest:: txn

          >>> with client.transaction():
          ...     thing2 = datastore.Entity(key=client.key('Thing'))
          ...     client.put(thing2)
          ...     print(thing2.key.is_partial)  # There is no ID on this key.
          ...
          True
          >>> print(thing2.key.is_partial)  # There *is* an ID.
          False

    If you don't want to use the context manager you can initialize a
    transaction manually:

    .. doctest:: txn

       >>> transaction = client.transaction()
       >>> transaction.begin()
       >>>
       >>> thing3 = datastore.Entity(key=client.key('Thing'))
       >>> transaction.put(thing3)
       >>>
       >>> transaction.commit()

    .. testcleanup:: txn

        with client.batch() as batch:
            batch.delete(client.key('EntityKind', 1234))
            batch.delete(client.key('EntityKind', 2345))
            batch.delete(thing1.key)
            batch.delete(thing2.key)
            batch.delete(thing3.key)

    :type client: :class:`google.cloud.datastore.client.Client`
    :param client: the client used to connect to datastore.

    :type read_only: bool
    :param read_only: indicates the transaction is read only. Calling
                      :meth:`put` on a read-only transaction raises
                      :class:`RuntimeError`.

    :type read_time: datetime
    :param read_time: (Optional) Time at which the transaction reads entities.
                      Only allowed when ``read_only=True``.

    :type begin_later: bool
    :param begin_later: (Optional) If ``True``, starting the transaction is
                        deferred until the first ``get`` or ``get_multi`` RPC
                        (via ``new_transaction`` read options) or until
                        :meth:`commit` if mutations are queued without reads.
                        If a ``begin_later=True`` transaction ends with no
                        reads and no mutations, :meth:`commit` aborts locally
                        without sending an RPC. If ``False``, the transaction
                        is started as soon as the context manager is entered.
                        ``self.begin()`` can also be called manually to begin
                        the transaction at any time. Default is ``False``.

    :raises: :class:`ValueError` if ``read_time`` is specified when
             ``read_only=False``.
    """

    _status = None

    def __init__(self, client, read_only=False, read_time=None, begin_later=False):
        super(Transaction, self).__init__(client)
        self._id = None
        self._begin_later = begin_later

        if read_only:
            if read_time is not None:
                read_time_pb = timestamp_pb2.Timestamp()
                read_time_pb.FromDatetime(read_time)
                options = TransactionOptions(
                    read_only=TransactionOptions.ReadOnly(read_time=read_time_pb)
                )
            else:
                options = TransactionOptions(read_only=TransactionOptions.ReadOnly())
        else:
            if read_time is not None:
                raise ValueError("read_time is only allowed in read only transaction.")
            else:
                options = TransactionOptions()

        self._options = options

    @property
    def id(self):
        """Getter for the transaction ID.

        Returns ``bytes`` while the transaction is active. Returns ``None``
        before :meth:`begin` (or before the first lookup when
        ``begin_later=True``) and after :meth:`commit` or :meth:`rollback`.

        :rtype: bytes or None
        :returns: The ID of the current transaction, or None if not active.
        """
        return self._id

    def current(self):
        """Return the topmost transaction.

        .. note::

            If the topmost element on the stack is not a transaction,
            returns None.

        :rtype: :class:`google.cloud.datastore.transaction.Transaction` or None
        :returns: The current transaction (if any are active).
        """
        top = super(Transaction, self).current()
        if isinstance(top, Transaction):
            return top

    def begin(self, retry=None, timeout=None):
        """Begins a transaction.

        This method is called automatically when entering a ``with``
        statement (unless ``begin_later=True``), however it can be called
        explicitly if you do not want to use a context manager. If used
        outside a context manager, the transaction is not pushed onto the
        client's thread-local stack, so ``client.get()`` and
        ``client.get_multi()`` require passing ``transaction=txn`` explicitly,
        writes must use ``txn.put()`` and ``txn.delete()`` (calling
        ``client.put()`` or ``client.delete()`` runs outside the transaction),
        ``query.fetch()`` cannot attach to the transaction, and
        :meth:`commit` or :meth:`rollback` must be called explicitly.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            no default retry is configured for ``begin_transaction`` RPCs in
            gRPC or HTTP transport (requests are sent once without automatic
            retry).

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.

        :raises: :class:`ValueError` if the transaction has already begun.
        """
        super(Transaction, self).begin()

        kwargs = _make_retry_timeout_kwargs(retry, timeout)

        request = {
            "project_id": self.project,
            "transaction_options": self._options,
        }
        set_database_id_to_request(request, self._client.database)

        try:
            response_pb = self._client._datastore_api.begin_transaction(
                request=request, **kwargs
            )
            self._id = response_pb.transaction
        except:  # noqa: E722 do not use bare except, specify exception instead
            self._status = self._ABORTED
            raise

    def _begin_with_id(self, transaction_id):
        """
        Attach newly created transaction to an existing transaction ID.

        This is used when begin_later is True, when the first lookup request
        associated with this transaction creates a new transaction ID.

        :type transaction_id: bytes
        :param transaction_id: ID of the transaction to attach to.
        """
        if self._status is not self._INITIAL:
            raise ValueError("Transaction already begun.")
        self._id = transaction_id
        self._status = self._IN_PROGRESS

    def rollback(self, retry=None, timeout=None):
        """Rolls back the current transaction.

        This is called automatically when exiting a ``with`` statement with
        an exception. If the transaction has not started yet (for example,
        with ``begin_later=True``), it aborts locally without sending an RPC.

        This method has necessary side-effects:

        - Sets the current transaction's ID to None.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            no default retry is configured for ``rollback`` RPCs in gRPC or
            HTTP transport (requests are sent once without automatic retry).

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.
        """
        # if transaction has not started, abort it
        if self._status == self._INITIAL:
            self._status = self._ABORTED
            self._id = None
            return None

        kwargs = _make_retry_timeout_kwargs(retry, timeout)

        try:
            # No need to use the response it contains nothing.
            request = {
                "project_id": self.project,
                "transaction": self._id,
            }

            set_database_id_to_request(request, self._client.database)
            self._client._datastore_api.rollback(request=request, **kwargs)
        finally:
            super(Transaction, self).rollback()
            # Clear our own ID in case this gets accidentally reused.
            self._id = None

    def commit(self, retry=None, timeout=None):
        """Commits the transaction.

        This is called automatically upon exiting a ``with`` statement without
        an exception (exiting with an exception calls :meth:`rollback`). It
        can also be called explicitly if you do not want to use a context
        manager.

        When ``begin_later=True`` and the transaction has not yet started,
        commits with queued mutations call :meth:`begin` first, while an empty
        transaction with no reads and no mutations aborts locally without
        sending an RPC. Contention raises
        :class:`~google.api_core.exceptions.Aborted` (gRPC) or
        :class:`~google.api_core.exceptions.Conflict` (HTTP) and is not
        automatically retried.

        This method has necessary side-effects:

        - Sets the current transaction's ID to None.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            no default retry is configured for ``commit`` RPCs in gRPC or HTTP
            transport (requests are sent once without automatic retry).

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.
        """
        # if transaction has not begun, either begin now, or abort if empty
        if self._status == self._INITIAL:
            if not self._mutations:
                self._status = self._ABORTED
                self._id = None
                return None
            else:
                self.begin(retry=retry, timeout=timeout)

        kwargs = _make_retry_timeout_kwargs(retry, timeout)

        try:
            super(Transaction, self).commit(**kwargs)
        finally:
            # Clear our own ID in case this gets accidentally reused.
            self._id = None

    def put(self, entity):
        """Adds an entity to be committed.

        Ensures the transaction is not marked read-only. Writes are buffered
        and serialized at ``put()`` time, are sent only on :meth:`commit`, and
        are not visible to reads within the same transaction. Please see
        documentation at :meth:`~google.cloud.datastore.batch.Batch.put`.

        :type entity: :class:`~google.cloud.datastore.entity.Entity`
        :param entity: the entity to be saved.

        :raises: :class:`RuntimeError` if the transaction
                 is marked ReadOnly
        """
        if "read_only" in self._options:
            raise RuntimeError("Transaction is read only")
        else:
            super(Transaction, self).put(entity)

    def __enter__(self):
        if not self._begin_later:
            self.begin()
        self._client._push_batch(self)
        return self

    def _allow_mutations(self):
        """
        Mutations can be added to a transaction if it is in IN_PROGRESS state,
        or if it is in INITIAL state and the begin_later flag is set.
        """
        return self._status == self._IN_PROGRESS or (
            self._begin_later and self._status == self._INITIAL
        )
