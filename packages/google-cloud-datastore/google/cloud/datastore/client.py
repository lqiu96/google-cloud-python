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
"""Convenience wrapper for invoking APIs/factories w/ a project."""

import os
import warnings

import google.api_core.client_options
from google.auth.credentials import AnonymousCredentials  # type: ignore
from google.cloud._helpers import _determine_default_project as _base_default_project
from google.cloud._helpers import _LocalStack
from google.cloud.client import ClientWithProject

from google.cloud.datastore import helpers
from google.cloud.datastore._http import HTTPDatastoreAPI
from google.cloud.datastore.aggregation import AggregationQuery
from google.cloud.datastore.batch import Batch
from google.cloud.datastore.entity import Entity
from google.cloud.datastore.key import Key
from google.cloud.datastore.query import Query
from google.cloud.datastore.transaction import Transaction
from google.cloud.datastore.version import __version__

try:
    from google.cloud.datastore._gapic import make_datastore_api
except ImportError:  # pragma: NO COVER
    from google.api_core import client_info as api_core_client_info

    def make_datastore_api(client):
        raise RuntimeError("No gRPC available")

    _HAVE_GRPC = False
    _CLIENT_INFO = api_core_client_info.ClientInfo(client_library_version=__version__)
else:
    from google.api_core.gapic_v1 import client_info

    _HAVE_GRPC = True
    _CLIENT_INFO = client_info.ClientInfo(
        client_library_version=__version__, gapic_version=__version__
    )


_MAX_LOOPS = 128
"""Maximum number of iterations to wait for deferred keys."""
_DATASTORE_BASE_URL = "https://datastore.googleapis.com"
"""Datastore API request URL base."""

DATASTORE_EMULATOR_HOST = "DATASTORE_EMULATOR_HOST"
"""Environment variable defining host for datastore emulator server."""
DATASTORE_DATASET = "DATASTORE_DATASET"
"""Environment variable defining default dataset ID under GCD."""
DISABLE_GRPC = "GOOGLE_CLOUD_DISABLE_GRPC"
"""Environment variable acting as flag to disable gRPC."""

_RESERVE_IDS_DEPRECATED_MESSAGE = """\
Client.reserve_ids is deprecated. Please use \
Client.reserve_ids_multi or Client.reserve_ids_sequential"""


_USE_GRPC = _HAVE_GRPC and not os.getenv(DISABLE_GRPC, False)


def _get_gcd_project():
    """Gets the GCD application ID if it can be inferred."""
    return os.getenv(DATASTORE_DATASET)


def _determine_default_project(project=None):
    """Determine default project explicitly or implicitly as fall-back.

    During :class:`Client` initialization, ``GOOGLE_CLOUD_PROJECT`` (or
    ``GCLOUD_PROJECT``) and an explicit ``credentials.project_id`` (when
    ``credentials`` is passed) take precedence before this fallback helper is
    invoked. This helper then checks ``DATASTORE_DATASET`` before falling back
    to Application Default Credentials (``google.auth.default()``).

    When called directly with ``project=None``, this helper checks environments
    in the following order:

    * ``DATASTORE_DATASET`` environment variable (for emulator testing)
    * ``GOOGLE_CLOUD_PROJECT`` environment variable
    * Google App Engine application ID
    * Google Compute Engine project ID (from metadata server)

    :type project: str
    :param project: Optional. The project to use as default.

    :rtype: str or ``NoneType``
    :returns: Default project if it can be determined.
    """
    if project is None:
        project = _get_gcd_project()

    if project is None:
        project = _base_default_project(project=project)

    return project


def _make_retry_timeout_kwargs(retry, timeout):
    """Helper: make optional retry / timeout kwargs dict."""
    kwargs = {}

    if retry is not None:
        kwargs["retry"] = retry

    if timeout is not None:
        kwargs["timeout"] = timeout

    return kwargs


def _extended_lookup(
    datastore_api,
    project,
    key_pbs,
    missing=None,
    deferred=None,
    eventual=False,
    transaction=None,
    retry=None,
    timeout=None,
    read_time=None,
    database=None,
):
    """Repeat lookup until all keys found (unless stop requested).

    Helper function for :meth:`Client.get_multi`.

    :type datastore_api:
        :class:`google.cloud.datastore._http.HTTPDatastoreAPI`
        or :class:`google.cloud.datastore_v1.services.datastore.client.DatastoreClient`
    :param datastore_api: The datastore API object used to connect
                          to datastore.

    :type project: str
    :param project: The project to make the request for.

    :type key_pbs: list of :class:`~google.cloud.datastore_v1.types.entity.Key`
    :param key_pbs: The keys to retrieve from the datastore.

    :type missing: list
    :param missing: (Optional) If a list is passed, the key-only entity
                    protobufs returned by the backend as "missing" will be
                    copied into it.

    :type deferred: list
    :param deferred: (Optional) If a list is passed, the key protobufs returned
                     by the backend as "deferred" will be copied into it.

    :type eventual: bool
    :param eventual: If False (the default), request ``STRONG`` read
                     consistency.  If True, request ``EVENTUAL`` read
                     consistency.

    :type transaction: :class:`~google.cloud.datastore.transaction.Transaction`
    :param transaction: If passed, make the request in the scope of
                        the given transaction.  Incompatible with
                        ``eventual==True`` or ``read_time``.

    :type retry: :class:`google.api_core.retry.Retry`
    :param retry:
        A retry object used to retry requests. In gRPC transport, if ``None``
        is specified, ``lookup`` requests are retried using the default
        configuration (on ``DeadlineExceeded`` and ``ServiceUnavailable``).
        In HTTP transport, no default retry is configured when ``None`` is
        specified.

    :type timeout: float
    :param timeout:
        Time, in seconds, to wait for the request to complete.
        Note that if ``retry`` is specified, the timeout applies
        to each individual attempt.

    :type read_time: datetime
    :param read_time:
        (Optional) Read time to use for read consistency. Incompatible with
        ``eventual==True`` or ``transaction``.

    :type database: str
    :param database:
        (Optional) Database from which to fetch data. Defaults to the (default) database.

    :rtype: list of :class:`~google.cloud.datastore_v1.types.entity.Entity`
    :returns: The requested entities.
    :raises: :class:`ValueError` if missing / deferred are not null or
             empty list.
    """
    if missing is not None and missing != []:
        raise ValueError("missing must be None or an empty list")

    if deferred is not None and deferred != []:
        raise ValueError("deferred must be None or an empty list")

    kwargs = _make_retry_timeout_kwargs(retry, timeout)

    results = []

    transaction_id = None
    transaction_id, new_transaction_options = helpers.get_transaction_options(
        transaction
    )
    read_options = helpers.get_read_options(
        eventual, transaction_id, read_time, new_transaction_options
    )
    loop_num = 0
    while loop_num < _MAX_LOOPS:  # loop against possible deferred.
        loop_num += 1
        request = {
            "project_id": project,
            "keys": key_pbs,
            "read_options": read_options,
        }
        helpers.set_database_id_to_request(request, database)
        lookup_response = datastore_api.lookup(
            request=request,
            **kwargs,
        )

        # set new transaction id if we just started a transaction
        if transaction and lookup_response.transaction:
            transaction._begin_with_id(lookup_response.transaction)

        # Accumulate the new results.
        results.extend(result.entity for result in lookup_response.found)

        if missing is not None:
            missing.extend(result.entity for result in lookup_response.missing)

        if deferred is not None:
            deferred.extend(lookup_response.deferred)
            break

        if len(lookup_response.deferred) == 0:
            break

        # We have deferred keys, and the user didn't ask to know about
        # them, so retry (but only with the deferred ones).
        key_pbs = lookup_response.deferred

    return results


class Client(ClientWithProject):
    """Synchronous client for Cloud Datastore operations.

    Provides entity CRUD, key factories, queries, aggregations, non-atomic
    batches, and transactions over gRPC by default using Application Default
    Credentials.

    .. doctest::

       >>> from google.cloud import datastore
       >>> client = datastore.Client()

    Lifecycle and concurrency:

    - Reuse a single ``Client`` instance across requests and threads to share
      the underlying gRPC channel. ``Client`` does not implement context
      manager methods (``__enter__`` / ``__exit__``), and inherited
      :meth:`close` closes only the HTTP session (it does not close the
      default gRPC channel).
    - Active ``with client.transaction():`` and ``with client.batch():``
      contexts are isolated per thread via a thread-local stack
      (``_LocalStack``). Individual ``Transaction``, ``Batch``, ``Query``, and
      ``Iterator`` instances are not thread-safe.
    - When using ``multiprocessing`` or pre-fork servers, instantiate
      ``Client`` after ``os.fork()`` because gRPC channels are not fork-safe.
      ``Client`` instances also cannot be pickled.

    Emulator, transport, and configuration:

    - Set ``DATASTORE_EMULATOR_HOST`` to bare ``host:port`` without an
      ``http://`` scheme (the client prepends ``http://`` automatically).
      Passing explicit ``credentials`` while ``DATASTORE_EMULATOR_HOST`` is set
      raises :class:`ValueError` (anonymous credentials are used
      automatically). ``DATASTORE_EMULATOR_HOST`` overrides
      ``client_options.api_endpoint``. The client ignores
      ``DATASTORE_PROJECT_ID`` (use ``project=``, ``GOOGLE_CLOUD_PROJECT``, or
      ``DATASTORE_DATASET`` instead).
    - When ``project`` is omitted, resolution checks in order:
      ``GOOGLE_CLOUD_PROJECT`` (or ``GCLOUD_PROJECT``), explicit
      ``credentials.project_id`` (when ``credentials`` is passed),
      ``DATASTORE_DATASET``, and finally Application Default Credentials
      (``google.auth.default()``).
    - When ``DATASTORE_EMULATOR_HOST`` is unset, ``client_options.api_endpoint``
      must include an explicit URL scheme (such as
      ``https://datastore.googleapis.com`` or ``http://localhost:8081``).
    - ``GOOGLE_CLOUD_DISABLE_GRPC`` is evaluated at module import time using
      truthiness, so any non-empty string (including ``"0"`` or ``"false"``)
      disables gRPC and switches to HTTP transport.
    - ``database`` defaults to ``None``, targeting the default database. Do not
      pass ``"(default)"``, which the Datastore API rejects.

    Retries and errors:

    - Under default gRPC transport, automatic retries (on ``DeadlineExceeded``
      and ``ServiceUnavailable``) are configured only for ``lookup``
      (``get`` / ``get_multi``), ``run_query`` (``Query.fetch``),
      ``run_aggregation_query`` (``AggregationQuery.fetch``), and
      ``reserve_ids`` (``reserve_ids*``). No default retry is configured for
      ``begin_transaction``, ``commit`` (``put``, ``put_multi``, ``delete``,
      ``delete_multi``, ``Batch.commit``, ``Transaction.commit``),
      ``rollback``, or ``allocate_ids``. Under HTTP transport, no RPCs have
      default retries.
    - Common server exceptions in ``google.api_core.exceptions`` include
      :class:`~google.api_core.exceptions.Aborted` /
      :class:`~google.api_core.exceptions.Conflict` (transaction contention,
      never retried automatically),
      :class:`~google.api_core.exceptions.FailedPrecondition` (missing
      composite index), and
      :class:`~google.api_core.exceptions.InvalidArgument` (entity/mutation
      limits or invalid request arguments).

    For service concepts and error references, see
    https://docs.cloud.google.com/datastore/docs/concepts/overview and
    https://docs.cloud.google.com/datastore/docs/concepts/errors.

    :type project: str
    :param project: (Optional) The project to pass to proxied API methods.
                    If not passed, falls back in order to
                    ``GOOGLE_CLOUD_PROJECT`` (or ``GCLOUD_PROJECT``), explicit
                    ``credentials.project_id`` (when ``credentials`` is
                    passed), ``DATASTORE_DATASET``, and Application Default
                    Credentials (``google.auth.default()``).

    :type namespace: str
    :param namespace: (Optional) namespace to pass to proxied API methods.

    :type credentials: :class:`~google.auth.credentials.Credentials`
    :param credentials: (Optional) The OAuth2 Credentials to use for this
                        client. If not passed (and if no ``_http`` object is
                        passed), falls back to the default inferred from the
                        environment. Must not be passed when
                        ``DATASTORE_EMULATOR_HOST`` is set.

    :type client_info: :class:`google.api_core.gapic_v1.client_info.ClientInfo`
                       or :class:`google.api_core.client_info.ClientInfo`
    :param client_info: (Optional) The client info used to send a user-agent
                        string along with API requests. If ``None``, then
                        default info will be used. Generally,
                        you only need to set this if you're developing your
                        own library or partner tool.

    :type client_options: :class:`~google.api_core.client_options.ClientOptions`
                          or :class:`dict`
    :param client_options: (Optional) Client options used to set user options on the
                           client. ``api_endpoint`` must include a URL scheme
                           (for example, ``https://datastore.googleapis.com``)
                           and is overridden by ``DATASTORE_EMULATOR_HOST``.

    :type database: str
    :param database: (Optional) Named database to pass to proxied API methods.
                     Defaults to ``None`` for the default database (do not pass
                     ``"(default)"``). Read-only after construction.

    :type _http: :class:`~requests.Session`
    :param _http: (Optional) HTTP object to make requests. Can be any object
                  that defines ``request()`` with the same interface as
                  :meth:`requests.Session.request`. If not passed, an
                  ``_http`` object is created that is bound to the
                  ``credentials`` for the current object.
                  This parameter should be considered private, and could
                  change in the future.

    :type _use_grpc: bool
    :param _use_grpc: (Optional) Explicitly specifies whether
                      to use the gRPC transport or HTTP. If unset,
                      falls back to the ``GOOGLE_CLOUD_DISABLE_GRPC``
                      environment variable (evaluated at import time).
                      This parameter should be considered private, and could
                      change in the future.
    """

    SCOPE = ("https://www.googleapis.com/auth/datastore",)
    """The scopes required for authenticating as a Cloud Datastore consumer."""

    def __init__(
        self,
        project=None,
        namespace=None,
        credentials=None,
        client_info=_CLIENT_INFO,
        client_options=None,
        database=None,
        _http=None,
        _use_grpc=None,
    ):
        emulator_host = os.getenv(DATASTORE_EMULATOR_HOST)

        if emulator_host is not None:
            if credentials is not None:
                raise ValueError(
                    "Explicit credentials are incompatible with the emulator"
                )
            credentials = AnonymousCredentials()

        super(Client, self).__init__(
            project=project,
            credentials=credentials,
            client_options=client_options,
            _http=_http,
        )
        self.namespace = namespace
        self._client_info = client_info
        self._client_options = client_options
        self._batch_stack = _LocalStack()
        self._datastore_api_internal = None
        self._database = database

        if _use_grpc is None:
            self._use_grpc = _USE_GRPC
        else:
            self._use_grpc = _use_grpc

        if emulator_host is not None:
            self._base_url = "http://" + emulator_host
        else:
            api_endpoint = _DATASTORE_BASE_URL
            if client_options:
                if isinstance(client_options, dict):
                    client_options = google.api_core.client_options.from_dict(
                        client_options
                    )
                if client_options.api_endpoint:
                    api_endpoint = client_options.api_endpoint
            self._base_url = api_endpoint

    @staticmethod
    def _determine_default(project):
        """Helper:  override default project detection."""
        return _determine_default_project(project)

    @property
    def base_url(self):
        """Getter for API base URL."""
        return self._base_url

    @base_url.setter
    def base_url(self, value):
        """Setter for API base URL."""
        self._base_url = value

    @property
    def database(self):
        """Getter for database"""
        return self._database

    @property
    def _datastore_api(self):
        """Getter for a wrapped API object."""
        if self._datastore_api_internal is None:
            if self._use_grpc:
                self._datastore_api_internal = make_datastore_api(self)
            else:
                self._datastore_api_internal = HTTPDatastoreAPI(self)
        return self._datastore_api_internal

    def _push_batch(self, batch):
        """Push a batch/transaction onto our stack.

        "Protected", intended for use by batch / transaction context mgrs.

        :type batch: :class:`google.cloud.datastore.batch.Batch`, or an object
                     implementing its API.
        :param batch: newly-active batch/transaction.
        """
        self._batch_stack.push(batch)

    def _pop_batch(self):
        """Pop a batch/transaction from our stack.

        "Protected", intended for use by batch / transaction context mgrs.

        :raises: IndexError if the stack is empty.
        :rtype: :class:`google.cloud.datastore.batch.Batch`, or an object
                 implementing its API.
        :returns: the top-most batch/transaction, after removing it.
        """
        return self._batch_stack.pop()

    @property
    def current_batch(self):
        """Currently-active batch, if within the scope of a Batch context manager.

        :rtype: :class:`google.cloud.datastore.batch.Batch`, or an object
                implementing its API, or ``NoneType`` (if no batch is active).
        :returns: The batch/transaction at the top of the batch stack.
        """
        return self._batch_stack.top

    @property
    def current_transaction(self):
        """Currently-active transaction, if within the scope of a Transaction
        context manager.

        :rtype: :class:`google.cloud.datastore.transaction.Transaction`, or an
                object implementing its API, or ``NoneType`` (if no transaction
                is active).
        :returns: The transaction at the top of the batch stack.
        """
        transaction = self.current_batch
        if isinstance(transaction, Transaction):
            return transaction

    def get(
        self,
        key,
        missing=None,
        deferred=None,
        transaction=None,
        eventual=False,
        retry=None,
        timeout=None,
        read_time=None,
    ):
        """Retrieve an entity from a single key (if it exists).

        Returns ``None`` if no entity exists for ``key`` (does not raise
        ``NotFound``). When called inside ``with client.transaction():`` with
        ``transaction=None``, automatically reads within
        :attr:`current_transaction`.

        .. note::

           This is just a thin wrapper over :meth:`get_multi`.
           The backend API does not make a distinction between a single key or
           multiple keys in a lookup request.

        :type key: :class:`google.cloud.datastore.key.Key`
        :param key: The key to be retrieved from the datastore.

        :type missing: list
        :param missing: (Optional) Must be ``None`` or an empty ``list``. If
                        an empty list is passed, any key-only
                        :class:`~google.cloud.datastore.entity.Entity` returned
                        as missing is appended to it.

        :type deferred: list
        :param deferred: (Optional) Must be ``None`` or an empty ``list``. If
                         an empty list is passed, lookup stops after one RPC
                         and appends any deferred
                         :class:`~google.cloud.datastore.key.Key` objects to it.

        :type transaction:
            :class:`~google.cloud.datastore.transaction.Transaction`
        :param transaction: (Optional) Transaction to use for read consistency.
                            If not passed, uses current transaction, if set.

        :type eventual: bool
        :param eventual: (Optional) Defaults to strongly consistent (False).
                         Setting True will use eventual consistency, but cannot
                         be used inside a transaction or with read_time, or will
                         raise ValueError.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. In gRPC transport, if
            ``None`` is specified, ``lookup`` requests are retried using the
            default configuration (on ``DeadlineExceeded`` and
            ``ServiceUnavailable``). In HTTP transport, no default retry is
            configured when ``None`` is specified.

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.

        :type read_time: datetime
        :param read_time: Read the entity from the specified time (may be null).
                          Cannot be used with eventual consistency or inside a
                          transaction, or will raise ValueError.

        :rtype: :class:`google.cloud.datastore.entity.Entity` or ``NoneType``
        :returns: The requested entity if it exists, or ``None`` otherwise.

        :raises: :class:`ValueError` if more than one of ``eventual==True``,
            ``transaction``, and ``read_time`` is specified.
        """
        entities = self.get_multi(
            keys=[key],
            missing=missing,
            deferred=deferred,
            transaction=transaction,
            eventual=eventual,
            retry=retry,
            timeout=timeout,
            read_time=read_time,
        )
        if entities:
            return entities[0]

    def get_multi(
        self,
        keys,
        missing=None,
        deferred=None,
        transaction=None,
        eventual=False,
        retry=None,
        timeout=None,
        read_time=None,
    ):
        """Retrieve entities, along with their attributes.

        .. note::

            - Returned entities are in arbitrary order (not guaranteed to match
              the input ``keys`` order) and omit missing keys. Index results by
              ``entity.key`` when order or presence matters.
            - Out-list parameters ``missing`` and ``deferred`` must be ``None``
              or an empty ``list`` (``[]``), or :class:`ValueError` is raised.
              ``missing`` is populated with key-only
              :class:`~google.cloud.datastore.entity.Entity` objects, whereas
              ``deferred`` is populated with
              :class:`~google.cloud.datastore.key.Key` objects.
            - When ``deferred`` is ``None``, deferred keys are retried
              automatically in a loop up to 128 RPCs. Passing ``deferred=[]``
              stops after a single RPC and leaves any remaining deferred keys
              in ``deferred`` for the caller to fetch.
            - Datastore enforces a limit of 1,000 keys per ``lookup`` RPC (this
              method does not chunk larger key lists).

        :type keys: list of :class:`google.cloud.datastore.key.Key`
        :param keys: The keys to be retrieved from the datastore.

        :type missing: list
        :param missing: (Optional) If an empty list is passed, key-only
                        :class:`~google.cloud.datastore.entity.Entity` objects
                        returned by the backend as missing are copied into it.
                        Passing a non-empty list raises :class:`ValueError`.

        :type deferred: list
        :param deferred: (Optional) If an empty list is passed,
                         :class:`~google.cloud.datastore.key.Key` objects
                         returned by the backend as deferred are copied into it
                         after a single RPC. Passing a non-empty list raises
                         :class:`ValueError`.

        :type transaction:
            :class:`~google.cloud.datastore.transaction.Transaction`
        :param transaction: (Optional) Transaction to use for read consistency.
                            If not passed, uses current transaction, if set.

        :type eventual: bool
        :param eventual: (Optional) Defaults to strongly consistent (False).
                         Setting True will use eventual consistency, but cannot
                         be used inside a transaction or with ``read_time``, or
                         will raise ValueError.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. In gRPC transport, if
            ``None`` is specified, ``lookup`` requests are retried using the
            default configuration (on ``DeadlineExceeded`` and
            ``ServiceUnavailable``). In HTTP transport, no default retry is
            configured when ``None`` is specified.

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.

        :type read_time: datetime
        :param read_time: (Optional) Read time to use for read consistency.
                          Cannot be used with eventual consistency or inside a
                          transaction, or will raise ValueError.

        :rtype: list of :class:`google.cloud.datastore.entity.Entity`
        :returns: The requested entities in arbitrary order (omitting missing
                  keys).
        :raises: :class:`ValueError` if one or more of ``keys`` has a project
                 which does not match our project, if ``missing`` or
                 ``deferred`` is not ``None`` or an empty list, or if more than
                 one of ``eventual==True``, ``transaction``, and ``read_time``
                 is specified.
        """
        if not keys:
            return []

        ids = set(key.project for key in keys)
        for current_id in ids:
            if current_id != self.project:
                raise ValueError("Keys do not match project")

        if transaction is None:
            transaction = self.current_transaction

        entity_pbs = _extended_lookup(
            datastore_api=self._datastore_api,
            project=self.project,
            key_pbs=[key.to_protobuf() for key in keys],
            eventual=eventual,
            missing=missing,
            deferred=deferred,
            transaction=transaction,
            retry=retry,
            timeout=timeout,
            read_time=read_time,
            database=self.database,
        )

        if missing is not None:
            missing[:] = [
                helpers.entity_from_protobuf(missed_pb) for missed_pb in missing
            ]

        if deferred is not None:
            deferred[:] = [
                helpers.key_from_protobuf(deferred_pb) for deferred_pb in deferred
            ]

        return [helpers.entity_from_protobuf(entity_pb._pb) for entity_pb in entity_pbs]

    def put(self, entity, retry=None, timeout=None):
        """Save an entity in the Cloud Datastore.

        .. note::

           This is a thin wrapper over :meth:`put_multi`. The backend API does
           not distinguish between a single entity or multiple entities in a
           commit request.

           - Performs a full-entity upsert replacement (or insert for partial
             keys), not a partial field merge. Properties omitted from
             ``entity`` are removed on the server.
           - Returns ``None``. If ``entity.key`` is partial, it is mutated in
             place with the server-allocated complete key upon commit.
           - Inside ``with client.batch():`` or ``with client.transaction():``,
             the write is buffered until commit and ``retry`` and ``timeout``
             are ignored.

        :type entity: :class:`google.cloud.datastore.entity.Entity`
        :param entity: The entity to be saved to the datastore.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            no default retry is configured for ``commit`` RPCs in gRPC or HTTP
            transport (requests are sent once without automatic retry). Only
            meaningful outside of another batch / transaction.

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.  Only meaningful outside of another
            batch / transaction.
        """
        self.put_multi(entities=[entity], retry=retry, timeout=timeout)

    def put_multi(self, entities, retry=None, timeout=None):
        """Save entities in the Cloud Datastore.

        .. note::

            - Performs full-entity upsert replacement (or insert for partial
              keys), not a partial field merge. Properties omitted from each
              entity are removed on the server.
            - Returns ``None``. Partial ``entity.key`` values are mutated in
              place with server-allocated complete keys upon commit.
            - Outside an active transaction, ``put_multi`` sends all entities
              in a single ``NON_TRANSACTIONAL`` commit RPC without chunking
              (not atomic across entities), subject to the 10 MiB maximum API
              request size.
            - Inside ``with client.batch():`` or
              ``with client.transaction():``, writes are buffered until commit
              and ``retry`` and ``timeout`` are ignored.

        :type entities: list of :class:`google.cloud.datastore.entity.Entity`
        :param entities: The entities to be saved to the datastore.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            no default retry is configured for ``commit`` RPCs in gRPC or HTTP
            transport (requests are sent once without automatic retry). Only
            meaningful outside of another batch / transaction.

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.  Only meaningful outside of another
            batch / transaction.

        :raises: :class:`ValueError` if ``entities`` is a single entity.
        """
        if isinstance(entities, Entity):
            raise ValueError("Pass a sequence of entities")

        if not entities:
            return

        current = self.current_batch
        in_batch = current is not None

        if not in_batch:
            current = self.batch()
            current.begin()

        for entity in entities:
            current.put(entity)

        if not in_batch:
            current.commit(retry=retry, timeout=timeout)

    def delete(self, key, retry=None, timeout=None):
        """Delete the key in the Cloud Datastore.

        .. note::

           This is a thin wrapper over :meth:`delete_multi`. The backend API
           does not distinguish between a single key or multiple keys in a
           commit request.

           - Idempotent: deleting a non-existent key succeeds without error.
           - Accepts either a complete :class:`~google.cloud.datastore.key.Key`
             or an :class:`~google.cloud.datastore.entity.Entity` (unlike
             :meth:`~google.cloud.datastore.batch.Batch.delete` and
             :meth:`~google.cloud.datastore.transaction.Transaction.delete`,
             which require a ``Key``). Partial keys raise :class:`ValueError`
             client-side.
           - Inside ``with client.batch():`` or ``with client.transaction():``,
             the delete is buffered until commit and ``retry`` and ``timeout``
             are ignored.

        :type key: :class:`google.cloud.datastore.key.Key`, :class:`google.cloud.datastore.entity.Entity`

        :param key: The key or entity to be deleted from the datastore.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            no default retry is configured for ``commit`` RPCs in gRPC or HTTP
            transport (requests are sent once without automatic retry). Only
            meaningful outside of another batch / transaction.

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.  Only meaningful outside of another
            batch / transaction.

        :raises: :class:`ValueError` if ``key`` is partial or does not match
                 the client's project or database.
        """
        self.delete_multi(keys=[key], retry=retry, timeout=timeout)

    def delete_multi(self, keys, retry=None, timeout=None):
        """Delete keys from the Cloud Datastore.

        .. note::

            - Idempotent: deleting non-existent keys succeeds without error.
            - Accepts complete :class:`~google.cloud.datastore.key.Key` or
              :class:`~google.cloud.datastore.entity.Entity` instances. Partial
              keys raise :class:`ValueError` client-side.
            - Outside an active transaction, ``delete_multi`` sends all deletes
              in a single ``NON_TRANSACTIONAL`` commit RPC without chunking
              (not atomic across keys), subject to the 10 MiB maximum API
              request size.
            - Inside ``with client.batch():`` or
              ``with client.transaction():``, deletes are buffered until commit
              and ``retry`` and ``timeout`` are ignored.

        :type keys: list of :class:`google.cloud.datastore.key.Key`, :class:`google.cloud.datastore.entity.Entity`
        :param keys: The keys or entities to be deleted from the Datastore.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            no default retry is configured for ``commit`` RPCs in gRPC or HTTP
            transport (requests are sent once without automatic retry). Only
            meaningful outside of another batch / transaction.

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.  Only meaningful outside of another
            batch / transaction.

        :raises: :class:`ValueError` if any key is partial or does not match
                 the client's project or database.
        """
        if not keys:
            return

        # Partial keys raise ValueError client-side in Batch.delete().
        current = self.current_batch
        in_batch = current is not None

        if not in_batch:
            current = self.batch()
            current.begin()

        for key in keys:
            if isinstance(key, Entity):
                # If the key is in fact an Entity, the key can be extracted.
                key = key.key
            current.delete(key)

        if not in_batch:
            current.commit(retry=retry, timeout=timeout)

    def allocate_ids(self, incomplete_key, num_ids, retry=None, timeout=None):
        """Allocate a list of IDs from a partial key.

        Requests ``num_ids`` server-generated numeric IDs from Datastore
        (unlike :meth:`reserve_ids_multi` and :meth:`reserve_ids_sequential`,
        which reserve caller-chosen numeric IDs so Datastore will not
        auto-allocate them).

        :type incomplete_key: :class:`google.cloud.datastore.key.Key`
        :param incomplete_key: Partial key to use as base for allocated IDs.

        :type num_ids: int
        :param num_ids: The number of IDs to allocate.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            no default retry is configured for ``allocate_ids`` RPCs in gRPC or
            HTTP transport (requests are sent once without automatic retry).

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.

        :rtype: list of :class:`google.cloud.datastore.key.Key`
        :returns: The (complete) keys allocated with ``incomplete_key`` as
                  root.
        :raises: :class:`ValueError` if ``incomplete_key`` is not a
                 partial key.
        """
        if not incomplete_key.is_partial:
            raise ValueError(("Key is not partial.", incomplete_key))

        incomplete_key_pb = incomplete_key.to_protobuf()
        incomplete_key_pbs = [incomplete_key_pb] * num_ids

        kwargs = _make_retry_timeout_kwargs(retry, timeout)

        request = {
            "project_id": incomplete_key.project,
            "keys": incomplete_key_pbs,
        }
        helpers.set_database_id_to_request(request, self.database)
        response_pb = self._datastore_api.allocate_ids(
            request=request,
            **kwargs,
        )
        allocated_ids = [
            allocated_key_pb.path[-1].id for allocated_key_pb in response_pb.keys
        ]
        return [
            incomplete_key.completed_key(allocated_id) for allocated_id in allocated_ids
        ]

    def key(self, *path_args, **kwargs):
        """Proxy to :class:`google.cloud.datastore.key.Key`.

        Passes this client's ``project``, ``database``, and (unless explicitly
        provided via ``namespace=``) ``namespace``.

        :rtype: :class:`~google.cloud.datastore.key.Key`
        :returns: A new key bound to this client's project and database.
        :raises: :class:`TypeError` if ``project`` or ``database`` is passed in
                 ``kwargs``.
        """
        if "project" in kwargs:
            raise TypeError("Cannot pass project")
        kwargs["project"] = self.project
        if "database" in kwargs:
            raise TypeError("Cannot pass database")
        kwargs["database"] = self.database
        if "namespace" not in kwargs:
            kwargs["namespace"] = self.namespace
        return Key(*path_args, **kwargs)

    def entity(self, key=None, exclude_from_indexes=()):
        """Proxy to :class:`google.cloud.datastore.entity.Entity`."""
        return Entity(key=key, exclude_from_indexes=exclude_from_indexes)

    def batch(self):
        """Proxy to :class:`google.cloud.datastore.batch.Batch`.

        .. warning::

            ``Batch`` commits in ``NON_TRANSACTIONAL`` mode and is NOT atomic
            or isolated across entities (unlike Firestore ``WriteBatch`` or
            :meth:`transaction`). Partial writes can persist if another
            mutation fails, and duplicate mutations for the same key fail
            server-side. Use :meth:`transaction` when all-or-nothing atomicity
            is required.

        Inside ``with client.batch():``, ``client.put()`` and
        ``client.delete()`` buffer mutations and commit on normal exit
        (discarding locally on exception). Subject to the 10 MiB maximum API
        request size per commit.

        :rtype: :class:`~google.cloud.datastore.batch.Batch`
        :returns: A new batch bound to this client.
        """
        return Batch(self)

    def transaction(self, **kwargs):
        """Proxy to :class:`google.cloud.datastore.transaction.Transaction`.

        .. warning::

            Datastore transactions do NOT automatically retry on contention
            (:class:`~google.api_core.exceptions.Aborted` in gRPC or
            :class:`~google.api_core.exceptions.Conflict` in HTTP), unlike
            ``google-cloud-firestore`` ``@firestore.transactional`` or
            ``google-cloud-ndb`` ``@ndb.transactional``. Wrap idempotent
            read-modify-write logic in a manual retry loop with exponential
            backoff, creating a fresh ``client.transaction()`` for each
            attempt.

        .. note::

            - Using ``with client.transaction():`` pushes the transaction onto
              the client's thread-local stack so ``client.get()``,
              ``client.put()``, ``client.delete()``, and ``query.fetch()``
              attach automatically, commit on normal exit, and roll back on
              exceptions.
            - Managing ``txn.begin()`` manually without ``with`` does NOT push
              onto the stack: ``client.put()`` and ``client.delete()`` run
              outside the transaction unless you call ``txn.put()`` and
              ``txn.delete()``, reads require ``transaction=txn``, and
              ``query.fetch()`` cannot attach.
            - Avoid nesting ``with client.batch():`` inside
              ``with client.transaction():``, which hides
              :attr:`current_transaction`. Exhaust lazy query iterators before
              exiting the ``with`` block.
            - Buffered writes are serialized at ``put()`` time, are not visible
              to reads in the same transaction, and are subject to the 10 MiB
              maximum transaction and API request size per commit. See
              :class:`~google.cloud.datastore.transaction.Transaction` for a
              retry-loop example and full lifecycle details.

        :param kwargs: Keyword arguments passed to
                       :class:`~google.cloud.datastore.transaction.Transaction`
                       (such as ``read_only``, ``read_time``, and
                       ``begin_later``).

        :rtype: :class:`~google.cloud.datastore.transaction.Transaction`
        :returns: A new transaction bound to this client.
        """
        return Transaction(self, **kwargs)

    def query(self, **kwargs):
        """Proxy to :class:`google.cloud.datastore.query.Query`.

        Passes this client's ``project`` and (unless explicitly provided via
        ``namespace=``) ``namespace``.

        Using query to search a datastore:

        .. testsetup:: query

            import uuid

            from google.cloud import datastore
            from google.cloud.datastore.query import PropertyFilter

            unique = str(uuid.uuid4())[0:8]
            client = datastore.Client(namespace='ns{}'.format(unique))

            def do_something_with(entity):
                pass

        .. doctest:: query

            >>> query = client.query(kind='MyKind')
            >>> query.add_filter(filter=PropertyFilter('property', '=', 'val'))
            <google.cloud.datastore.query.Query object at ...>

        Using the query iterator

        .. doctest:: query

            >>> filters = [PropertyFilter('property', '=', 'val')]
            >>> query = client.query(kind='MyKind', filters=filters)
            >>> query_iter = query.fetch()
            >>> for entity in query_iter:
            ...     do_something_with(entity)

        or manually page through results

        .. doctest:: query

            >>> query_iter = query.fetch()
            >>> pages = query_iter.pages
            >>>
            >>> first_page = next(pages)
            >>> first_page_entities = list(first_page)
            >>> query_iter.next_page_token is None
            True

        :param kwargs: Parameters for initializing an instance of
                       :class:`~google.cloud.datastore.query.Query`.

        :rtype: :class:`~google.cloud.datastore.query.Query`
        :returns: A query object.
        :raises: :class:`TypeError` if ``client`` or ``project`` is passed in
                 ``kwargs``.
        """
        if "client" in kwargs:
            raise TypeError("Cannot pass client")
        if "project" in kwargs:
            raise TypeError("Cannot pass project")
        kwargs["project"] = self.project
        if "namespace" not in kwargs:
            kwargs["namespace"] = self.namespace
        return Query(self, **kwargs)

    def aggregation_query(self, query, **kwargs):
        """Proxy to :class:`google.cloud.datastore.aggregation.AggregationQuery`.

        Using aggregation_query to count over a query:

        .. testsetup:: aggregation_query

            import uuid

            from google.cloud import datastore
            from google.cloud.datastore.aggregation import CountAggregation

            unique = str(uuid.uuid4())[0:8]
            client = datastore.Client(namespace='ns{}'.format(unique))

            def do_something_with(result):
                pass

        .. doctest:: aggregation_query

            >>> query = client.query(kind='MyKind')
            >>> aggregation_query = client.aggregation_query(query)
            >>> aggregation_query.count(alias='total')
            <google.cloud.datastore.aggregation.AggregationQuery object at ...>
            >>> aggregation_query.fetch()
            <google.cloud.datastore.aggregation.AggregationResultIterator object at ...>

        Adding an aggregation to the aggregation_query

        .. doctest:: aggregation_query

            >>> query = client.query(kind='MyKind')
            >>> aggregation_query = client.aggregation_query(query)
            >>> aggregation_query.add_aggregation(CountAggregation(alias='total'))
            >>> aggregation_query.fetch()
            <google.cloud.datastore.aggregation.AggregationResultIterator object at ...>

        Adding multiple aggregations to the aggregation_query

        .. doctest:: aggregation_query

            >>> query = client.query(kind='MyKind')
            >>> aggregation_query = client.aggregation_query(query)
            >>> total_count = CountAggregation(alias='total')
            >>> all_count = CountAggregation(alias='all')
            >>> aggregation_query.add_aggregations([total_count, all_count])
            >>> aggregation_query.fetch()
            <google.cloud.datastore.aggregation.AggregationResultIterator object at ...>


        Using the aggregation_query iterator

        .. doctest:: aggregation_query

            >>> query = client.query(kind='MyKind')
            >>> aggregation_query = client.aggregation_query(query)
            >>> aggregation_query.count(alias='total')
            <google.cloud.datastore.aggregation.AggregationQuery object at ...>
            >>> aggregation_query_iter = aggregation_query.fetch()
            >>> for aggregation_results in aggregation_query_iter:
            ...     for aggregation_result in aggregation_results:
            ...         do_something_with(aggregation_result)

        or manually page through results

        .. doctest:: aggregation_query

            >>> aggregation_query_iter = aggregation_query.fetch()
            >>> pages = aggregation_query_iter.pages
            >>>
            >>> first_page = next(pages)
            >>> first_page_results = list(first_page)
            >>> aggregation_query_iter.next_page_token is None
            True

        :type query: :class:`~google.cloud.datastore.query.Query`
        :param query: The query to aggregate over.

        :param kwargs: Parameters for initializing an instance of
                       :class:`~google.cloud.datastore.aggregation.AggregationQuery`.

        :rtype: :class:`~google.cloud.datastore.aggregation.AggregationQuery`
        :returns: An AggregationQuery object.
        """
        return AggregationQuery(self, query, **kwargs)

    def reserve_ids_sequential(self, complete_key, num_ids, retry=None, timeout=None):
        """Reserve a list of IDs sequentially from a complete key.

        Reserves the caller-chosen key passed as ``complete_key`` as well as
        additional keys derived by incrementing the last ID in the path of
        ``complete_key`` sequentially to obtain ``num_ids`` keys, preventing
        Datastore from auto-allocating those IDs (unlike :meth:`allocate_ids`,
        which generates new IDs).

        :type complete_key: :class:`google.cloud.datastore.key.Key`
        :param complete_key:
            Complete key to use as base for reserved IDs. Key must use a
            numeric ID and not a string name.

        :type num_ids: int
        :param num_ids: The number of IDs to reserve.

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. In gRPC transport, if
            ``None`` is specified, ``reserve_ids`` requests are retried using
            the default configuration (on ``DeadlineExceeded`` and
            ``ServiceUnavailable``). In HTTP transport, no default retry is
            configured when ``None`` is specified.

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.

        :rtype: ``NoneType``
        :returns: None
        :raises: :class:`ValueError` if ``complete_key`` is not a complete key,
                 if ``complete_key`` uses a string name instead of a numeric
                 ID, or if ``num_ids`` is not an integer.
        """
        if complete_key.is_partial:
            raise ValueError(("Key is not Complete.", complete_key))

        if complete_key.id is None:
            raise ValueError(("Key must use numeric id.", complete_key))

        if not isinstance(num_ids, int):
            raise ValueError(("num_ids is not a valid integer.", num_ids))

        key_class = type(complete_key)
        namespace = complete_key._namespace
        project = complete_key._project
        database = complete_key._database
        flat_path = list(complete_key._flat_path[:-1])
        start_id = complete_key._flat_path[-1]

        key_pbs = []
        for id in range(start_id, start_id + num_ids):
            path = flat_path + [id]
            key = key_class(
                *path, project=project, database=database, namespace=namespace
            )
            key_pbs.append(key.to_protobuf())

        kwargs = _make_retry_timeout_kwargs(retry, timeout)
        request = {
            "project_id": complete_key.project,
            "keys": key_pbs,
        }
        helpers.set_database_id_to_request(request, self.database)
        self._datastore_api.reserve_ids(
            request=request,
            **kwargs,
        )
        return None

    def reserve_ids(self, complete_key, num_ids, retry=None, timeout=None):
        """Reserve a list of IDs sequentially from a complete key.

        DEPRECATED. Alias for :meth:`reserve_ids_sequential`.

        Please use either :meth:`reserve_ids_multi` (recommended) or
        :meth:`reserve_ids_sequential`.
        """
        warnings.warn(_RESERVE_IDS_DEPRECATED_MESSAGE, DeprecationWarning)
        return self.reserve_ids_sequential(
            complete_key, num_ids, retry=retry, timeout=timeout
        )

    def reserve_ids_multi(self, complete_keys, retry=None, timeout=None):
        """Reserve IDs from a list of complete keys.

        Recommended over :meth:`reserve_ids` for reserving specific
        caller-chosen numeric IDs so Datastore will not auto-allocate them
        (unlike :meth:`allocate_ids`, which generates new IDs). Each key must
        be complete and ``complete_keys`` must be non-empty. Unlike
        :meth:`reserve_ids_sequential`, string-named keys are accepted by the
        API without raising, though Datastore only auto-allocates numeric IDs.

        :type complete_keys: list of :class:`google.cloud.datastore.key.Key`
        :param complete_keys:
            Non-empty sequence of complete keys to reserve (intended for
            numeric IDs).

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. In gRPC transport, if
            ``None`` is specified, ``reserve_ids`` requests are retried using
            the default configuration (on ``DeadlineExceeded`` and
            ``ServiceUnavailable``). In HTTP transport, no default retry is
            configured when ``None`` is specified.

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.

        :rtype: ``NoneType``
        :returns: None
        :raises: :class:`ValueError` if any of ``complete_keys`` is not a
                 complete key, or :class:`IndexError` if ``complete_keys`` is
                 empty.
        """
        for complete_key in complete_keys:
            if complete_key.is_partial:
                raise ValueError(("Key is not Complete.", complete_key))

        kwargs = _make_retry_timeout_kwargs(retry, timeout)
        key_pbs = [key.to_protobuf() for key in complete_keys]
        request = {
            "project_id": complete_keys[0].project,
            "keys": key_pbs,
        }
        helpers.set_database_id_to_request(request, complete_keys[0].database)

        self._datastore_api.reserve_ids(
            request=request,
            **kwargs,
        )

        return None
