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

"""Create / interact with Google Cloud Datastore queries."""

import abc
import base64
import warnings
from abc import ABC

from google.api_core import page_iterator
from google.cloud._helpers import _ensure_tuple_or_list

from google.cloud.datastore import helpers
from google.cloud.datastore.key import Key
from google.cloud.datastore.query_profile import ExplainMetrics, QueryExplainError
from google.cloud.datastore_v1.types import entity as entity_pb2
from google.cloud.datastore_v1.types import query as query_pb2

_NOT_FINISHED = query_pb2.QueryResultBatch.MoreResultsType.NOT_FINISHED
_NO_MORE_RESULTS = query_pb2.QueryResultBatch.MoreResultsType.NO_MORE_RESULTS

_FINISHED = (
    _NO_MORE_RESULTS,
    query_pb2.QueryResultBatch.MoreResultsType.MORE_RESULTS_AFTER_LIMIT,
    query_pb2.QueryResultBatch.MoreResultsType.MORE_RESULTS_AFTER_CURSOR,
    query_pb2.QueryResultBatch.MoreResultsType.MORE_RESULTS_TYPE_UNSPECIFIED,  # received when explain_options(analyze=False)
)

KEY_PROPERTY_NAME = "__key__"


class BaseFilter(ABC):
    """Base class for Filters"""

    @abc.abstractmethod
    def build_pb(self, container_pb=None):
        """Build the protobuf representation based on values in the Filter."""


class PropertyFilter(BaseFilter):
    """Class representation of a Property Filter.

    Import from ``google.cloud.datastore.query`` (do not mix with
    ``google.cloud.datastore_v1.PropertyFilter``). Pass instances to
    :meth:`Query.add_filter` using the ``filter=`` keyword argument.
    """

    def __init__(self, property_name, operator, value):
        if property_name == KEY_PROPERTY_NAME and not isinstance(value, Key):
            raise ValueError('Invalid key: "%s"' % value)
        if Query.OPERATORS.get(operator) is None:
            error_message = 'Invalid expression: "%s"' % (operator,)
            choices_message = "Please use one of: =, <, <=, >, >=, !=, IN, NOT_IN."
            raise ValueError(error_message, choices_message)
        self.property_name = property_name
        self.operator = operator
        self.value = value

    def build_pb(self, container_pb=None):
        """Build the protobuf representation based on values in the Property Filter."""
        container_pb.op = Query.OPERATORS.get(self.operator)
        container_pb.property.name = self.property_name
        if self.property_name == KEY_PROPERTY_NAME:
            key_pb = self.value.to_protobuf()
            container_pb.value.key_value.CopyFrom(key_pb._pb)
        else:
            helpers._set_protobuf_value(container_pb.value, self.value)
        return container_pb

    def __repr__(self):
        return f"<{self.property_name} {self.operator} '{self.value}'>"


class BaseCompositeFilter(BaseFilter):
    """Base class for a Composite Filter. (either OR or AND)."""

    def __init__(
        self,
        operation=query_pb2.CompositeFilter.Operator.OPERATOR_UNSPECIFIED,
        filters=None,
    ):
        self.operation = operation
        if filters is None:
            self.filters = []
        else:
            self.filters = filters

    def __repr__(self):
        repr = f"op: {self.operation}\nFilters:"
        for filter in self.filters:
            repr += f"\n\t{filter}"
        return repr

    def build_pb(self, container_pb=None):
        """Build the protobuf representation based on values in the Composite Filter."""
        container_pb.op = self.operation
        for filter in self.filters:
            if isinstance(filter, PropertyFilter):
                child_pb = container_pb.filters.add().property_filter
            elif isinstance(filter, BaseCompositeFilter):
                child_pb = container_pb.filters.add().composite_filter
            else:
                # unpack to legacy filter
                property_name, operator, value = filter
                filter = PropertyFilter(property_name, operator, value)
                child_pb = container_pb.filters.add().property_filter
            filter.build_pb(container_pb=child_pb)
        return container_pb


class Or(BaseCompositeFilter):
    """Class representation of an OR Filter.

    Import from ``google.cloud.datastore.query`` and pass to
    :meth:`Query.add_filter` using the ``filter=`` keyword argument.
    """

    def __init__(self, filters):
        super().__init__(
            operation=query_pb2.CompositeFilter.Operator.OR, filters=filters
        )


class And(BaseCompositeFilter):
    """Class representation of an AND Filter.

    Import from ``google.cloud.datastore.query`` and pass to
    :meth:`Query.add_filter` using the ``filter=`` keyword argument.
    """

    def __init__(self, filters):
        super().__init__(
            operation=query_pb2.CompositeFilter.Operator.AND, filters=filters
        )


class Query(object):
    """A Query against the Cloud Datastore.

    This class serves as an abstraction for creating a query over data
    stored in the Cloud Datastore.

    :type client: :class:`google.cloud.datastore.client.Client`
    :param client: The client used to connect to Datastore.

    :type kind: str
    :param kind: The kind to query.

    :type project: str
    :param project:
        (Optional) The project associated with the query.  If not passed, uses
        the client's value.

    :type namespace: str
    :param namespace:
        (Optional) The namespace to which to restrict results.  If not passed,
        uses the client's value (it is NOT inherited from
        ``ancestor.namespace``).

    :type ancestor: :class:`~google.cloud.datastore.key.Key`
    :param ancestor:
        (Optional) key of the ancestor to which this query's results are
        restricted. Passing ``ancestor=`` does NOT inherit
        ``ancestor.namespace``: you must also pass
        ``namespace=ancestor.namespace`` (or initialize the client with that
        namespace), otherwise the query runs in the client's namespace and
        fails or returns empty results.

    :type filters: sequence of :class:`BaseFilter` or tuple[str, str, Any]
    :param filters: Property or composite filters applied by this query. Prefer
        :class:`PropertyFilter`, :class:`And`, or :class:`Or` instances
        imported from ``google.cloud.datastore.query``. Legacy 3-tuples
        ``(property_name, operator, value)`` are also accepted but emit a
        :class:`UserWarning`.

    :type projection: sequence of string
    :param projection:  fields returned as part of query results.

    :type order: sequence of string
    :param order:  field names used to order query results. Prepend ``-``
                   to a field name to sort it in descending order.

    :type distinct_on: sequence of string
    :param distinct_on: field names used to group query results.

    :type explain_options: :class:`~google.cloud.datastore.ExplainOptions`
    :param explain_options: (Optional) Options to enable query profiling for
        this query. When set, ``explain_metrics`` will be available on the
        iterator returned by ``query.fetch()``.

    .. note::

        - Queries combining filters across multiple properties, or filtering
          and ordering on different properties, generally require a composite
          index. Missing indexes raise
          :class:`~google.api_core.exceptions.FailedPrecondition`.
        - See https://docs.cloud.google.com/datastore/docs/concepts/queries
          and https://docs.cloud.google.com/datastore/docs/concepts/indexes
          for query restrictions and index requirements.
    """

    OPERATORS = {
        "<=": query_pb2.PropertyFilter.Operator.LESS_THAN_OR_EQUAL,
        ">=": query_pb2.PropertyFilter.Operator.GREATER_THAN_OR_EQUAL,
        "<": query_pb2.PropertyFilter.Operator.LESS_THAN,
        ">": query_pb2.PropertyFilter.Operator.GREATER_THAN,
        "=": query_pb2.PropertyFilter.Operator.EQUAL,
        "!=": query_pb2.PropertyFilter.Operator.NOT_EQUAL,
        "IN": query_pb2.PropertyFilter.Operator.IN,
        "NOT_IN": query_pb2.PropertyFilter.Operator.NOT_IN,
    }
    """Mapping of operator strings and their protobuf equivalents."""

    def __init__(
        self,
        client,
        kind=None,
        project=None,
        namespace=None,
        ancestor=None,
        filters=(),
        projection=(),
        order=(),
        distinct_on=(),
        explain_options=None,
    ):
        self._client = client
        self._kind = kind

        if project:
            self._project = project
        elif hasattr(client, "project"):
            self._project = client.project
        else:
            self._project = None

        if namespace:
            self._namespace = namespace
        elif hasattr(client, "namespace"):
            self._namespace = client.namespace
        else:
            self._namespace = None

        self._explain_options = explain_options
        self._ancestor = ancestor
        self._filters = []

        # Verify filters passed in.
        for filter in filters:
            if isinstance(filter, BaseFilter):
                self.add_filter(filter=filter)
            else:
                property_name, operator, value = filter
                self.add_filter(property_name, operator, value)
        self._projection = _ensure_tuple_or_list("projection", projection)
        self._order = _ensure_tuple_or_list("order", order)
        self._distinct_on = _ensure_tuple_or_list("distinct_on", distinct_on)

    @property
    def project(self):
        """Get the project for this Query.

        :rtype: str
        :returns: The project for the query.
        """
        return self._project or self._client.project

    @property
    def namespace(self):
        """This query's namespace

        :rtype: str or None
        :returns: the namespace assigned to this query
        """
        return self._namespace or self._client.namespace

    @namespace.setter
    def namespace(self, value):
        """Update the query's namespace.

        :type value: str
        """
        if not isinstance(value, str):
            raise ValueError("Namespace must be a string")
        self._namespace = value

    @property
    def kind(self):
        """Get the Kind of the Query.

        :rtype: str
        :returns: The kind for the query.
        """
        return self._kind

    @kind.setter
    def kind(self, value):
        """Update the Kind of the Query.

        :type value: str
        :param value: updated kind for the query.

        .. note::

           The protobuf specification allows for ``kind`` to be repeated,
           but the current implementation returns an error if more than
           one value is passed.  If the back-end changes in the future to
           allow multiple values, this method will be updated to allow passing
           either a string or a sequence of strings.
        """
        if not isinstance(value, str):
            raise TypeError("Kind must be a string")
        self._kind = value

    @property
    def ancestor(self):
        """The ancestor key for the query.

        :rtype: :class:`~google.cloud.datastore.key.Key` or None
        :returns: The ancestor for the query.
        """
        return self._ancestor

    @ancestor.setter
    def ancestor(self, value):
        """Set the ancestor for the query

        :type value: :class:`~google.cloud.datastore.key.Key`
        :param value: the new ancestor key
        """
        if not isinstance(value, Key):
            raise TypeError("Ancestor must be a Key")
        self._ancestor = value

    @ancestor.deleter
    def ancestor(self):
        """Remove the ancestor for the query."""
        self._ancestor = None

    @property
    def filters(self):
        """Filters set on the query.

        :rtype: list of :class:`BaseFilter` or tuple[str, str, Any]
        :returns: The filters set on the query as :class:`BaseFilter`
            instances (or legacy ``(property_name, operator, value)`` tuples).
        """
        return self._filters[:]

    def add_filter(
        self,
        property_name=None,
        operator=None,
        value=None,
        *,
        filter=None,
    ):
        """Filter the query based on a property name, operator and a value.

        Import :class:`PropertyFilter`, :class:`And`, and :class:`Or` from
        ``google.cloud.datastore.query`` (they are not re-exported at
        ``google.cloud.datastore``). Expressions take the form of::

          .add_filter(
            filter=PropertyFilter('<property>', '<operator>', <value>)
          )

        where property is a property stored on the entity in the datastore
        and operator is one of ``OPERATORS``
        (``=``, ``<``, ``<=``, ``>``, ``>=``, ``!=``, ``IN``, ``NOT_IN``).

        Both AND and OR operations are supported by passing a
        :class:`BaseCompositeFilter` (:class:`And` or :class:`Or`) object to
        the ``filter`` keyword parameter::

           .add_filter(
               filter=And(
                   [
                       PropertyFilter('<property>', '<operator>', <value>),
                       PropertyFilter('<property>', '<operator>', <value>)

                   ]
               )
           )

           .add_filter(
               filter=Or(
                   [
                       PropertyFilter('<property>', '<operator>', <value>),
                       PropertyFilter('<property>', '<operator>', <value>)
                   ]
               )
           )

        .. note::

            - Always pass :class:`PropertyFilter`, :class:`And`, or
              :class:`Or` via the ``filter=`` keyword argument. Passing them
              positionally raises :class:`ValueError`, and positional
              ``(property_name, operator, value)`` arguments emit a
              :class:`UserWarning`.
            - Repeated ``add_filter()`` calls combine with logical ``AND``.
            - Multi-property filters or inequality filters combined with sort
              orders may require a composite index (otherwise Datastore raises
              :class:`~google.api_core.exceptions.FailedPrecondition`).

        .. testsetup:: query-filter

            import uuid

            from google.cloud import datastore
            from google.cloud.datastore.query import PropertyFilter

            client = datastore.Client()

        .. doctest:: query-filter

            >>> query = client.query(kind='Person')
            >>> query = query.add_filter(filter=PropertyFilter('name', '=', 'James'))
            >>> query = query.add_filter(filter=PropertyFilter('age', '>', 50))

        :type property_name: str
        :param property_name: (Deprecated) A property name for positional use.

        :type operator: str
        :param operator: (Deprecated) One of ``=``, ``<``, ``<=``, ``>``,
                         ``>=``, ``!=``, ``IN``, ``NOT_IN``.

        :type value: :class:`int`, :class:`str`, :class:`bool`,
                     :class:`float`, :class:`NoneType`,
                     :class:`datetime.datetime`,
                     :class:`google.cloud.datastore.key.Key`
        :param value: (Deprecated) The value to filter on.

        :type filter: :class:`BaseCompositeFilter` or :class:`PropertyFilter`
        :param filter: An instance of :class:`BaseFilter`, such as
                       :class:`PropertyFilter`, :class:`And`, or :class:`Or`.

        :rtype: :class:`~google.cloud.datastore.query.Query`
        :returns: This query object (for chaining).

        :raises: :class:`ValueError` if ``operator`` is not one of the
                 specified values, if a filter names ``'__key__'`` with a
                 non-``Key`` value, if a :class:`BaseFilter` is passed
                 positionally, or if both positional arguments and ``filter``
                 are provided.
        """
        if isinstance(property_name, PropertyFilter):
            raise ValueError(
                "PropertyFilter object must be passed using keyword argument 'filter'"
            )
        if isinstance(property_name, BaseCompositeFilter):
            raise ValueError(
                "'Or' and 'And' objects must be passed using keyword argument 'filter'"
            )

        if property_name is not None and operator is not None:
            if filter is not None:
                raise ValueError(
                    "Can't pass in both the positional arguments and 'filter' at the same time"
                )

            if property_name == KEY_PROPERTY_NAME and not isinstance(value, Key):
                raise ValueError('Invalid key: "%s"' % value)

            if self.OPERATORS.get(operator) is None:
                error_message = 'Invalid expression: "%s"' % (operator,)
                choices_message = "Please use one of: =, <, <=, >, >=, !=, IN, NOT_IN."
                raise ValueError(error_message, choices_message)

            warnings.warn(
                "Detected filter using positional arguments. Prefer using the 'filter' keyword argument instead.",
                UserWarning,
                stacklevel=2,
            )
            self._filters.append((property_name, operator, value))

        if isinstance(filter, BaseFilter):
            self._filters.append(filter)

        return self

    @property
    def projection(self):
        """Field names returned by the query.

        .. warning::

            Projection queries return partial
            :class:`~google.cloud.datastore.entity.Entity` instances containing
            only the projected properties and ``key``. Passing an unmodified
            projection entity to
            :meth:`~google.cloud.datastore.client.Client.put` fails with
            :class:`~google.api_core.exceptions.InvalidArgument` (projection
            meaning 18 is rejected on write). Modifying projected fields or
            copying them into a new ``Entity`` and calling ``put()`` succeeds
            and deletes unprojected properties on the server.

        :rtype: sequence of string
        :returns: Names of fields in query results.
        """
        return self._projection[:]

    @projection.setter
    def projection(self, projection):
        """Set the fields returned by the query.

        .. warning::

            Projection queries return partial
            :class:`~google.cloud.datastore.entity.Entity` objects. Unmodified
            projection entities fail
            :meth:`~google.cloud.datastore.client.Client.put` with
            :class:`~google.api_core.exceptions.InvalidArgument`, while
            modifying projected fields or copying into a new ``Entity`` and
            calling ``put()`` overwrites the entity and deletes unprojected
            properties on the server.

        :type projection: str or sequence of strings
        :param projection: Each value is a string giving the name of a
                           property to be included in the projection query.
        """
        if isinstance(projection, str):
            projection = [projection]
        self._projection[:] = projection

    def keys_only(self):
        """Set the projection to include only keys.

        .. warning::

            A keys-only query yields empty
            :class:`~google.cloud.datastore.entity.Entity` shells with only
            ``entity.key`` populated (not bare
            :class:`~google.cloud.datastore.key.Key` objects). Never pass
            yielded shells to :meth:`~google.cloud.datastore.client.Client.put`
            or all stored properties on those entities will be deleted.
        """
        self._projection[:] = [KEY_PROPERTY_NAME]

    def key_filter(self, key, operator="="):
        """Filter on a key.

        :type key: :class:`google.cloud.datastore.key.Key`
        :param key: The key to filter on.

        :type operator: str
        :param operator: (Optional) One of ``=``, ``<``, ``<=``, ``>``, ``>=``, ``!=``, ``IN``, ``NOT_IN``.
                         Defaults to ``=``.
        """
        self.add_filter(KEY_PROPERTY_NAME, operator, key)

    @property
    def order(self):
        """Names of fields used to sort query results.

        :rtype: sequence of string
        :returns: The order(s) set on the query.
        """
        return self._order[:]

    @order.setter
    def order(self, value):
        """Set the fields used to sort query results.

        Sort fields will be applied in the order specified.

        :type value: str or sequence of strings
        :param value: Each value is a string giving the name of the
                      property on which to sort, optionally preceded by a
                      hyphen (-) to specify descending order.
                      Omitting the hyphen implies ascending order.
        """
        if isinstance(value, str):
            value = [value]
        self._order[:] = value

    @property
    def distinct_on(self):
        """Names of fields used to group query results.

        :rtype: sequence of string
        :returns: The "distinct on" fields set on the query.
        """
        return self._distinct_on[:]

    @distinct_on.setter
    def distinct_on(self, value):
        """Set fields used to group query results.

        :type value: str or sequence of strings
        :param value: Each value is a string giving the name of a
                      property to use to group results together.
        """
        if isinstance(value, str):
            value = [value]
        self._distinct_on[:] = value

    def fetch(
        self,
        limit=None,
        offset=0,
        start_cursor=None,
        end_cursor=None,
        client=None,
        eventual=False,
        retry=None,
        timeout=None,
        read_time=None,
    ):
        """Execute the Query and return an iterator for the matching entities.

        .. note::

            - ``fetch()`` is lazy (no RPC is sent until iterated) and returns a
              single-pass :class:`~collections.abc.Iterator`
              (``next(query_iter)`` works directly, and calling ``iter()`` a
              second time after iteration starts raises :class:`ValueError`).
              Inside a ``with client.transaction():`` block, exhaust the
              iterator before exiting so the query executes within the
              transaction.
            - ``start_cursor`` and ``end_cursor`` are URL-safe base64-encoded
              :class:`bytes` (or ASCII :class:`str`) matching
              ``query_iter.next_page_token`` (not raw protobuf bytes).
            - ``query_iter.pages`` yields server-sized RPC batches rather than
              ``limit``-sized pages when ``limit`` exceeds one server batch.
              For cursor pagination with ``limit=N``, exhaust the iterator
              (for example, ``entities = list(query_iter)``) and pass
              ``query_iter.next_page_token`` as ``start_cursor`` on the next
              call while ``next_page_token`` is not ``None``. The token is
              cleared to ``None`` when the server response reports
              ``NO_MORE_RESULTS`` (note that ``next_page_token`` may remain
              populated when ``limit`` matches the exact remaining entity count
              until a subsequent empty fetch returns ``NO_MORE_RESULTS``).
            - ``offset`` is ignored when ``start_cursor`` is provided. Large
              offsets loop internally in chunks of 1,000 skipped results and
              incur billed small/index reads, so prefer cursors for pagination.
            - Queries using :attr:`projection` or :meth:`keys_only` yield
              partial or empty :class:`~google.cloud.datastore.entity.Entity`
              shells (not bare :class:`~google.cloud.datastore.key.Key`
              objects). Unmodified projection entities fail ``client.put()``
              with :class:`~google.api_core.exceptions.InvalidArgument`, while
              ``keys_only()`` shells (or modified or copied projection
              entities) overwrite the entity and delete unprojected or all
              stored properties.
            - When ``explain_options=ExplainOptions(analyze=True)`` is set on
              the query, exhaust the returned iterator before reading
              :attr:`Iterator.explain_metrics`.

        For example:

        .. testsetup:: query-fetch

            import uuid

            from google.cloud import datastore
            from google.cloud.datastore.query import PropertyFilter
            unique = str(uuid.uuid4())[0:8]
            client = datastore.Client(namespace='ns{}'.format(unique))


        .. doctest:: query-fetch

            >>> andy = datastore.Entity(client.key('Person', 1234))
            >>> andy['name'] = 'Andy'
            >>> sally = datastore.Entity(client.key('Person', 2345))
            >>> sally['name'] = 'Sally'
            >>> bobby = datastore.Entity(client.key('Person', 3456))
            >>> bobby['name'] = 'Bobby'
            >>> client.put_multi([andy, sally, bobby])
            >>> query = client.query(kind='Person')
            >>> result = list(query.add_filter(filter=PropertyFilter('name', '=', 'Sally')).fetch())
            >>> result
            [<Entity('Person', 2345) {'name': 'Sally'}>]

        .. testcleanup:: query-fetch

            client.delete(andy.key)
            client.delete(sally.key)
            client.delete(bobby.key)

        :type limit: int
        :param limit: (Optional) limit passed through to the iterator.

        :type offset: int
        :param offset: (Optional) offset passed through to the iterator.

        :type start_cursor: bytes or str
        :param start_cursor: (Optional) URL-safe base64 cursor (such as
                             ``iterator.next_page_token``) to start reading
                             results from. When set, ``offset`` is ignored.

        :type end_cursor: bytes or str
        :param end_cursor: (Optional) URL-safe base64 cursor to stop reading
                           results at.

        :type client: :class:`google.cloud.datastore.client.Client`
        :param client: (Optional) client used to connect to datastore.
                       If not supplied, uses the query's value.

        :type eventual: bool
        :param eventual: (Optional) Defaults to strongly consistent (``False``)
                                    in Firestore in Datastore mode. Setting
                                    ``True`` requests eventual consistency,
                                    which cannot be used inside a transaction
                                    or with ``read_time`` (raises
                                    :class:`ValueError`).

        :type retry: :class:`google.api_core.retry.Retry`
        :param retry:
            A retry object used to retry requests. If ``None`` is specified,
            requests will be retried using a default configuration in gRPC
            transport (no default retry in HTTP transport).

        :type timeout: float
        :param timeout:
            Time, in seconds, to wait for the request to complete.
            Note that if ``retry`` is specified, the timeout applies
            to each individual attempt.

        :type read_time: datetime
        :param read_time:
            (Optional) use read_time read consistency, cannot be used inside a
            transaction or with eventual consistency, or will raise ValueError.

        :rtype: :class:`Iterator`
        :returns: The iterator for the query.
        """
        if client is None:
            client = self._client

        return Iterator(
            self,
            client,
            limit=limit,
            offset=offset,
            start_cursor=start_cursor,
            end_cursor=end_cursor,
            eventual=eventual,
            retry=retry,
            timeout=timeout,
            read_time=read_time,
        )


class Iterator(page_iterator.Iterator):
    """Represent the state of a given execution of a Query.

    This class is a single-pass :class:`~collections.abc.Iterator`
    (``next(query_iter)`` works directly, and calling ``iter()`` a second time
    after iteration starts raises :class:`ValueError`). After exhausting the
    iterator, ``next_page_token`` holds the URL-safe base64 cursor to resume
    pagination, or ``None`` once the server response reports
    ``NO_MORE_RESULTS`` (it may remain populated when ``limit`` matches the
    exact remaining count until a subsequent empty fetch returns
    ``NO_MORE_RESULTS``). Note that ``pages`` yields server-sized RPC batches
    rather than ``limit``-sized pages.

    :type query: :class:`~google.cloud.datastore.query.Query`
    :param query: Query object holding permanent configuration (i.e.
                  things that don't change with each page in
                  a results set).

    :type client: :class:`~google.cloud.datastore.client.Client`
    :param client: The client used to make a request.

    :type limit: int
    :param limit: (Optional) Limit the number of results returned.

    :type offset: int
    :param offset: (Optional) Offset used to begin a query. Ignored when
                   ``start_cursor`` is set.

    :type start_cursor: bytes or str
    :param start_cursor: (Optional) URL-safe base64 cursor to begin paging
                         through query results.

    :type end_cursor: bytes or str
    :param end_cursor: (Optional) URL-safe base64 cursor to end paging through
                       query results.

    :type eventual: bool
    :param eventual: (Optional) Defaults to strongly consistent (``False``).
                                Setting ``True`` will use eventual consistency,
                                but cannot be used inside a transaction or
                                with ``read_time``, otherwise will raise
                                :class:`ValueError`.

    :type retry: :class:`google.api_core.retry.Retry`
    :param retry:
        A retry object used to retry requests. If ``None`` is specified,
        requests will be retried using a default configuration in gRPC
        transport (no default retry in HTTP transport).

    :type timeout: float
    :param timeout:
        Time, in seconds, to wait for the request to complete.
        Note that if ``retry`` is specified, the timeout applies
        to each individual attempt.

    :type read_time: datetime
    :param read_time: (Optional) Runs the query with read time consistency.
                      Cannot be used with eventual consistency or inside a
                      transaction, otherwise will raise ValueError.
    """

    next_page_token = None

    def __init__(
        self,
        query,
        client,
        limit=None,
        offset=None,
        start_cursor=None,
        end_cursor=None,
        eventual=False,
        retry=None,
        timeout=None,
        read_time=None,
    ):
        super(Iterator, self).__init__(
            client=client,
            item_to_value=_item_to_entity,
            page_token=start_cursor,
            max_results=limit,
        )
        self._query = query
        self._offset = offset
        self._end_cursor = end_cursor
        self._eventual = eventual
        self._retry = retry
        self._timeout = timeout
        self._read_time = read_time
        # The attributes below will change over the life of the iterator.
        self._explain_metrics = None
        self._more_results = True
        self._skipped_results = 0

    def _build_protobuf(self):
        """Build a query protobuf.

        Relies on the current state of the iterator.

        :rtype:
            :class:`google.cloud.datastore_v1.types.Query`
        :returns: The query protobuf object for the current
                  state of the iterator.
        """
        pb = _pb_from_query(self._query)

        start_cursor = self.next_page_token
        if start_cursor is not None:
            pb.start_cursor = base64.urlsafe_b64decode(start_cursor)

        end_cursor = self._end_cursor
        if end_cursor is not None:
            pb.end_cursor = base64.urlsafe_b64decode(end_cursor)

        if self.max_results is not None:
            pb.limit = self.max_results - self.num_results

        if start_cursor is None and self._offset is not None:
            # NOTE: We don't need to add an offset to the request protobuf
            #       if we are using an existing cursor, because the offset
            #       is only relative to the start of the result set, not
            #       relative to each page (this method is called per-page)
            pb.offset = self._offset

        return pb

    def _process_query_results(self, response_pb):
        """Process the response from a datastore query.

        :type response_pb: :class:`google.cloud.datastore_v1.types.RunQueryResponse`
        :param response_pb: The protobuf response from a ``runQuery`` request.

        :rtype: iterable
        :returns: The next page of entity results.
        :raises ValueError: If ``more_results`` is an unexpected value.
        """
        self._skipped_results = response_pb.batch.skipped_results
        if response_pb.batch.more_results == _NO_MORE_RESULTS:
            self.next_page_token = None
        else:
            self.next_page_token = base64.urlsafe_b64encode(
                response_pb.batch.end_cursor
            )
        self._end_cursor = None

        if response_pb.batch.more_results == _NOT_FINISHED:
            self._more_results = True
        elif response_pb.batch.more_results in _FINISHED:
            self._more_results = False
        else:
            raise ValueError("Unexpected value returned for `more_results`.")

        return [result.entity for result in response_pb.batch.entity_results]

    def _next_page(self):
        """Get the next page in the iterator.

        :rtype: :class:`~google.api_core.page_iterator.Page`
        :returns: The next page in the iterator (or :data:`None` if
                  there are no pages left).
        """
        if not self._more_results:
            return None

        new_transaction_options = None
        transaction_id, new_transaction_options = helpers.get_transaction_options(
            self.client.current_transaction
        )
        read_options = helpers.get_read_options(
            self._eventual, transaction_id, self._read_time, new_transaction_options
        )

        partition_id = entity_pb2.PartitionId(
            project_id=self._query.project,
            database_id=self.client.database,
            namespace_id=self._query.namespace,
        )

        kwargs = {}

        if self._retry is not None:
            kwargs["retry"] = self._retry

        if self._timeout is not None:
            kwargs["timeout"] = self._timeout

        request = {
            "project_id": self._query.project,
            "partition_id": partition_id,
            "read_options": read_options,
            "query": self._build_protobuf(),
        }
        if self._query._explain_options:
            request["explain_options"] = self._query._explain_options._to_dict()

        helpers.set_database_id_to_request(request, self.client.database)

        response_pb = None

        while response_pb is None or (
            response_pb.batch.more_results == _NOT_FINISHED
            and response_pb.batch.skipped_results < request["query"].offset
        ):
            if response_pb is not None:
                # We haven't finished processing. A likely reason is we haven't
                # skipped all of the results yet. Don't return any results.
                # Instead, rerun query, adjusting offsets. Datastore doesn't process
                # more than 1000 skipped results in a query.
                new_query_pb = query_pb2.Query()
                new_query_pb._pb.CopyFrom(request["query"]._pb)  # copy for testability
                new_query_pb.start_cursor = response_pb.batch.end_cursor
                new_query_pb.offset -= response_pb.batch.skipped_results
                request["query"] = new_query_pb

            response_pb = self.client._datastore_api.run_query(
                request=request.copy(), **kwargs
            )
            # capture explain metrics if present in response
            # should only be present in last response, and only if explain_options was set
            if response_pb and response_pb.explain_metrics:
                self._explain_metrics = ExplainMetrics._from_pb(
                    response_pb.explain_metrics
                )

        entity_pbs = self._process_query_results(response_pb)
        return page_iterator.Page(self, entity_pbs, self.item_to_value)

    @property
    def explain_metrics(self) -> ExplainMetrics:
        """
        Get the metrics associated with the query execution.

        Metrics are only available when ``explain_options`` is set on the
        query at construction time.

        - When ``ExplainOptions(analyze=False)`` (the default), no query
          results are returned. Accessing ``explain_metrics`` sends the plan
          request on demand and populates ``plan_summary`` only (reading
          ``execution_stats`` raises :class:`QueryExplainError`).
        - When ``ExplainOptions(analyze=True)``, both ``plan_summary`` and
          ``execution_stats`` are returned, but you must exhaust the iterator
          first (for example, ``list(iterator)``) before accessing
          ``explain_metrics`` or :class:`QueryExplainError` is raised.

        See https://docs.cloud.google.com/datastore/docs/query-explain-analyze.

        :rtype: :class:`~google.cloud.datastore.query_profile.ExplainMetrics`
        :returns: The metrics associated with the query execution.
        :raises: :class:`~google.cloud.datastore.query_profile.QueryExplainError`
            if ``explain_options`` is not set or the query has not finished
            iterating when ``analyze=True``.
        """
        if self._explain_metrics is not None:
            return self._explain_metrics
        elif self._query._explain_options is None:
            raise QueryExplainError("explain_options not set on query.")
        elif self._query._explain_options.analyze is False:
            # we need to run the query to get the explain_metrics
            # analyze=False only returns explain_metrics, no results
            self._next_page()
            if self._explain_metrics is not None:
                return self._explain_metrics
        raise QueryExplainError(
            "explain_metrics not available until query is complete."
        )


def _pb_from_query(query):
    """Convert a Query instance to the corresponding protobuf.

    :type query: :class:`Query`
    :param query: The source query.

    :rtype: :class:`google.cloud.datastore_v1.types.Query`
    :returns: A protobuf that can be sent to the protobuf API.  N.b. that
              it does not contain "in-flight" fields for ongoing query
              executions (cursors, offset, limit).
    """
    pb = query_pb2.Query()

    for projection_name in query.projection:
        projection = query_pb2.Projection()
        projection.property.name = projection_name
        pb.projection.append(projection)

    if query.kind:
        kind = query_pb2.KindExpression()
        kind.name = query.kind
        pb.kind.append(kind)

    composite_filter = pb.filter.composite_filter
    composite_filter.op = query_pb2.CompositeFilter.Operator.AND

    for filter in query.filters:
        if isinstance(filter, BaseCompositeFilter):
            pb_to_add = pb.filter.composite_filter.filters._pb.add().composite_filter
        elif isinstance(filter, PropertyFilter):
            pb_to_add = pb.filter.composite_filter.filters._pb.add().property_filter
        else:
            property_name, operator, value = filter
            filter = PropertyFilter(property_name, operator, value)
            pb_to_add = pb.filter.composite_filter.filters._pb.add().property_filter
        filter.build_pb(container_pb=pb_to_add)

    if query.ancestor:
        ancestor_pb = query.ancestor.to_protobuf()

        # Filter on __key__ HAS_ANCESTOR == ancestor.
        ancestor_filter = composite_filter.filters._pb.add().property_filter
        ancestor_filter.property.name = KEY_PROPERTY_NAME
        ancestor_filter.op = query_pb2.PropertyFilter.Operator.HAS_ANCESTOR
        ancestor_filter.value.key_value.CopyFrom(ancestor_pb._pb)

    if not composite_filter.filters:
        pb._pb.ClearField("filter")

    for prop in query.order:
        property_order = query_pb2.PropertyOrder()

        if prop.startswith("-"):
            property_order.property.name = prop[1:]
            property_order.direction = property_order.Direction.DESCENDING
        else:
            property_order.property.name = prop
            property_order.direction = property_order.Direction.ASCENDING

        pb.order.append(property_order)

    for distinct_on_name in query.distinct_on:
        ref = query_pb2.PropertyReference()
        ref.name = distinct_on_name
        pb.distinct_on.append(ref)

    return pb


# pylint: disable=unused-argument
def _item_to_entity(iterator, entity_pb):
    """Convert a raw protobuf entity to the native object.

    :type iterator: :class:`~google.api_core.page_iterator.Iterator`
    :param iterator: The iterator that is currently in use.

    :type entity_pb:
        :class:`google.cloud.datastore_v1.types.Entity`
    :param entity_pb: An entity protobuf to convert to a native entity.

    :rtype: :class:`~google.cloud.datastore.entity.Entity`
    :returns: The next entity in the page.
    """
    return helpers.entity_from_protobuf(entity_pb)


# pylint: enable=unused-argument
