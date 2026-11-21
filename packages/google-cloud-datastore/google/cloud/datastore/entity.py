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

"""Class for representing a single entity in the Cloud Datastore."""

from google.cloud._helpers import _ensure_tuple_or_list


class Entity(dict):
    """Entities are akin to rows in a relational database

    An entity storing the actual instance of data.

    Each entity is officially represented with a
    :class:`~google.cloud.datastore.key.Key`, however it is possible that
    you might create an entity with only a partial key (that is, a key
    with a kind, and possibly a parent, but without an ID).  In such a
    case, the datastore service will automatically assign an ID to the
    partial key.

    Entities in this API act like dictionaries with attached metadata
    (``key`` and ``exclude_from_indexes``). ``Entity`` itself has no
    persistence or delete methods. Persist or delete entities through
    :class:`~google.cloud.datastore.client.Client`,
    :class:`~google.cloud.datastore.batch.Batch`, or
    :class:`~google.cloud.datastore.transaction.Transaction`.

    Entities are mutable and subclass :class:`dict`. Standard dictionary
    copying operations such as ``dict(entity)``, ``{**entity}``, or
    ``entity.copy()`` return a plain :class:`dict` and drop ``key``,
    ``exclude_from_indexes``, and internal meaning metadata. You can duplicate
    an entity by constructing a new ``Entity`` with a different key and
    copying its properties and ``exclude_from_indexes``.

    Use :meth:`~google.cloud.datastore.client.Client.get` to retrieve an
    existing entity:

    .. testsetup:: entity-ctor

        import uuid

        from google.cloud import datastore

        unique = str(uuid.uuid4())[0:8]
        client = datastore.Client(namespace='ns{}'.format(unique))

        entity = datastore.Entity(client.key('EntityKind', 1234))
        entity['property'] = 'value'
        client.put(entity)

    .. doctest:: entity-ctor

        >>> key = client.key('EntityKind', 1234)
        >>> client.get(key)
        <Entity('EntityKind', 1234) {'property': 'value'}>

    You can set values on the entity just like you would on any
    other dictionary.

    .. doctest:: entity-ctor

        >>> entity['age'] = 20
        >>> entity['name'] = 'JJ'

    .. testcleanup:: entity-ctor

        client.delete(entity.key)

    However, not all types are allowed as a value for a Google Cloud Datastore
    entity. The following basic types are supported by the API:

    * :class:`datetime.datetime`
    * :class:`~google.cloud.datastore.key.Key`
    * :class:`bool`
    * :class:`float`
    * :class:`int` (signed 64-bit integer)
    * :class:`str`
    * :class:`bytes`
    * :class:`~google.cloud.datastore.helpers.GeoPoint` (imported from
      ``google.cloud.datastore.helpers``)
    * :data:`None`

    In addition, three container types are supported:

    * :class:`list` (nested lists such as ``[[1, 2]]`` are accepted when
      indexed, but fail with
      :class:`~google.api_core.exceptions.InvalidArgument` if added to
      ``exclude_from_indexes`` because inner array values cannot be marked
      excluded)
    * :class:`~google.cloud.datastore.entity.Entity`
    * :class:`dict` (treated like an embedded ``Entity`` without a key or
      ``exclude_from_indexes``)

    Each entry in a list must be one of the supported value types (basic or
    container) and each value in an
    :class:`~google.cloud.datastore.entity.Entity` must as well. In
    this case an :class:`~google.cloud.datastore.entity.Entity` **as a
    container** acts as a :class:`dict`, but also has the special annotations
    of ``key`` and ``exclude_from_indexes``.

    Unsupported Python types such as :class:`datetime.date`,
    :class:`decimal.Decimal`, :class:`tuple`, and :class:`set` raise
    :class:`ValueError` during serialization.

    And you can treat an entity like a regular Python dictionary:

    .. testsetup:: entity-dict

        from google.cloud import datastore

        entity = datastore.Entity()
        entity['age'] = 20
        entity['name'] = 'JJ'

    .. doctest:: entity-dict

        >>> sorted(entity.keys())
        ['age', 'name']
        >>> sorted(entity.items())
        [('age', 20), ('name', 'JJ')]

    .. note::

        - All properties are indexed by default. Indexed :class:`str` (UTF-8
          encoded, stored in ``string_value``) and :class:`bytes` (stored raw
          in ``blob_value``) values are limited to 1,500 bytes. Larger values
          (up to 1,048,487 bytes, or 1 MiB minus 89 bytes, within the 1 MiB
          minus 4 bytes total entity limit) must be listed in
          ``exclude_from_indexes``.
        - The ``exclude_from_indexes`` constructor argument requires a
          :class:`tuple` or :class:`list` (passing a :class:`set` or
          :class:`str` raises :class:`TypeError`). On an existing or fetched
          entity, ``entity.exclude_from_indexes`` is a mutable :class:`set`
          that can be updated in place (for example,
          ``entity.exclude_from_indexes.add("body")``).
        - ``exclude_from_indexes`` matches top-level property names only.
          Dotted paths such as ``"nested.large_text"`` have no effect. Adding
          ``"nested"`` sets ``exclude_from_indexes`` on the outer embedded
          entity value only, which prevents indexing of nested values up to
          1,500 bytes, but nested strings or bytes over 1,500 bytes in a plain
          :class:`dict` still fail with
          :class:`~google.api_core.exceptions.InvalidArgument`. To store
          nested values over 1,500 bytes or to exclude individual nested
          fields while indexing siblings, store an embedded
          ``Entity(exclude_from_indexes=("large_text",))`` instead of a plain
          :class:`dict`.
        - Excluding a :class:`list` property marks every element unindexed.
          Empty lists (``[]``) lose their exclusion flag on round-trip, and
          reading an entity written by another client whose array mixes
          indexed and unindexed elements raises :class:`ValueError`.
        - Naive :class:`datetime.datetime` values are treated as UTC (not
          local time), aware datetimes are converted to UTC, and sub-microsecond
          nanoseconds are truncated on write. Values read back as
          timezone-aware UTC
          ``proto.datetime_helpers.DatetimeWithNanoseconds`` objects (not
          ``google.api_core.datetime_helpers.DatetimeWithNanoseconds``).
          Ordering comparisons (``<``, ``<=``, ``>``, ``>=``) against a naive
          ``datetime`` raise :class:`TypeError`, while equality comparisons
          (``==`` / ``!=``) silently return ``False`` / ``True``.
        - See https://docs.cloud.google.com/datastore/docs/concepts/entities,
          https://docs.cloud.google.com/datastore/docs/concepts/limits, and
          https://docs.cloud.google.com/datastore/docs/concepts/indexes#unindexed_properties
          for entity, value type, and indexing limits.

    :type key: :class:`google.cloud.datastore.key.Key`
    :param key: Optional key to be set on entity.

    :type exclude_from_indexes: tuple of str or list of str
    :param exclude_from_indexes: Top-level property names whose values are not
                                 to be indexed for this entity.
    """

    def __init__(self, key=None, exclude_from_indexes=()):
        super(Entity, self).__init__()
        self.key = key
        self.exclude_from_indexes = set(
            _ensure_tuple_or_list("exclude_from_indexes", exclude_from_indexes)
        )
        """Names of fields which are *not* to be indexed for this entity."""
        # NOTE: This will be populated when parsing a protobuf in
        #       google.cloud.datastore.helpers.entity_from_protobuf.
        self._meanings = {}

    def __eq__(self, other):
        """Compare two entities for equality.

        Entities compare equal if their keys, ``exclude_from_indexes`` sets,
        internal ``_meanings`` mappings, and properties all compare equal.
        Because partial keys never compare equal to any key (including
        themselves), an entity with a partial key never compares equal to
        another entity or to itself.

        :rtype: bool
        :returns: True if the entities compare equal, else False.
        """
        if not isinstance(other, Entity):
            return NotImplemented

        return (
            self.key == other.key
            and self.exclude_from_indexes == other.exclude_from_indexes
            and self._meanings == other._meanings
            and super(Entity, self).__eq__(other)
        )

    def __ne__(self, other):
        """Compare two entities for inequality.

        Entities compare equal if their keys, ``exclude_from_indexes`` sets,
        internal ``_meanings`` mappings, and properties all compare equal.
        Entities with partial keys always compare unequal.

        :rtype: bool
        :returns: False if the entities compare equal, else True.
        """
        return not self == other

    @property
    def kind(self):
        """Get the kind of the current entity.

        .. note::

            This relies entirely on the :class:`google.cloud.datastore.key.Key`
            set on the entity.  That means that we're not storing the kind
            of the entity at all, just the properties and a pointer to a
            Key which knows its Kind.
        """
        if self.key:
            return self.key.kind

    @property
    def id(self):
        """Get the ID of the current entity.

        .. note::

            This relies entirely on the :class:`google.cloud.datastore.key.Key`
            set on the entity.  That means that we're not storing the ID
            of the entity at all, just the properties and a pointer to a
            Key which knows its ID.
        """
        if self.key is None:
            return None
        else:
            return self.key.id

    def __repr__(self):
        if self.key:
            return "<Entity%s %s>" % (
                self.key._flat_path,
                super(Entity, self).__repr__(),
            )
        else:
            return "<Entity %s>" % (super(Entity, self).__repr__(),)
