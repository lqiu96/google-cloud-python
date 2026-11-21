# -*- coding: utf-8 -*-
# Copyright 2026 Google LLC
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
#
"""Google Cloud Datastore API client library.

This package provides the hand-written synchronous veneer for Cloud Datastore
(and Firestore in Datastore mode).

Choosing the right package:

- Use ``google.cloud.datastore`` (this veneer) for idiomatic synchronous
  entities, keys, queries, aggregations, batches, and transactions. Top-level
  exports include ``Client``, ``Entity``, ``Key``, ``Query``, ``Batch``,
  ``Transaction``, and ``ExplainOptions``. Query filters (``PropertyFilter``,
  ``And``, ``Or``), ``AggregationQuery``, and ``GeoPoint`` are imported from
  submodules under ``google.cloud.datastore``.
- Use ``google.cloud.datastore_v1`` (bundled in this distribution) when you
  need ``asyncio`` (:class:`~google.cloud.datastore_v1.DatastoreAsyncClient`)
  or GAPIC-only features such as GQL queries (``GqlQuery``), vector search
  (``FindNearest``), ``PropertyMask``, ``PropertyTransform`` server-side
  transforms, or ``previous_transaction`` retry priority. Do not mix types
  between ``google.cloud.datastore`` and ``google.cloud.datastore_v1``, which
  share 10 class names: ``AggregationQuery``, ``AggregationResult``,
  ``Entity``, ``ExecutionStats``, ``ExplainMetrics``, ``ExplainOptions``,
  ``Key``, ``PlanSummary``, ``PropertyFilter``, and ``Query``.
- Use ``google-cloud-ndb`` when migrating App Engine NDB applications, or
  ``google-cloud-firestore`` for Firestore in Native mode.

Example:

.. code-block:: python

    from google.cloud import datastore

    # Reuse a single Client instance across requests.
    client = datastore.Client()
    key = client.key("Task", "sample_task")
    entity = datastore.Entity(key=key)
    entity["description"] = "Buy milk"
    client.put(entity)

See :class:`~google.cloud.datastore.client.Client` for lifecycle, transport,
emulator, and concurrency details, and the canonical product documentation at
https://docs.cloud.google.com/datastore/docs.
"""

from google.cloud.datastore.batch import Batch
from google.cloud.datastore.client import Client
from google.cloud.datastore.entity import Entity
from google.cloud.datastore.key import Key
from google.cloud.datastore.query import Query
from google.cloud.datastore.query_profile import ExplainOptions
from google.cloud.datastore.transaction import Transaction
from google.cloud.datastore.version import __version__

__all__ = [
    "__version__",
    "Batch",
    "Client",
    "Entity",
    "Key",
    "Query",
    "ExplainOptions",
    "Transaction",
]
