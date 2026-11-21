# Copyright 2017 Google LLC
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

"""Helpers for making API requests via gapic / gRPC."""

from urllib.parse import urlparse

from google.cloud._helpers import make_secure_channel
from google.cloud._http import DEFAULT_USER_AGENT
from grpc import insecure_channel

from google.cloud.datastore_v1.services.datastore import client as datastore_client
from google.cloud.datastore_v1.services.datastore.transports import grpc


def make_datastore_api(client):
    """Create an instance of the GAPIC Datastore API.

    Parses ``client._base_url`` with :func:`urllib.parse.urlparse`. An
    ``https`` scheme creates a secure gRPC channel targeting the parsed
    host on port 443, while any other scheme (such as ``http`` for the
    local emulator) creates an insecure gRPC channel. Custom
    ``client_options.api_endpoint`` values must therefore include an
    explicit URL scheme (for example, ``https://datastore.googleapis.com``
    or ``http://localhost:8081``). Passing a bare hostname without a scheme
    yields an empty host and an insecure channel.

    :type client: :class:`~google.cloud.datastore.client.Client`
    :param client: The client that holds configuration details.

    :rtype: :class:`google.cloud.datastore_v1.services.datastore.client.DatastoreClient`
    :returns: A datastore API instance with the proper credentials.
    """
    parse_result = urlparse(client._base_url)
    host = parse_result.netloc
    if parse_result.scheme == "https":
        channel = make_secure_channel(client._credentials, DEFAULT_USER_AGENT, host)
    else:
        channel = insecure_channel(host)

    transport = grpc.DatastoreGrpcTransport(channel=channel)
    return datastore_client.DatastoreClient(
        transport=transport, client_info=client._client_info
    )
