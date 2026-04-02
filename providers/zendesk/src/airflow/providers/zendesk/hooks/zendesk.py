#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from zenpy import Zenpy

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from zenpy.lib.api import BaseApi
    from zenpy.lib.api_objects import JobStatus, Ticket, TicketAudit
    from zenpy.lib.generator import SearchResultGenerator


class ZendeskHook(BaseHook):
    """
    Interact with Zendesk. This hook uses the Zendesk conn_id.

    :param zendesk_conn_id: The Airflow connection used for Zendesk credentials.
    """

    conn_name_attr = "zendesk_conn_id"
    default_conn_name = "zendesk_default"
    conn_type = "zendesk"
    hook_name = "Zendesk"

    def __init__(self, zendesk_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.zendesk_conn_id = zendesk_conn_id
        self.base_api: BaseApi | None = None
        self._zenpy_client: Zenpy | None = None
        self._url: str | None = None

    def _init_conn(self) -> tuple[Zenpy, str]:
        """
        Initialize the Zendesk client.

        The following authentication modes are supported:
        1. Use token: If 'use_token' is True in extras, the password field is treated as an API token.
        2. Token: If 'token' is provided in extras, it's used as an API token.
        3. OAuth: If 'oauth_token' is provided in extras, it's used as an OAuth token.
        4. Password: Defaults to email/password authentication if none of the above are provided.

        Precedence: use_token > token > oauth_token > password.
        """
        conn = self.get_connection(self.zendesk_conn_id)
        if not conn.host:
            raise ValueError(f"No host provided for {self.zendesk_conn_id}")

        domain = conn.host.split(".")[-2] + "." + conn.host.split(".")[-1]
        subdomain = conn.host.split(".")[0]
        url = f"https://{conn.host}"

        kwargs: dict[str, Any] = {
            "domain": domain,
            "subdomain": subdomain,
            "email": conn.login,
        }
        extra = conn.extra_dejson
        if extra.get("use_token"):
            kwargs["token"] = conn.password
        elif extra.get("token"):
            kwargs["token"] = extra.get("token")
        elif extra.get("oauth_token"):
            kwargs["oauth_token"] = extra.get("oauth_token")
        else:
            kwargs["password"] = conn.password

        return Zenpy(**kwargs), url

    @property
    def get(self):
        return self.get_conn().users._get

    def get_conn(self) -> Zenpy:
        """
        Get the underlying Zenpy client.

        :return: zenpy.Zenpy client.
        """
        if self._zenpy_client is None:
            self._zenpy_client, self._url = self._init_conn()
        return self._zenpy_client

    def get_ticket(self, ticket_id: int) -> Ticket:
        """
        Retrieve ticket.

        :return: Ticket object retrieved.
        """
        return self.get_conn().tickets(id=ticket_id)

    def search_tickets(self, **kwargs) -> SearchResultGenerator:
        """
        Search tickets.

        :param kwargs: (optional) Search fields given to the zenpy search method.
        :return: SearchResultGenerator of Ticket objects.
        """
        return self.get_conn().search(type="ticket", **kwargs)

    def create_tickets(self, tickets: Ticket | list[Ticket], **kwargs) -> TicketAudit | JobStatus:
        """
        Create tickets.

        :param tickets: Ticket or List of Ticket to create.
        :param kwargs: (optional) Additional fields given to the zenpy create method.
        :return: A TicketAudit object containing information about the Ticket created.
            When sending bulk request, returns a JobStatus object.
        """
        return self.get_conn().tickets.create(tickets, **kwargs)

    def update_tickets(self, tickets: Ticket | list[Ticket], **kwargs) -> TicketAudit | JobStatus:
        """
        Update tickets.

        :param tickets: Updated Ticket or List of Ticket object to update.
        :param kwargs: (optional) Additional fields given to the zenpy update method.
        :return: A TicketAudit object containing information about the Ticket updated.
            When sending bulk request, returns a JobStatus object.
        """
        return self.get_conn().tickets.update(tickets, **kwargs)

    def delete_tickets(self, tickets: Ticket | list[Ticket], **kwargs) -> None:
        """
        Delete tickets, returns nothing on success and raises APIException on failure.

        :param tickets: Ticket or List of Ticket to delete.
        :param kwargs: (optional) Additional fields given to the zenpy delete method.
        :return:
        """
        return self.get_conn().tickets.delete(tickets, **kwargs)
