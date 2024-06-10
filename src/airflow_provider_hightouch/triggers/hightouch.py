"""
Airflow Trigger for HighTouch Syncs
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

import requests
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from requests.cookies import RequestsCookieJar
from requests.structures import CaseInsensitiveDict

if TYPE_CHECKING:
    from aiohttp.client_reqrep import ClientResponse


class HightouchSyncTrigger(BaseTrigger):
    def __init__(
        self,
        *,
        sync_id: str | None = None,
        sync_slug: str | None = None,
        hightouch_conn_id: str = "hightouch_default",
        api_version: str = "v3",
        timeout: int = 3600,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.hightouch_conn_id = hightouch_conn_id
        self.api_version = api_version
        self.sync_id = sync_id
        self.sync_slug = sync_slug
        self.sync_request_id: str = None
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize HighTouchTrigger arguments and classpath."""
        return (
            "airflow_provider_hightouch.triggers.HightouchSyncTrigger",
            {
                "hightouch_conn_id": self.hightouch_conn_id,
                "method": self.method,
                "auth_type": self.auth_type,
                "endpoint": self.endpoint,
                "headers": self.headers,
                "data": self.data,
                "extra_options": self.extra_options,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            # Trigger the sync
            trigger_response = self.trigger_sync()
            if trigger_response.get("status") != "success":
                raise AirflowException("Failed to trigger HighTouch sync")

            # Check the sync status periodically
            start_time = asyncio.get_event_loop().time()
            while True:
                if asyncio.get_event_loop().time() - start_time > self.timeout:
                    raise AirflowException("HighTouch sync timed out")

                status_response = self.check_sync_status()
                if status_response.get("status") == "completed":
                    yield TriggerEvent({"status": "success"})
                    return
                elif status_response.get("status") == "failed":
                    raise AirflowException("HighTouch sync failed")

                await asyncio.sleep(10)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    async def trigger_sync(self) -> Any:
        url = f"https://api.hightouch.io/api/{self.api_version}/syncs/trigger"
        payload = {"syncId": self.sync_id, "syncSlug": self.sync_slug}
        headers = {
            "Authorization": f"Bearer {self.get_api_key()}",
            "Content-Type": "application/json",
        }
        response = requests.post(
            url, headers=headers, json=payload, timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def check_sync_status(self) -> Any:
        url = f"https://api.hightouch.io/api/{self.api_version}/syncs/{self.sync_id}/status"
        headers = {"Authorization": f"Bearer {self.get_api_key()}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def get_api_key(self) -> str:
        hook = HttpAsyncHook(http_conn_id=self.hightouch_conn_id)
        connection = hook.get_connection(self.hightouch_conn_id)
        return connection.password
