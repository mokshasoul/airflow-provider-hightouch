"""
Airflow Trigger for HighTouch Syncs
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpAsyncHook
from airflow.providers.http.triggers.http import HttpTrigger
from airflow.triggers.base import BaseTrigger, TriggerEvent


class HightouchSyncTrigger(HttpTrigger):
    def __init__(
        self,
        *,
        sync_id: str | None = None,
        hightouch_conn_id: str = "hightouch_default",
        api_version: str = "v1",
        timeout: int = 3600,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.hightouch_conn_id = hightouch_conn_id
        self.api_version = api_version
        self.sync_id = sync_id
        self.sync_request_id: str = None
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize HighTouchTrigger arguments and classpath."""
        return (
            "airflow_provider_hightouch.triggers.HightouchSyncTrigger",
            {
                "hightouch_conn_id": self.hightouch_conn_id,
                "api_version": self.api_version,
                "sync_id": self.sync_id,
                "sync_request_id": self.sync_request_id,
                "timeout": self.timeout,
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
            trigger_response = await self.trigger_sync()
            if trigger_response.get("status_code", 404) != 200:
                raise AirflowException("Failed to trigger HighTouch sync")

            # Check the sync status periodically
            start_time = asyncio.get_event_loop().time()
            trigger_res_json = trigger_response.json()
            self.sync_request_id = trigger_res_json.get("id")
            while True:
                if asyncio.get_event_loop().time() - start_time > self.timeout:
                    raise AirflowException("HighTouch sync timed out")

                status_response = self.check_sync_status()
                if status_response.get("status_code") == 200:
                    yield TriggerEvent({"status": "success"})
                    return

                await asyncio.sleep(10)
        except AirflowException as e:
            self.log.exception(e)
            yield TriggerEvent({"status": "error", "message": str(e)})

    async def trigger_sync(self) -> Any:
        http_hook = HttpAsyncHook(http_conn_id=self.hightouch_conn_id)
        url = f"{self.api_version}/syncs/trigger"
        payload = {"syncId": self.sync_id, "syncSlug": self.sync_slug}
        headers = {
            "Authorization": f"Bearer {self.get_api_key()}",
            "Content-Type": "application/json",
        }
        response = http_hook.run(endpoint=url, json=payload, headers=headers)
        parsed_response = await self._convert_response(response)
        return parsed_response

    async def check_sync_status(self) -> Any:
        http_hook = HttpAsyncHook(http_conn_id=self.hightouch_conn_id)
        url = f"{self.api_version}/syncs/{self.sync_id}/status"
        headers = {"Authorization": f"Bearer {self.get_api_key()}"}
        payload = {"syncSequenceRunId": self.sync_request_id}
        response = http_hook.run(endpoint=url, json=payload, headers=headers)
        parsed_response = await HttpTrigger._convert_response(response)
        return parsed_response.json()

    async def get_api_key(self) -> str:
        hook = HttpAsyncHook(http_conn_id=self.hightouch_conn_id)
        connection = hook.get_connection(self.hightouch_conn_id)
        return connection.password
