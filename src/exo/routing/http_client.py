import os
from dataclasses import dataclass

import anyio
import httpx

from exo.routing.http_relay import (
    RelayConnectionUpdateResponse,
    RelayMessageResponse,
    RelayPublishRequest,
    RelayRegisterRequest,
    RelaySubscribeRequest,
    get_http_relay,
    decode_payload,
    encode_payload,
)
from exo.shared.types.common import NodeId


@dataclass(frozen=True)
class HttpConnectionUpdate:
    update_type: int
    peer_id: str
    remote_ipv4: str
    remote_tcp_port: int


class HttpRelayClient:
    def __init__(self, node_id: str) -> None:
        self._node_id = NodeId(node_id)
        self._base_url = os.getenv("EXO_HTTP_RELAY_URL", "http://127.0.0.1:52415")
        inproc_flag = os.getenv("EXO_HTTP_RELAY_INPROC", "").lower() in {
            "1",
            "true",
            "yes",
        }
        inproc_url = self._base_url in {"inproc", "local"} or self._base_url.startswith(
            "inproc://"
        )
        self._use_http = not (inproc_flag or inproc_url)
        self._client = (
            httpx.AsyncClient(base_url=self._base_url, timeout=None)
            if self._use_http
            else None
        )
        self._relay = get_http_relay() if not self._use_http else None
        self._registered = False

    async def _ensure_registered(self) -> None:
        if self._registered:
            return
        listen_port = int(os.getenv("EXO_LIBP2P_LISTEN_PORT", "0") or "0")
        payload = RelayRegisterRequest(
            node_id=self._node_id,
            listen_port=listen_port if listen_port > 0 else None,
        )
        if not self._use_http:
            await self._relay.register_node(
                self._node_id, remote_ipv4="127.0.0.1", remote_tcp_port=listen_port
            )
            self._registered = True
            return
        for _ in range(30):
            try:
                await self._client.post("/relay/register", json=payload.model_dump())
                self._registered = True
                return
            except httpx.TransportError:
                await anyio.sleep(0.5)
        await self._client.post("/relay/register", json=payload.model_dump())
        self._registered = True

    async def gossipsub_subscribe(self, topic: str) -> bool:
        await self._ensure_registered()
        payload = RelaySubscribeRequest(node_id=self._node_id, topic=topic)
        if self._use_http:
            await self._client.post("/relay/subscribe", json=payload.model_dump())
        else:
            await self._relay.subscribe(self._node_id, topic)
        return True

    async def gossipsub_unsubscribe(self, topic: str) -> bool:
        await self._ensure_registered()
        payload = RelaySubscribeRequest(node_id=self._node_id, topic=topic)
        if self._use_http:
            await self._client.post("/relay/unsubscribe", json=payload.model_dump())
        else:
            await self._relay.unsubscribe(self._node_id, topic)
        return True

    async def gossipsub_publish(self, topic: str, data: bytes) -> None:
        await self._ensure_registered()
        payload = RelayPublishRequest(
            node_id=self._node_id, topic=topic, data_b64=encode_payload(data)
        )
        if self._use_http:
            await self._client.post("/relay/publish", json=payload.model_dump())
        else:
            await self._relay.publish(self._node_id, topic, data)

    async def gossipsub_recv(self) -> tuple[str, bytes]:
        await self._ensure_registered()
        if self._use_http:
            while True:
                try:
                    resp = await self._client.get(
                        "/relay/recv", params={"node_id": self._node_id}
                    )
                    resp.raise_for_status()
                    message = RelayMessageResponse.model_validate(resp.json())
                    return message.topic, decode_payload(message.data_b64)
                except (httpx.HTTPError, RuntimeError):
                    await anyio.sleep(0.5)
        message = await self._relay.recv_message(self._node_id)
        return message.topic, message.data

    async def connection_update_recv(self) -> HttpConnectionUpdate:
        await self._ensure_registered()
        if self._use_http:
            while True:
                try:
                    resp = await self._client.get(
                        "/relay/conn_recv", params={"node_id": self._node_id}
                    )
                    resp.raise_for_status()
                    update = RelayConnectionUpdateResponse.model_validate(resp.json())
                    return HttpConnectionUpdate(
                        update_type=update.update_type,
                        peer_id=update.peer_id,
                        remote_ipv4=update.remote_ipv4,
                        remote_tcp_port=update.remote_tcp_port,
                    )
                except (httpx.HTTPError, RuntimeError):
                    await anyio.sleep(0.5)
        update = await self._relay.recv_connection_update(self._node_id)
        return HttpConnectionUpdate(
            update_type=update.update_type,
            peer_id=update.peer_id,
            remote_ipv4=update.remote_ipv4,
            remote_tcp_port=update.remote_tcp_port,
        )
