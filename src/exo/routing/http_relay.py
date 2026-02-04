import base64
from dataclasses import dataclass
from typing import Final

import anyio
from anyio import create_memory_object_stream
from anyio.abc import ObjectReceiveStream, ObjectSendStream
from pydantic import PositiveInt

from exo.shared.types.common import NodeId
from exo.utils.pydantic_ext import CamelCaseModel


class RelayRegisterRequest(CamelCaseModel):
    node_id: NodeId
    listen_port: PositiveInt | None = None


class RelaySubscribeRequest(CamelCaseModel):
    node_id: NodeId
    topic: str


class RelayPublishRequest(CamelCaseModel):
    node_id: NodeId
    topic: str
    data_b64: str


class RelayMessageResponse(CamelCaseModel):
    topic: str
    data_b64: str


class RelayConnectionUpdateResponse(CamelCaseModel):
    update_type: int
    peer_id: str
    remote_ipv4: str
    remote_tcp_port: int


@dataclass(frozen=True)
class RelayMessage:
    topic: str
    data: bytes


@dataclass(frozen=True)
class RelayConnectionUpdate:
    update_type: int
    peer_id: str
    remote_ipv4: str
    remote_tcp_port: int


@dataclass
class _NodeState:
    subscriptions: set[str]
    message_send: ObjectSendStream[RelayMessage]
    message_recv: ObjectReceiveStream[RelayMessage]
    connection_send: ObjectSendStream[RelayConnectionUpdate]
    connection_recv: ObjectReceiveStream[RelayConnectionUpdate]
    remote_ipv4: str
    remote_tcp_port: int


class HttpRelay:
    def __init__(self) -> None:
        self._nodes: dict[str, _NodeState] = {}
        self._lock = anyio.Lock()

    async def register_node(
        self, node_id: NodeId, *, remote_ipv4: str, remote_tcp_port: int
    ) -> None:
        async with self._lock:
            if str(node_id) in self._nodes:
                state = self._nodes[str(node_id)]
                state.remote_ipv4 = remote_ipv4
                state.remote_tcp_port = remote_tcp_port
                return

            msg_send, msg_recv = create_memory_object_stream[RelayMessage](1024)
            conn_send, conn_recv = create_memory_object_stream[RelayConnectionUpdate](1024)

            self._nodes[str(node_id)] = _NodeState(
                subscriptions=set(),
                message_send=msg_send,
                message_recv=msg_recv,
                connection_send=conn_send,
                connection_recv=conn_recv,
                remote_ipv4=remote_ipv4,
                remote_tcp_port=remote_tcp_port,
            )

            existing = [
                (nid, state)
                for nid, state in self._nodes.items()
                if nid != str(node_id)
            ]

        for other_id, other in existing:
            await other.connection_send.send(
                RelayConnectionUpdate(
                    update_type=0,
                    peer_id=str(node_id),
                    remote_ipv4=remote_ipv4,
                    remote_tcp_port=remote_tcp_port,
                )
            )

            await self._nodes[str(node_id)].connection_send.send(
                RelayConnectionUpdate(
                    update_type=0,
                    peer_id=other_id,
                    remote_ipv4=other.remote_ipv4,
                    remote_tcp_port=other.remote_tcp_port,
                )
            )

    async def unregister_node(self, node_id: NodeId) -> None:
        async with self._lock:
            removed = self._nodes.pop(str(node_id), None)
            remaining = list(self._nodes.items())

        if removed is None:
            return

        for other_id, other in remaining:
            await other.connection_send.send(
                RelayConnectionUpdate(
                    update_type=1,
                    peer_id=str(node_id),
                    remote_ipv4=removed.remote_ipv4,
                    remote_tcp_port=removed.remote_tcp_port,
                )
            )

    async def subscribe(self, node_id: NodeId, topic: str) -> None:
        async with self._lock:
            if str(node_id) not in self._nodes:
                raise KeyError(f"Node {node_id} not registered")
            self._nodes[str(node_id)].subscriptions.add(topic)

    async def unsubscribe(self, node_id: NodeId, topic: str) -> None:
        async with self._lock:
            if str(node_id) not in self._nodes:
                raise KeyError(f"Node {node_id} not registered")
            self._nodes[str(node_id)].subscriptions.discard(topic)

    async def publish(self, node_id: NodeId, topic: str, data: bytes) -> None:
        async with self._lock:
            targets = [
                state
                for nid, state in self._nodes.items()
                if nid != str(node_id) and topic in state.subscriptions
            ]

        for state in targets:
            await state.message_send.send(RelayMessage(topic=topic, data=data))

    async def recv_message(self, node_id: NodeId) -> RelayMessage:
        async with self._lock:
            state = self._nodes.get(str(node_id))

        if state is None:
            raise KeyError(f"Node {node_id} not registered")

        return await state.message_recv.receive()

    async def recv_connection_update(self, node_id: NodeId) -> RelayConnectionUpdate:
        async with self._lock:
            state = self._nodes.get(str(node_id))

        if state is None:
            raise KeyError(f"Node {node_id} not registered")

        return await state.connection_recv.receive()


_HTTP_RELAY: Final[HttpRelay] = HttpRelay()


def get_http_relay() -> HttpRelay:
    return _HTTP_RELAY


def encode_payload(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def decode_payload(data_b64: str) -> bytes:
    return base64.b64decode(data_b64.encode("ascii"))
