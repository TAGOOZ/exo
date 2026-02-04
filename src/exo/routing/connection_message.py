from enum import Enum

from exo_pyo3_bindings import ConnectionUpdate, ConnectionUpdateType

from exo.shared.types.common import NodeId
from exo.utils.pydantic_ext import CamelCaseModel

"""Serialisable types for Connection Updates/Messages"""


class ConnectionMessageType(Enum):
    Connected = 0
    Disconnected = 1

    @staticmethod
    def from_update_type(update_type: ConnectionUpdateType):
        match update_type:
            case ConnectionUpdateType.Connected:
                return ConnectionMessageType.Connected
            case ConnectionUpdateType.Disconnected:
                return ConnectionMessageType.Disconnected


class ConnectionMessage(CamelCaseModel):
    node_id: NodeId
    connection_type: ConnectionMessageType
    remote_ipv4: str
    remote_tcp_port: int

    @classmethod
    def from_update(cls, update: ConnectionUpdate) -> "ConnectionMessage":
        peer_id = update.peer_id.to_base58() if hasattr(update.peer_id, "to_base58") else str(update.peer_id)
        update_type = update.update_type
        if isinstance(update_type, ConnectionUpdateType):
            conn_type = ConnectionMessageType.from_update_type(update_type)
        elif isinstance(update_type, ConnectionMessageType):
            conn_type = update_type
        elif isinstance(update_type, int):
            conn_type = ConnectionMessageType.Connected if update_type == 0 else ConnectionMessageType.Disconnected
        else:
            conn_type = ConnectionMessageType.Connected
        return cls(
            node_id=NodeId(peer_id),
            connection_type=conn_type,
            remote_ipv4=update.remote_ipv4,
            remote_tcp_port=update.remote_tcp_port,
        )
