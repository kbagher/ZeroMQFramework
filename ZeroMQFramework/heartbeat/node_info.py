from dataclasses import dataclass, asdict
from ZeroMQFramework.common.node_type import ZeroMQNodeType


@dataclass
class ZeroMQNodeInfo:
    node_id: str
    session_id: str
    node_type: ZeroMQNodeType
    last_heartbeat: int
    missed_count: int = 0

    def to_dict(self) -> dict:
        data = asdict(self)
        data['node_type'] = self.node_type.value
        return data

    @staticmethod
    def from_dict(data: dict) -> 'ZeroMQNodeInfo':
        return ZeroMQNodeInfo(
            node_id=data['node_id'],
            session_id=data['session_id'],
            node_type=ZeroMQNodeType(data['node_type']),
            last_heartbeat=data['last_heartbeat'],
            missed_count=data.get('missed_count', 0)
        )
