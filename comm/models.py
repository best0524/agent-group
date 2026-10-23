from dataclasses import dataclass


@dataclass(frozen=True)
class TopicInfo:
    topic_id: str
    name: str
    description: str = ""


@dataclass(frozen=True)
class Message:
    sender_id: str
    payload: str


@dataclass(frozen=True)
class PeerInfo:
    agent_id: str
    address: str
    is_connected: bool
