from typing import Protocol

from .models import Message, TopicInfo


class CommAPI(Protocol):
    """上层 agent 使用的接口；发送者身份由 Comm 实例持有。"""

    def create_topic(
        self,
        name: str,
        description: str = "",
    ) -> str:
        ...

    def discover_topics(self) -> list[TopicInfo]:
        ...

    def join_topic(self, topic_id: str) -> None:
        ...

    def send(self, topic_id: str, payload: str) -> None:
        ...

    def receive(
        self,
        topic_id: str,
        limit: int = 100,
    ) -> list[Message]:
        ...

    def leave_topic(self, topic_id: str) -> None:
        ...

    def close(self) -> None:
        ...
