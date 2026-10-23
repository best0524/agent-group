from typing import Protocol

from .models import TopicInfo


class CommAPI(Protocol):
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

    def send(self, sender: str, topic_id: str, payload: str) -> None:
        ...

    def receive(
            self,
            topic_id: str,
            limit: int = 100,
    ) -> list[str]:
        ...

    def leave_topic(self, topic_id: str) -> None:
        ...
