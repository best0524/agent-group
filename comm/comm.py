from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from threading import Lock, RLock
from types import TracebackType
from urllib.error import URLError
from uuid import uuid4

from .models import Message, TopicInfo
from .network import NetworkTransport
from .shared_memory import SharedMemoryTransport


class Comm:
    """一个 agent 节点的通信组件；构造完成后即可使用六个业务接口。"""

    def __init__(
        self,
        agent_id: str | None = None,
        host: str | None = None,
        port: int = 0,
        *,
        seeds: Sequence[str] = (),
        timeout: float = 5.0,
    ) -> None:
        """同机及同一局域网自动发现；seeds 为可选的额外发现入口。"""
        if isinstance(seeds, str):
            raise TypeError("seeds must be a sequence of endpoint strings")

        self.agent_id = uuid4().hex if agent_id is None else agent_id
        self._seeds = tuple(endpoint.rstrip("/") for endpoint in seeds)
        self._subscriptions: dict[str, dict] = {}
        self._peers: dict[str, dict] = {}
        self._lock = RLock()
        self._refresh_lock = Lock()
        self._closed = False

        self._network = NetworkTransport(
            self.agent_id, host, port, timeout=timeout
        )
        self._shared_memory = SharedMemoryTransport(self.agent_id)
        try:
            self._network.start(
                on_message=self._shared_memory.accept,
                on_exchange=self._handle_exchange,
            )
            with self._lock:
                self._publish_self()
            self._refresh(discover_network=True)
        except BaseException:
            self.close()
            raise

    @property
    def endpoint(self) -> str:
        """本节点实际的 HTTP 地址，可作为其他节点的 seed。"""
        return self._network.endpoint

    def create_topic(self, name: str, description: str = "") -> str:
        """创建并自动加入一个 topic；同名 topic 可以有不同的 ID。"""
        if not isinstance(name, str) or not name:
            raise ValueError("name must be a non-empty string")
        if not isinstance(description, str):
            raise TypeError("description must be a string")

        topic = TopicInfo(uuid4().hex, name, description)
        with self._lock:
            self._ensure_open()
            self._join(topic)
        return topic.topic_id

    def discover_topics(self) -> list[TopicInfo]:
        """刷新发现信息，返回当前可联系到的节点参与的 topic。"""
        topics = {}
        for peer in self._refresh(discover_network=True):
            for topic_id, subscription in peer["topics"].items():
                topics[topic_id] = TopicInfo(
                    topic_id,
                    subscription["name"],
                    subscription["description"],
                )
        return sorted(topics.values(), key=lambda topic: (topic.name, topic.topic_id))

    def join_topic(self, topic_id: str) -> None:
        """加入已有 topic；重复加入不清空队列，只接收加入后的消息。"""
        with self._lock:
            self._ensure_open()
            if topic_id in self._subscriptions:
                return

        topics = self.discover_topics()
        topic = next((item for item in topics if item.topic_id == topic_id), None)
        if topic is None:
            raise ValueError(f"topic not found: {topic_id}")

        with self._lock:
            self._ensure_open()
            if topic_id not in self._subscriptions:
                self._join(topic)

    def send(self, topic_id: str, payload: str) -> None:
        """向该 topic 的其他成员投递原始字符串，不给自己回送。"""
        if not isinstance(payload, str):
            raise TypeError("payload must be a string")
        with self._lock:
            self._require_joined(topic_id)

        peers = self._refresh()
        with self._lock:
            self._require_joined(topic_id)

        for peer in peers:
            if peer["agent_id"] == self.agent_id:
                continue
            subscription = peer["topics"].get(topic_id)
            if subscription is None:
                continue

            inbox = subscription["inbox"]
            if self._shared_memory.can_access(inbox):
                self._shared_memory.send(inbox, payload)
            else:
                self._network.send(peer["endpoint"], inbox, payload)

    def receive(self, topic_id: str, limit: int = 100) -> list[Message]:
        """消费最多 limit 条消息；没有消息时立即返回空列表。"""
        with self._lock:
            self._require_joined(topic_id)
            return [
                Message(**item)
                for item in self._shared_memory.receive(topic_id, limit)
            ]

    def leave_topic(self, topic_id: str) -> None:
        """退出并丢弃未读消息；重复退出无副作用。"""
        with self._lock:
            self._ensure_open()
            if topic_id not in self._subscriptions:
                return
            self._shared_memory.remove_inbox(topic_id)
            del self._subscriptions[topic_id]
            self._publish_self()

    def close(self) -> None:
        """停止接收并清理本节点的公告和收件箱。"""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._shared_memory.remove_announcement()

        # 不持有状态锁等待网络线程，避免阻塞正在完成的发现回调。
        try:
            self._network.close()
        finally:
            self._shared_memory.close()

    def __enter__(self) -> Comm:
        with self._lock:
            self._ensure_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("comm is closed")

    def _require_joined(self, topic_id: str) -> None:
        self._ensure_open()
        if topic_id not in self._subscriptions:
            raise ValueError(f"this agent has not joined topic: {topic_id}")

    def _join(self, topic: TopicInfo) -> None:
        inbox = self._shared_memory.create_inbox(topic.topic_id)
        self._subscriptions[topic.topic_id] = {
            "name": topic.name,
            "description": topic.description,
            "inbox": inbox,
        }
        try:
            self._publish_self()
        except BaseException:
            del self._subscriptions[topic.topic_id]
            self._shared_memory.remove_inbox(topic.topic_id)
            raise

    def _announcement(self) -> dict:
        # 调用方持有状态锁。订阅项创建后不修改，因此浅拷贝即可。
        return {
            "agent_id": self.agent_id,
            "endpoint": self.endpoint,
            "topics": dict(self._subscriptions),
            "peers": list({peer["endpoint"] for peer in self._peers.values()}),
        }

    def _publish_self(self) -> None:
        self._shared_memory.publish_announcement(self._announcement())

    def _remember(self, announcement: dict) -> None:
        agent_id = announcement["agent_id"]
        if agent_id == self.agent_id:
            if announcement["endpoint"] != self.endpoint:
                raise ValueError(f"agent_id is already in use: {agent_id}")
        else:
            self._peers[agent_id] = announcement

    def _handle_exchange(self, announcement: dict) -> list[dict]:
        """只交换本地信息，不从请求回调中发起下一轮网络发现。"""
        with self._lock:
            self._ensure_open()
            self._remember(announcement)
            for local in self._shared_memory.read_announcements():
                self._remember(local)
            self._publish_self()
            # 首项明确是应答者自己；后续公告仅供请求方寻找更多节点。
            return [self._announcement(), *self._peers.values()]

    def _refresh(self, *, discover_network: bool = False) -> list[dict]:
        """本机扫描、局域网发现和逐个握手；确认后的公告用于成员路由。"""
        with self._refresh_lock:
            with self._lock:
                self._ensure_open()
                own = self._announcement()
                local = self._shared_memory.read_announcements()
                pending = deque(self._seeds)
                pending.extend(peer["endpoint"] for peer in self._peers.values())
                current = {self.agent_id: own}
                for peer in local:
                    self._remember(peer)
                    current[peer["agent_id"]] = peer
                    pending.extend(peer.get("peers", ()))

            # 组播只获取地址，不在状态锁内等待，也不直接作为 topic 成员。
            if discover_network:
                pending.extend(self._network.discover())

            # 本机公告已经确认过，无需再用 HTTP 查询这些节点。
            confirmed = {peer["endpoint"] for peer in current.values()}
            visited = set()
            while pending:
                endpoint = pending.popleft()
                if endpoint in visited or endpoint in confirmed:
                    continue
                visited.add(endpoint)

                try:
                    announcements = self._network.exchange(endpoint, own)
                except (URLError, OSError):
                    # 节点正常退出后，旧地址不参与本轮 topic 和成员计算。
                    continue

                peer = announcements[0]
                with self._lock:
                    self._remember(peer)
                current[peer["agent_id"]] = peer
                confirmed.add(peer["endpoint"])
                for item in announcements:
                    pending.append(item["endpoint"])
                    pending.extend(item.get("peers", ()))

            with self._lock:
                self._ensure_open()
                # 刷新期间本节点可能创建或退出了 topic，取自己的最新状态。
                self._publish_self()
                current[self.agent_id] = self._announcement()
            return list(current.values())
