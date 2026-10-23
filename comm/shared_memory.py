from __future__ import annotations

import ctypes
import ctypes.util
import json
import os
import struct
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from multiprocessing.shared_memory import SharedMemory
from threading import RLock
from uuid import uuid4


# macOS 从进程的系统库取符号；Linux 兼容仍由 libpthread 提供 semaphore
# 的系统。仅使用系统自带的 POSIX API，不需要额外的 Python 依赖。
_POSIX = ctypes.CDLL(
    ctypes.util.find_library("pthread") if sys.platform.startswith("linux") else None,
    use_errno=True,
)
_POSIX.sem_open.restype = ctypes.c_void_p
# sem_open 是可变参数函数；Apple ARM64 必须只声明前两个固定参数。
_POSIX.sem_open.argtypes = [ctypes.c_char_p, ctypes.c_int]
for _function in ("sem_wait", "sem_post", "sem_close"):
    getattr(_POSIX, _function).restype = ctypes.c_int
    getattr(_POSIX, _function).argtypes = [ctypes.c_void_p]
_POSIX.sem_unlink.restype = ctypes.c_int
_POSIX.sem_unlink.argtypes = [ctypes.c_char_p]


def _check_status(result: int) -> None:
    if result == -1:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def _open_semaphore(name: str, *, create: bool = False) -> int:
    encoded_name = f"/{name}".encode("ascii")
    if create:
        handle = _POSIX.sem_open(
            encoded_name,
            os.O_CREAT | os.O_EXCL,
            ctypes.c_uint(0o600),
            ctypes.c_uint(1),
        )
    else:
        handle = _POSIX.sem_open(encoded_name, 0)
    # SEM_FAILED 在不同系统中可能是 NULL 或 (sem_t *) -1。
    if handle is None or handle == ctypes.c_void_p(-1).value:
        _check_status(-1)
    return handle


class SharedMemoryTransport:
    """本机节点公告，以及每个 agent、topic 独立的共享内存收件箱。"""

    ANNOUNCEMENT_SLOTS = 64
    ANNOUNCEMENT_BYTES = 64 * 1024
    INBOX_BYTES = 1024 * 1024
    _LENGTH = struct.Struct("!I")

    def __init__(self, agent_id: str) -> None:
        if not isinstance(agent_id, str) or not agent_id:
            raise ValueError("agent_id must be a non-empty string")

        self.agent_id = agent_id
        self._inboxes: dict[str, dict] = {}
        self._announcement_name: str | None = None
        self._closed = False
        self._lock = RLock()

    def create_inbox(self, topic_id: str) -> dict:
        """创建自己的收件箱；同一次订阅重复调用返回相同引用。"""
        with self._lock:
            self._ensure_open()
            if not isinstance(topic_id, str) or not topic_id:
                raise ValueError("topic_id must be a non-empty string")

            existing = self._inboxes.get(topic_id)
            if existing is not None:
                return dict(existing)

            inbox_id = uuid4().hex
            # semaphore 名称也要满足 macOS 的长度限制。
            name = f"aq1_{inbox_id[:24]}"
            inbox = {
                "topic_id": topic_id,
                "agent_id": self.agent_id,
                "inbox_id": inbox_id,
                "shm_name": name,
            }
            self._create(
                name,
                {"inbox": inbox, "messages": []},
                self.INBOX_BYTES,
            )
            self._inboxes[topic_id] = inbox
            return dict(inbox)

    def remove_inbox(self, topic_id: str) -> None:
        """删除自己的收件箱，丢弃未读消息；重复删除无副作用。"""
        with self._lock:
            self._ensure_open()
            inbox = self._inboxes.get(topic_id)
            if inbox is not None:
                self._destroy(inbox["shm_name"])
                del self._inboxes[topic_id]

    def can_access(self, inbox: dict) -> bool:
        """探测本机是否可以访问这个确切的收件箱，不写入探测消息。"""
        with self._lock:
            self._ensure_open()
            try:
                with self._locked(inbox["shm_name"]) as block:
                    return self._read(block).get("inbox") == inbox
            except FileNotFoundError:
                return False

    def send(self, inbox: dict, payload: str) -> None:
        """直接写入一个本机可访问的目标收件箱。"""
        with self._lock:
            self._ensure_open()
            self._append(inbox, sender_id=self.agent_id, payload=payload)

    def accept(self, message: dict) -> None:
        """网络接收回调：仅接受当前订阅的收件箱，不再向其他节点转发。"""
        with self._lock:
            self._ensure_open()
            inbox = message["inbox"]
            current = self._inboxes.get(inbox["topic_id"])
            if current is None or current != inbox:
                raise ValueError("target inbox is not an active subscription")

            self._append(
                current,
                sender_id=message["sender_id"],
                payload=message["payload"],
            )

    def receive(self, topic_id: str, limit: int = 100) -> list[dict[str, str]]:
        """消费最多 limit 条消息；没有新消息时立即返回空列表。"""
        with self._lock:
            self._ensure_open()
            if type(limit) is not int or limit <= 0:
                raise ValueError("limit must be a positive integer")

            inbox = self._inboxes.get(topic_id)
            if inbox is None:
                raise ValueError("this agent has not joined the topic")

            with self._locked(inbox["shm_name"]) as block:
                state = self._read(block)
                if state["inbox"] != inbox:
                    raise RuntimeError("inbox reference mismatch")

                received = state["messages"][:limit]
                if received:
                    state["messages"] = state["messages"][len(received):]
                    self._write(block, state)
                return received

    def publish_announcement(self, announcement: dict) -> None:
        """发布或完整替换自己的公告，公告结构由 Comm 定义。"""
        with self._lock:
            self._ensure_open()
            if announcement.get("agent_id") != self.agent_id:
                raise ValueError("announcement must belong to this agent")

            if self._announcement_name is not None:
                with self._locked(self._announcement_name) as block:
                    self._read(block)
                    self._write(block, announcement)
                return

            for index in range(self.ANNOUNCEMENT_SLOTS):
                name = f"ac1_ad_{index}"
                try:
                    self._create(name, announcement, self.ANNOUNCEMENT_BYTES)
                except FileExistsError:
                    continue
                self._announcement_name = name
                return

            raise RuntimeError("no free local announcement slot")

    def read_announcements(self) -> list[dict]:
        """扫描本机公告槽；固定的是槽位名称，不是 agent 名单。"""
        with self._lock:
            self._ensure_open()
            announcements = []
            for index in range(self.ANNOUNCEMENT_SLOTS):
                try:
                    with self._locked(f"ac1_ad_{index}") as block:
                        announcements.append(self._read(block))
                except FileNotFoundError:
                    # 空槽、尚未初始化完成或已经正常退出。
                    continue
            return announcements

    def remove_announcement(self) -> None:
        """撤销自己的本机公告。"""
        with self._lock:
            self._ensure_open()
            if self._announcement_name is not None:
                self._destroy(self._announcement_name)
                self._announcement_name = None

    def close(self) -> None:
        """清理自己创建的资源；重复关闭无副作用。"""
        with self._lock:
            if self._closed:
                return
            self.remove_announcement()
            for topic_id in tuple(self._inboxes):
                self.remove_inbox(topic_id)
            self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("shared memory transport is closed")

    def _append(self, inbox: dict, *, sender_id: str, payload: str) -> None:
        if not isinstance(sender_id, str):
            raise TypeError("sender_id must be a string")
        if not isinstance(payload, str):
            raise TypeError("payload must be a string")

        with self._locked(inbox["shm_name"]) as block:
            state = self._read(block)
            if state.get("inbox") != inbox:
                raise ValueError("target inbox reference mismatch")
            # 只解析传输封装；payload 始终作为不透明字符串保存。
            state["messages"].append({"sender_id": sender_id, "payload": payload})
            self._write(block, state)

    @classmethod
    def _create(cls, name: str, data: dict, size: int) -> None:
        """先独占创建并初始化共享内存，再发布可用于加锁的 semaphore。"""
        block = SharedMemory(name=name, create=True, size=size, track=False)
        semaphore = None
        try:
            cls._write(block, data)
            semaphore = _open_semaphore(name, create=True)
        except BaseException:
            block.unlink()
            raise
        finally:
            try:
                if semaphore is not None:
                    _check_status(_POSIX.sem_close(semaphore))
            finally:
                block.close()

    @staticmethod
    @contextmanager
    def _locked(name: str) -> Iterator[SharedMemory]:
        """临时打开资源并加锁；使用其他 agent 的资源后只关闭句柄。"""
        block = SharedMemory(name=name, track=False)
        try:
            semaphore = _open_semaphore(name)
            try:
                _check_status(_POSIX.sem_wait(semaphore))
                try:
                    yield block
                finally:
                    _check_status(_POSIX.sem_post(semaphore))
            finally:
                _check_status(_POSIX.sem_close(semaphore))
        finally:
            block.close()

    @classmethod
    def _read(cls, block: SharedMemory) -> dict:
        length = cls._LENGTH.unpack_from(block.buf, 0)[0]
        if length == 0:
            raise FileNotFoundError(f"shared memory is inactive: {block.name}")
        if length > block.size - cls._LENGTH.size:
            raise RuntimeError("invalid shared memory record length")
        start = cls._LENGTH.size
        return json.loads(bytes(block.buf[start:start + length]))

    @classmethod
    def _write(cls, block: SharedMemory, data: dict) -> None:
        raw = json.dumps(data, separators=(",", ":")).encode("utf-8")
        start = cls._LENGTH.size
        # 序列化和容量检查都在写入之前完成，队列满时不改变原内容。
        if len(raw) > block.size - start:
            raise BufferError(f"shared memory capacity exceeded: {block.name}")
        block.buf[start:start + len(raw)] = raw
        cls._LENGTH.pack_into(block.buf, 0, len(raw))

    @classmethod
    def _destroy(cls, name: str) -> None:
        """仅供本 transport 的自有资源使用：先失效，再释放名称。"""
        try:
            with cls._locked(name) as block:
                if cls._LENGTH.unpack_from(block.buf, 0)[0] == 0:
                    return
                cls._LENGTH.pack_into(block.buf, 0, 0)
                _check_status(_POSIX.sem_unlink(f"/{name}".encode("ascii")))
                block.unlink()
        except FileNotFoundError:
            return
