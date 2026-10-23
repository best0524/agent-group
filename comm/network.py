from __future__ import annotations

import json
import math
import socket
import sys
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, RLock, Thread
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener


class NetworkTransport:
    """每个 agent 独立运行的 HTTP 服务，不维护 topic 或成员状态。"""

    _DISCOVERY_GROUP = "239.255.42.99"
    _DISCOVERY_PORT = 42429
    _DISCOVERY_PROTOCOL = "agent-comm/1"
    _DISCOVERY_QUERY = b"agent-comm/1 discover"

    def __init__(
        self,
        agent_id: str,
        host: str | None = None,
        port: int = 0,
        *,
        timeout: float = 5.0,
    ) -> None:
        """默认选择局域网 IPv4 地址；127.* 为本机模式，port=0 自动分配。"""
        if not isinstance(agent_id, str) or not agent_id:
            raise ValueError("agent_id must be a non-empty string")
        if host is not None and (not isinstance(host, str) or not host):
            raise ValueError("host must be a local IPv4 address or hostname")
        if type(port) is not int or not 0 <= port <= 65535:
            raise ValueError("port must be an integer between 0 and 65535")
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("timeout must be a finite positive number")

        self.agent_id = agent_id
        self.host = self._resolve_host(host)
        if self.host == "0.0.0.0":
            raise ValueError("host must be a concrete local IPv4 address")
        self.port = port
        self.timeout = timeout

        self._server: ThreadingHTTPServer | None = None
        self._thread: Thread | None = None
        self._endpoint: str | None = None
        self._closed = False
        self._lifecycle_lock = RLock()
        self._discovery_socket: socket.socket | None = None
        self._discovery_thread: Thread | None = None
        self._discovery_stop = Event()

    @property
    def endpoint(self) -> str:
        """启动后公布给其他节点的地址，包含实际分配的端口。"""
        if self._endpoint is None:
            raise RuntimeError("network transport has not started")
        return self._endpoint

    def start(
        self,
        *,
        on_message: Callable[[dict], None],
        on_exchange: Callable[[dict], list[dict]],
    ) -> str:
        """启动服务并返回 endpoint。

        on_message 完成目标收件箱入队后才返回，服务随后确认接收。
        on_exchange 返回的首项必须为应答节点自身公告，后续项为节点线索。
        回调只处理本地信息，不递归请求其他节点。
        两个回调可能被不同请求线程同时调用，应自行保护共享状态。
        """
        if not callable(on_message) or not callable(on_exchange):
            raise TypeError("on_message and on_exchange must be callable")
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("network transport is closed")
            if self._server is not None:
                raise RuntimeError("network transport has already started")

            owner = self

            class Handler(BaseHTTPRequestHandler):
                def setup(self) -> None:
                    self.request.settimeout(owner.timeout)
                    super().setup()

                def log_message(self, format: str, *args) -> None:
                    pass

                def _reply(self, status: int, data: dict) -> None:
                    raw = json.dumps(
                        data,
                        separators=(",", ":"),
                    ).encode("utf-8")

                    self.send_response(status)
                    self.send_header(
                        "Content-Type",
                        "application/json; charset=utf-8",
                    )
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)

                def do_POST(self) -> None:
                    if self.path not in ("/exchange", "/message"):
                        self._reply(404, {"error": "unknown endpoint"})
                        return

                    try:
                        length = int(self.headers.get("Content-Length", "0"))
                        if length <= 0:
                            raise ValueError("request body is required")

                        raw = self.rfile.read(length)
                        if len(raw) != length:
                            raise ValueError("incomplete request body")

                        body = json.loads(raw)
                        if not isinstance(body, dict):
                            raise ValueError("request body must be an object")

                        if self.path == "/exchange":
                            announcements = on_exchange(body)
                            if not isinstance(announcements, list) or not all(
                                isinstance(item, dict) for item in announcements
                            ):
                                raise TypeError(
                                    "on_exchange must return a list of objects"
                                )

                            result = {
                                "announcements": announcements,
                            }

                        else:
                            inbox = body["inbox"]
                            if not isinstance(inbox, dict):
                                raise TypeError("inbox must be an object")
                            if inbox["agent_id"] != owner.agent_id:
                                raise ValueError(
                                    "message targets another agent"
                                )
                            if not isinstance(body["sender_id"], str):
                                raise TypeError("sender_id must be a string")
                            if not isinstance(body["payload"], str):
                                raise TypeError("payload must be a string")

                            # 回调完成入队后，才能返回成功。
                            on_message(body)
                            result = {"ok": True}

                        status = 200

                    except BufferError as exc:
                        status = 409
                        result = {"error": str(exc)}

                    except (ValueError, KeyError, TypeError) as exc:
                        status = 400
                        result = {"error": str(exc)}

                    except Exception as exc:
                        status = 500
                        result = {"error": str(exc)}

                    self._reply(status, result)

            server = ThreadingHTTPServer(
                (self.host, self.port),
                Handler,
            )

            # close 时等待正在执行的接收回调结束。
            server.daemon_threads = False
            server.block_on_close = True

            self._server = server
            self._endpoint = f"http://{self.host}:{server.server_port}"
            self._thread = Thread(
                target=server.serve_forever,
                name=f"comm-network-{self.agent_id}",
                daemon=True,
            )

            http_started = False
            try:
                self._thread.start()
                http_started = True
                self._start_discovery()
            except BaseException:
                self._stop_discovery()
                if http_started:
                    server.shutdown()
                server.server_close()
                if http_started:
                    self._thread.join()
                self._server = None
                self._thread = None
                self._endpoint = None
                raise

            return self.endpoint

    def discover(self) -> list[str]:
        """通过局域网组播收集 0.3 秒节点地址；本机模式直接返回空列表。"""
        if self._closed or self._server is None:
            raise RuntimeError("network transport is not running")
        if self.host.startswith("127."):
            return []

        endpoints: set[str] = set()
        # 查询使用独立端口，避免同机 agent 之间分走对方的单播回复。
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as query:
            query.bind((self.host, 0))
            query.setsockopt(
                socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.host)
            )
            query.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
            query.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            query.sendto(
                self._DISCOVERY_QUERY, (self._DISCOVERY_GROUP, self._DISCOVERY_PORT)
            )
            deadline = time.monotonic() + 0.3
            while (remaining := deadline - time.monotonic()) > 0:
                query.settimeout(remaining)
                try:
                    raw, _ = query.recvfrom(65535)
                except socket.timeout:
                    break
                try:
                    reply = json.loads(raw)
                except (ValueError, UnicodeError):
                    continue
                if (
                    isinstance(reply, dict)
                    and reply.get("protocol") == self._DISCOVERY_PROTOCOL
                    and isinstance(reply.get("agent_id"), str)
                    and reply["agent_id"] != self.agent_id
                    and isinstance(reply.get("endpoint"), str)
                    and reply["endpoint"]
                    and reply["endpoint"] != self.endpoint
                ):
                    endpoints.add(reply["endpoint"])
        return sorted(endpoints)

    def exchange(
        self,
        endpoint: str,
        announcement: dict,
    ) -> list[dict]:
        """提交自己的公告，返回对方当前已知的节点公告。"""
        if not isinstance(announcement, dict):
            raise TypeError("announcement must be an object")
        if announcement.get("agent_id") != self.agent_id:
            raise ValueError("announcement must belong to this agent")
        result = self._post(
            endpoint,
            "/exchange",
            announcement,
        )
        announcements = result.get("announcements")
        if not isinstance(announcements, list) or not announcements or not all(
            isinstance(item, dict) for item in announcements
        ):
            raise RuntimeError("invalid exchange response")
        return announcements

    def send(
        self,
        endpoint: str,
        inbox: dict,
        payload: str,
    ) -> None:
        """向一个远程收件箱投递原始字符串，不解析 payload 的 JSON 字段。"""
        if not isinstance(inbox, dict):
            raise TypeError("inbox must be an object")
        if not isinstance(payload, str):
            raise TypeError("payload must be a string")
        result = self._post(
            endpoint,
            "/message",
            {
                "sender_id": self.agent_id,
                "inbox": inbox,
                "payload": payload,
            },
        )

        if result.get("ok") is not True:
            raise RuntimeError("message was not accepted")

    def close(self) -> None:
        """停止服务并等待已有接收回调结束；不要在回调中调用。"""
        with self._lifecycle_lock:
            if self._closed:
                return

            self._closed = True
            self._stop_discovery()

            if self._server is not None:
                self._server.shutdown()
                self._server.server_close()

            if self._thread is not None:
                self._thread.join()

            self._server = None
            self._thread = None

    @classmethod
    def _resolve_host(cls, host: str | None) -> str:
        if host is not None:
            return socket.gethostbyname(host)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
                # UDP connect 仅选择出站接口，不发送报文或联系外部服务。
                probe.connect((cls._DISCOVERY_GROUP, cls._DISCOVERY_PORT))
                return probe.getsockname()[0]
        except OSError:
            return "127.0.0.1"

    def _start_discovery(self) -> None:
        if self.host.startswith("127."):
            return
        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if sys.platform == "darwin":
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            listener.bind(("", self._DISCOVERY_PORT))
            listener.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_ADD_MEMBERSHIP,
                socket.inet_aton(self._DISCOVERY_GROUP) + socket.inet_aton(self.host),
            )
            listener.settimeout(0.2)
            self._discovery_stop.clear()
            thread = Thread(
                target=self._listen_for_discovery,
                args=(listener,),
                name=f"comm-discovery-{self.agent_id}",
                daemon=True,
            )
            thread.start()
        except BaseException:
            listener.close()
            raise
        self._discovery_socket = listener
        self._discovery_thread = thread

    def _listen_for_discovery(self, listener: socket.socket) -> None:
        reply = json.dumps(
            {
                "protocol": self._DISCOVERY_PROTOCOL,
                "agent_id": self.agent_id,
                "endpoint": self.endpoint,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        while not self._discovery_stop.is_set():
            try:
                query, address = listener.recvfrom(65535)
            except socket.timeout:
                continue
            if query == self._DISCOVERY_QUERY:
                listener.sendto(reply, address)

    def _stop_discovery(self) -> None:
        self._discovery_stop.set()
        if self._discovery_thread is not None:
            self._discovery_thread.join()
            self._discovery_thread = None
        if self._discovery_socket is not None:
            self._discovery_socket.close()
            self._discovery_socket = None

    def _post(
        self,
        endpoint: str,
        path: str,
        body: dict,
    ) -> dict:
        if self._closed or self._server is None:
            raise RuntimeError("network transport is not running")

        raw = json.dumps(
            body,
            separators=(",", ":"),
        ).encode("utf-8")

        request = Request(
            endpoint.rstrip("/") + path,
            data=raw,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )

        # agent 之间直接连接，不使用环境变量中的 HTTP 代理。
        opener = build_opener(ProxyHandler({}))

        try:
            with opener.open(
                request,
                timeout=self.timeout,
            ) as response:
                result = json.loads(response.read())

        except HTTPError as exc:
            with exc:
                detail = exc.read().decode(
                    "utf-8",
                    errors="replace",
                )

            message = f"HTTP {exc.code}: {detail}"

            if exc.code == 409:
                raise BufferError(message) from exc

            raise RuntimeError(message) from exc

        if not isinstance(result, dict):
            raise RuntimeError("response must be an object")

        return result
