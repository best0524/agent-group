"""真实跨进程通信；用不可访问收件箱模拟远程节点的路由边界。"""

from contextlib import ExitStack, suppress
import multiprocessing
import os
import socket
import traceback
import unittest
from unittest.mock import patch
from uuid import uuid4

from comm import Comm, Message
from comm.network import NetworkTransport
from comm.shared_memory import SharedMemoryTransport


def _run_peer(connection, options, discovery_port, network_only):
    """独立进程运行真实 Comm，Pipe 仅用来安排测试步骤和读取结果。"""
    try:
        with ExitStack() as stack:
            if discovery_port is not None:
                stack.enter_context(
                    patch.object(NetworkTransport, "_DISCOVERY_PORT", discovery_port)
                )
            if network_only:
                stack.enter_context(
                    patch.object(SharedMemoryTransport, "read_announcements", return_value=[])
                )
                stack.enter_context(
                    patch.object(SharedMemoryTransport, "can_access", return_value=False)
                )
            node = stack.enter_context(Comm(**options))
            connection.send((True, (node.agent_id, node.endpoint)))
            while (request := connection.recv()) is not None:
                method, args = request
                result = getattr(node, method)(*args)
                connection.send((True, result))
    except EOFError:
        pass
    except Exception:
        connection.send((False, traceback.format_exc()))
    finally:
        connection.close()


class _ProcessPeer:
    """给集成测试提供有超时的子进程调用及退出清理。"""

    def __init__(self, *, host, discovery_port=None, network_only=False):
        context = multiprocessing.get_context("spawn")
        self.connection, self._child_connection = context.Pipe()
        self.process = context.Process(
            target=_run_peer,
            args=(
                self._child_connection,
                {"agent_id": f"test-{uuid4().hex}", "host": host, "timeout": 2.0},
                discovery_port,
                network_only,
            ),
        )

    def start(self):
        self.process.start()
        self._child_connection.close()
        self.agent_id, self.endpoint = self._result()

    def call(self, method, *args):
        self.connection.send((method, args))
        return self._result()

    def _result(self):
        if not self.connection.poll(15):
            raise AssertionError("Comm 子进程在 15 秒内未返回结果")
        succeeded, result = self.connection.recv()
        if not succeeded:
            raise AssertionError(f"Comm 子进程异常：\n{result}")
        return result

    def close(self):
        try:
            if self.process.pid is None:
                return
            if self.process.is_alive():
                with suppress(BrokenPipeError, OSError):
                    self.connection.send(None)
            self.process.join(5)
            if self.process.is_alive():
                self.process.terminate()
                self.process.join(5)
                raise AssertionError("Comm 子进程未正常退出")
            if self.process.exitcode != 0:
                raise AssertionError(f"Comm 子进程退出码：{self.process.exitcode}")
        finally:
            self.connection.close()
            self._child_connection.close()


class IntegrationTests(unittest.TestCase):
    def node(self, **options):
        node = Comm(agent_id=f"test-{uuid4().hex}", host="127.0.0.1", **options)
        self.addCleanup(node.close)
        return node

    def peer(self, **options):
        peer = _ProcessPeer(**options)
        self.addCleanup(peer.close)
        peer.start()
        return peer

    def test_independent_process_discovers_and_communicates_without_seeds(self):
        sender = self.node()
        topic_id = sender.create_topic(f"process-{uuid4().hex}", "跨进程发现")
        peer = self.peer(host="127.0.0.1")
        self.assertNotEqual(peer.process.pid, os.getpid())
        self.assertIn(topic_id, {topic.topic_id for topic in peer.call("discover_topics")})
        peer.call("join_topic", topic_id)

        payload = ' {"text": "跨进程", "spacing": [1,  2]}\n'
        # 本机发送应直接写共享内存，不应走 HTTP 消息接口。
        with patch.object(sender._network, "send", side_effect=AssertionError("本机消息走了 HTTP")):
            sender.send(topic_id, payload)
        self.assertEqual(peer.call("receive", topic_id), [Message(sender.agent_id, payload)])
        self.assertEqual(peer.call("receive", topic_id), [])

        peer.call("send", topic_id, "opaque reply, not JSON")
        self.assertEqual(sender.receive(topic_id), [Message(peer.agent_id, "opaque reply, not JSON")])
        peer.call("leave_topic", topic_id)
        sender.leave_topic(topic_id)
        self.assertNotIn(topic_id, {topic.topic_id for topic in sender.discover_topics()})

    def test_topic_routes_to_shared_memory_and_http_members_once(self):
        sender, local, remote, outsider = (self.node() for _ in range(4))
        topic_id = sender.create_topic(f"mixed-{uuid4().hex}")
        other_topic = outsider.create_topic(f"other-{uuid4().hex}")
        local.join_topic(topic_id)
        remote.join_topic(topic_id)
        can_access = sender._shared_memory.can_access

        def accessible(inbox):
            # 只模拟远程内存无法打开，HTTP 服务和实际收件箱仍真实运行。
            return inbox["agent_id"] != remote.agent_id and can_access(inbox)

        with (
            patch.object(sender._shared_memory, "can_access", side_effect=accessible),
            patch.object(sender._shared_memory, "send", wraps=sender._shared_memory.send) as local_send,
            patch.object(sender._network, "send", wraps=sender._network.send) as remote_send,
        ):
            payload = ' {"message": "混合成员"}\n'
            sender.send(topic_id, payload)
            self.assertEqual(local_send.call_count, 1)
            self.assertEqual(local_send.call_args.args[0]["agent_id"], local.agent_id)
            self.assertEqual(remote_send.call_count, 1)
            self.assertEqual(remote_send.call_args.args[0], remote.endpoint)
            self.assertEqual(remote_send.call_args.args[1]["agent_id"], remote.agent_id)

        for receiver in (local, remote):
            self.assertEqual(receiver.receive(topic_id), [Message(sender.agent_id, payload)])
            self.assertEqual(receiver.receive(topic_id), [])
        self.assertEqual(sender.receive(topic_id), [])
        self.assertEqual(outsider.receive(other_topic), [])

    @unittest.skipUnless(os.environ.get("COMM_TEST_LAN") == "1", "设置 COMM_TEST_LAN=1 运行真实组播发现测试")
    def test_multicast_discovers_process_without_seeds_or_local_announcements(self):
        host = NetworkTransport(f"probe-{uuid4().hex}").host
        if host.startswith("127."):
            self.skipTest("没有可用的局域网 IPv4 路由")
        # 使用独立发现端口，避免连接当前局域网中的真实业务节点。
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as reservation:
            reservation.bind(("", 0))
            discovery_port = reservation.getsockname()[1]

        with (
            patch.object(NetworkTransport, "_DISCOVERY_PORT", discovery_port),
            patch.object(SharedMemoryTransport, "read_announcements", return_value=[]),
            patch.object(SharedMemoryTransport, "can_access", return_value=False),
            Comm(host=host, timeout=2.0) as sender,
        ):
            # 两个进程都看不到共享内存公告，必须通过真实 UDP 发现与 HTTP 交换。
            topic_id = sender.create_topic(f"lan-{uuid4().hex}")
            peer = self.peer(host=host, discovery_port=discovery_port, network_only=True)
            try:
                self.assertIn(peer.endpoint, sender._network.discover())
                self.assertNotIn(sender.endpoint, sender._network.discover())
                self.assertIn(topic_id, {topic.topic_id for topic in peer.call("discover_topics")})
                peer.call("join_topic", topic_id)
                payload = ' {"path": "UDP发现 + HTTP消息"}\n'
                sender.send(topic_id, payload)
                self.assertEqual(peer.call("receive", topic_id), [Message(sender.agent_id, payload)])
                peer.call("send", topic_id, "reply")
                self.assertEqual(sender.receive(topic_id), [Message(peer.agent_id, "reply")])
            finally:
                peer.close()


if __name__ == "__main__":
    unittest.main()
