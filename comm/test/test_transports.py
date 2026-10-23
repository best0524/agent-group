"""直接验证共享内存收件箱和真实 loopback HTTP 传输。"""

import unittest
from uuid import uuid4

from comm.network import NetworkTransport
from comm.shared_memory import SharedMemoryTransport


class SharedMemoryTransportTests(unittest.TestCase):
    def transport(self):
        transport = SharedMemoryTransport(uuid4().hex)
        self.addCleanup(transport.close)
        return transport

    def test_send_preserves_opaque_payload_and_receive_consumes_fifo(self):
        sender = self.transport()
        receiver = self.transport()
        topic_id = uuid4().hex
        inbox = receiver.create_inbox(topic_id)
        payloads = [' { "内容": "你好", "n": 1 }\n', "not JSON", ""]

        self.assertEqual(receiver.create_inbox(topic_id), inbox)
        self.assertTrue(sender.can_access(inbox))
        for payload in payloads:
            sender.send(inbox, payload)

        expected = [
            {"sender_id": sender.agent_id, "payload": payload}
            for payload in payloads
        ]
        self.assertEqual(receiver.receive(topic_id, limit=2), expected[:2])
        self.assertEqual(receiver.receive(topic_id), expected[2:])
        self.assertEqual(receiver.receive(topic_id), [])

    def test_accept_only_writes_the_owners_active_inbox(self):
        sender = self.transport()
        receiver = self.transport()
        other = self.transport()
        topic_id = uuid4().hex
        inbox = receiver.create_inbox(topic_id)
        other.create_inbox(topic_id)
        envelope = {
            "inbox": inbox,
            "sender_id": sender.agent_id,
            "payload": '{"message":"one recipient"}',
        }

        with self.assertRaises(ValueError):
            other.accept(envelope)
        self.assertEqual(receiver.receive(topic_id), [])
        receiver.accept(envelope)

        self.assertEqual(
            receiver.receive(topic_id),
            [{"sender_id": sender.agent_id, "payload": envelope["payload"]}],
        )
        self.assertEqual(other.receive(topic_id), [])

    def test_remove_and_recreate_discards_unread_and_rejects_old_reference(self):
        sender = self.transport()
        receiver = self.transport()
        topic_id = uuid4().hex
        old_inbox = receiver.create_inbox(topic_id)
        sender.send(old_inbox, "unread")

        receiver.remove_inbox(topic_id)
        receiver.remove_inbox(topic_id)
        self.assertFalse(sender.can_access(old_inbox))
        new_inbox = receiver.create_inbox(topic_id)
        self.assertNotEqual(new_inbox, old_inbox)
        self.assertEqual(receiver.receive(topic_id), [])
        with self.assertRaises(ValueError):
            receiver.accept(
                {"inbox": old_inbox, "sender_id": sender.agent_id, "payload": "late"}
            )

        sender.send(new_inbox, "new subscription")
        self.assertEqual(
            receiver.receive(topic_id),
            [{"sender_id": sender.agent_id, "payload": "new subscription"}],
        )

    def test_overflow_preserves_unread_messages_and_allows_later_send(self):
        sender = self.transport()
        receiver = self.transport()
        topic_id = uuid4().hex
        inbox = receiver.create_inbox(topic_id)
        sender.send(inbox, "before overflow")

        with self.assertRaises(BufferError):
            sender.send(inbox, "x" * receiver.INBOX_BYTES)

        sender.send(inbox, "after overflow")
        self.assertEqual(
            receiver.receive(topic_id),
            [
                {"sender_id": sender.agent_id, "payload": "before overflow"},
                {"sender_id": sender.agent_id, "payload": "after overflow"},
            ],
        )

    def test_announcement_replacement_and_close_only_remove_owned_resources(self):
        first = self.transport()
        second = self.transport()
        first_inbox = first.create_inbox(uuid4().hex)
        second_inbox = second.create_inbox(uuid4().hex)
        first_ad = {
            "agent_id": first.agent_id,
            "endpoint": "http://127.0.0.1:9",
            "topics": {},
            "peers": [],
            "old_field": True,
        }
        second_ad = {
            "agent_id": second.agent_id,
            "endpoint": "http://127.0.0.1:9",
            "topics": {},
            "peers": [],
        }
        first.publish_announcement(first_ad)
        second.publish_announcement(second_ad)
        replacement = {
            "agent_id": first.agent_id,
            "endpoint": "http://127.0.0.1:9",
            "topics": {},
            "peers": [],
            "new_field": True,
        }
        first.publish_announcement(replacement)

        own_ads = [
            ad for ad in second.read_announcements()
            if ad["agent_id"] == first.agent_id
        ]
        self.assertEqual(own_ads, [replacement])

        first.remove_announcement()
        first.remove_announcement()
        self.assertNotIn(first.agent_id, {
            ad["agent_id"] for ad in second.read_announcements()
        })
        first.publish_announcement(first_ad)
        first.close()
        first.close()

        ads = {ad["agent_id"]: ad for ad in second.read_announcements()}
        self.assertNotIn(first.agent_id, ads)
        self.assertEqual(ads[second.agent_id], second_ad)
        self.assertFalse(second.can_access(first_inbox))
        self.assertTrue(second.can_access(second_inbox))
        with self.assertRaises(RuntimeError):
            first.create_inbox(uuid4().hex)

    def test_receive_rejects_invalid_limits_without_consuming(self):
        sender = self.transport()
        receiver = self.transport()
        topic_id = uuid4().hex
        inbox = receiver.create_inbox(topic_id)
        sender.send(inbox, "pending")

        for limit in (0, -1, True, 1.5):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                receiver.receive(topic_id, limit=limit)

        self.assertEqual(
            receiver.receive(topic_id),
            [{"sender_id": sender.agent_id, "payload": "pending"}],
        )


class NetworkTransportTests(unittest.TestCase):
    def setUp(self):
        self.left_memory, self.left = self.node()
        self.right_memory, self.right = self.node()
        self.exchanges = {self.left.agent_id: [], self.right.agent_id: []}
        self.announcements = {}
        for network, memory in (
            (self.left, self.left_memory), (self.right, self.right_memory)
        ):
            network.start(
                on_message=memory.accept,
                on_exchange=lambda announcement, agent_id=network.agent_id: (
                    self.record_exchange(agent_id, announcement)
                ),
            )
            self.announcements[network.agent_id] = {
                "agent_id": network.agent_id,
                "endpoint": network.endpoint,
                "topics": {},
            }

    def node(self):
        agent_id = uuid4().hex
        memory = SharedMemoryTransport(agent_id)
        self.addCleanup(memory.close)
        network = NetworkTransport(agent_id, host="127.0.0.1", timeout=2.0)
        self.addCleanup(network.close)
        return memory, network

    def record_exchange(self, agent_id, announcement):
        self.exchanges[agent_id].append(announcement)
        return [self.announcements[agent_id]]

    def test_exchange_delivers_announcements_in_both_directions(self):
        for sender, receiver in (
            (self.left, self.right), (self.right, self.left)
        ):
            with self.subTest(sender=sender.agent_id):
                own = self.announcements[sender.agent_id]
                reply = sender.exchange(receiver.endpoint + "/", own)
                self.assertEqual(reply, [self.announcements[receiver.agent_id]])
                self.assertEqual(self.exchanges[receiver.agent_id], [own])

    def test_http_send_delivers_opaque_payload_once_in_both_directions(self):
        topic_id = uuid4().hex
        left_inbox = self.left_memory.create_inbox(topic_id)
        right_inbox = self.right_memory.create_inbox(topic_id)
        payload = ' { "内容": "网络消息", "nested": "{not parsed}" }\n'

        self.left.send(self.right.endpoint, right_inbox, payload)
        self.right.send(self.left.endpoint, left_inbox, "not JSON")

        self.assertEqual(
            self.right_memory.receive(topic_id),
            [{"sender_id": self.left.agent_id, "payload": payload}],
        )
        self.assertEqual(
            self.left_memory.receive(topic_id),
            [{"sender_id": self.right.agent_id, "payload": "not JSON"}],
        )
        self.assertEqual(self.right_memory.receive(topic_id), [])
        self.assertEqual(self.left_memory.receive(topic_id), [])

    def test_http_rejects_wrong_agent_and_stale_inbox(self):
        topic_id = uuid4().hex
        wrong_inbox = self.left_memory.create_inbox(topic_id)
        old_inbox = self.right_memory.create_inbox(topic_id)
        self.right_memory.remove_inbox(topic_id)
        self.right_memory.create_inbox(topic_id)

        for inbox in (wrong_inbox, old_inbox):
            with self.subTest(inbox=inbox), self.assertRaisesRegex(
                RuntimeError, "HTTP 400"
            ):
                self.left.send(self.right.endpoint, inbox, "rejected")

        self.assertEqual(self.left_memory.receive(topic_id), [])
        self.assertEqual(self.right_memory.receive(topic_id), [])

    def test_http_overflow_maps_to_buffer_error_and_preserves_queue(self):
        topic_id = uuid4().hex
        inbox = self.right_memory.create_inbox(topic_id)
        self.left.send(self.right.endpoint, inbox, "pending")

        with self.assertRaisesRegex(BufferError, "HTTP 409"):
            self.left.send(
                self.right.endpoint, inbox, "x" * self.right_memory.INBOX_BYTES
            )

        self.assertEqual(
            self.right_memory.receive(topic_id),
            [{"sender_id": self.left.agent_id, "payload": "pending"}],
        )


if __name__ == "__main__":
    unittest.main()
