"""Comm 公开接口测试；使用 loopback，不依赖局域网或外部节点。"""

import unittest
from uuid import uuid4

from comm import Comm, Message, TopicInfo


class CommAPITests(unittest.TestCase):
    def node(self) -> Comm:
        node = Comm(agent_id=f"test-{uuid4().hex}", host="127.0.0.1")
        self.addCleanup(node.close)
        return node

    def test_create_autojoins_and_discovers_distinct_topics_with_same_name(self):
        creator = self.node()
        name = f"讨论-{uuid4().hex}"
        first = creator.create_topic(name, "主题说明")
        second = creator.create_topic(name)
        observer = self.node()

        self.assertNotEqual(first, second)
        found = {topic.topic_id: topic for topic in observer.discover_topics()}
        self.assertEqual(found[first], TopicInfo(first, name, "主题说明"))
        self.assertEqual(found[second], TopicInfo(second, name))
        self.assertEqual(creator.receive(first), [])
        self.assertEqual(creator.receive(second), [])

    def test_send_preserves_opaque_payload_fifo_and_receive_limit(self):
        sender, receiver = self.node(), self.node()
        topic = sender.create_topic("原样传输")
        receiver.join_topic(topic)
        payloads = [' { "内容": "你好", "count": 1 }\n', "not JSON {", ""]

        for payload in payloads:
            sender.send(topic, payload)

        self.assertEqual(sender.receive(topic), [])  # 发送者没有自回环。
        self.assertEqual(
            receiver.receive(topic, limit=2),
            [Message(sender.agent_id, payload) for payload in payloads[:2]],
        )
        self.assertEqual(
            receiver.receive(topic), [Message(sender.agent_id, payloads[2])]
        )
        self.assertEqual(receiver.receive(topic), [])  # 消费后不会重复返回。

    def test_messages_are_isolated_to_subscribed_topic(self):
        sender, first_member, second_member = self.node(), self.node(), self.node()
        first = sender.create_topic("topic A")
        second = sender.create_topic("topic B")
        first_member.join_topic(first)
        second_member.join_topic(second)

        sender.send(first, "only A")
        sender.send(second, "only B")

        self.assertEqual(first_member.receive(first), [Message(sender.agent_id, "only A")])
        self.assertEqual(second_member.receive(second), [Message(sender.agent_id, "only B")])
        with self.assertRaises(ValueError):
            second_member.receive(first)
        with self.assertRaises(ValueError):
            first_member.send(second, "not a member")

    def test_join_has_no_history_and_repeated_join_keeps_unread_messages(self):
        sender, receiver = self.node(), self.node()
        topic = sender.create_topic("加入时机")
        sender.send(topic, "before join")

        receiver.join_topic(topic)
        self.assertEqual(receiver.receive(topic), [])
        sender.send(topic, "after join")
        receiver.join_topic(topic)

        self.assertEqual(
            receiver.receive(topic), [Message(sender.agent_id, "after join")]
        )

    def test_leave_discards_unread_messages_and_rejoin_starts_empty(self):
        sender, receiver = self.node(), self.node()
        topic = sender.create_topic("退出和重入")
        receiver.join_topic(topic)
        sender.send(topic, "unread")

        receiver.leave_topic(topic)
        receiver.leave_topic(topic)
        sender.send(topic, "while absent")
        with self.assertRaises(ValueError):
            receiver.receive(topic)
        receiver.join_topic(topic)
        self.assertEqual(receiver.receive(topic), [])

        sender.send(topic, "after rejoin")
        self.assertEqual(
            receiver.receive(topic), [Message(sender.agent_id, "after rejoin")]
        )

    def test_topic_survives_creator_close_until_last_member_leaves(self):
        creator, member, newcomer = self.node(), self.node(), self.node()
        topic = creator.create_topic("成员维护的 topic", "没有中心 owner")
        member.join_topic(topic)
        creator.close()

        self.assertIn(topic, {item.topic_id for item in newcomer.discover_topics()})
        newcomer.join_topic(topic)
        member.send(topic, "creator is gone")
        self.assertEqual(
            newcomer.receive(topic), [Message(member.agent_id, "creator is gone")]
        )

        member.leave_topic(topic)
        self.assertIn(topic, {item.topic_id for item in member.discover_topics()})
        newcomer.leave_topic(topic)
        self.assertNotIn(topic, {item.topic_id for item in member.discover_topics()})

    def test_unknown_topic_cannot_be_joined_sent_or_received(self):
        node = self.node()
        missing = uuid4().hex
        with self.assertRaises(ValueError):
            node.join_topic(missing)
        with self.assertRaises(ValueError):
            node.send(missing, "payload")
        with self.assertRaises(ValueError):
            node.receive(missing)
        node.leave_topic(missing)

    def test_invalid_inputs_are_rejected_without_consuming_messages(self):
        sender, receiver = self.node(), self.node()
        with self.assertRaises(ValueError):
            sender.create_topic("")
        with self.assertRaises(TypeError):
            sender.create_topic("name", description=123)

        topic = sender.create_topic("参数检查")
        receiver.join_topic(topic)
        with self.assertRaises(TypeError):
            sender.send(topic, {"not": "a string"})
        sender.send(topic, "still queued")
        for limit in (0, -1, True, 1.5):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                receiver.receive(topic, limit=limit)
        self.assertEqual(
            receiver.receive(topic), [Message(sender.agent_id, "still queued")]
        )

    def test_close_is_idempotent_and_rejects_further_api_calls(self):
        node = self.node()
        topic = node.create_topic("关闭")
        node.close()
        node.close()

        calls = {
            "create_topic": lambda: node.create_topic("after close"),
            "discover_topics": node.discover_topics,
            "join_topic": lambda: node.join_topic(topic),
            "send": lambda: node.send(topic, "payload"),
            "receive": lambda: node.receive(topic),
            "leave_topic": lambda: node.leave_topic(topic),
        }
        for name, call in calls.items():
            with self.subTest(method=name), self.assertRaises(RuntimeError):
                call()

    def test_context_manager_closes_on_exception_without_suppressing_it(self):
        node = self.node()
        with self.assertRaisesRegex(ValueError, "caller failure"):
            with node as entered:
                self.assertIs(entered, node)
                node.create_topic("context")
                raise ValueError("caller failure")
        with self.assertRaises(RuntimeError):
            node.discover_topics()


if __name__ == "__main__":
    unittest.main()
