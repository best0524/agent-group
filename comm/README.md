# Comm MVP

每个 `agent-group` 进程创建一个 `Comm` 实例，由上层 `agent/` 调用。
每个实例都能独立创建、发现和加入 topic，没有中心服务或 topic 管理者。

运行环境：Python 3.14+，macOS/Linux。通信模块只使用 Python 标准库，无第三方依赖。
共享内存使用 `multiprocessing.shared_memory`，进程互斥通过 `ctypes` 调用系统的命名信号量，不需要共享目录或额外服务进程。

## 上层如何接入

```python
from comm import Comm

with Comm() as comm:
    print(comm.agent_id, comm.endpoint)
    topic_id = comm.create_topic("任务讨论")
    comm.send(topic_id, '{"task": "hello"}')
    messages = comm.receive(topic_id)
```

`Comm()` 构造时自动生成节点 ID、选择本机 IPv4 地址、分配网络端口、发布本机公告并发现已有节点。
上层保存这个实例，在自己的业务循环中调用接口；通信组件不负责调度 agent 的业务。
同机和同一局域网中的不同机器，都可以直接使用 `Comm()`，不需要手动填写 `seeds`。
同机进程读取共享内存公告；跨机器通过 UDP 组播查询节点地址，再进行 HTTP 公告交换。

默认使用系统路由选出的一个网卡。多网卡或 VPN 场景需要指定目标局域网网卡时，可以填写 `host`，仍然无需填写其他 agent 的地址：

```python
# 机器 A 上的项目进程。
node_a = Comm(host="192.168.1.10")

# 机器 B 上的另一个项目进程。
node_b = Comm(host="192.168.1.20")
```

这里的地址仅为示例，需要替换为实际本机地址。自动发现需要节点处在组播可达的同一局域网；路由器、无线网络或虚拟网络禁用组播时，不能依靠这个机制发现其他机器。
发现使用固定地址 `239.255.42.99:42429/UDP`、TTL=1，只交换节点 ID 和 HTTP 地址，业务消息仍通过点对点连接传递。
同机多个进程可以共用发现端口，每次查询使用独立临时端口接收各节点的响应。

`seeds` 保留为可选入口，例如 `Comm(seeds=["http://192.168.1.10:9001"])`。seed 可以是任意已有节点，不是中心注册服务或全部 agent 的静态名单。
显式设置 `host="127.0.0.1"` 时只运行本机模式，不启动局域网发现；没有网络路由时默认也使用本机模式。

构造参数：`Comm(agent_id=None, host=None, port=0, seeds=(), timeout=5.0)`。
手动指定的 `agent_id` 必须在正在运行的节点中唯一；不指定时自动生成。

## 业务接口

| 方法 | 返回值 | 语义 |
| --- | --- | --- |
| `create_topic(name, description="")` | `str` | 生成唯一 topic ID，并自动加入 |
| `discover_topics()` | `list[TopicInfo]` | 返回当前发现范围内仍有成员的 topic |
| `join_topic(topic_id)` | `None` | 加入已有 topic；重复加入不清空消息 |
| `send(topic_id, payload)` | `None` | 向其他已加入成员发送字符串，不给自己回送 |
| `receive(topic_id, limit=100)` | `list[Message]` | 消费最多 limit 条消息，没有消息时返回空列表 |
| `leave_topic(topic_id)` | `None` | 退出并丢弃未读消息；重复退出无副作用 |
| `close()` | `None` | 停止接收并释放自己的资源；支持重复关闭 |

`TopicInfo` 包含 `topic_id`、`name`、`description`。
`Message` 包含 `sender_id`、`payload`。payload 的空格、转义和内容都保持原样，不校验或解析其中的 JSON 字段。
订阅不提供历史回放；退出后重新加入会创建新的收件箱。
创建者退出后，其他成员仍携带 topic 信息；全部成员退出后，该 topic 不再出现在发现结果中。

## 发现与发送

- 本机进程独立占用共享内存公告槽，发布自己的 ID、地址、订阅和已知节点地址。
- 初始化和发现 topic 时查询局域网，网络握手交换公告并继续联系新发现的节点；本轮直接确认的公告用于计算成员。
- 新节点启动时会向发现的节点握手，已有节点由此记住它。发送时刷新已知节点，不必每条消息都等待一次组播查询。
- 首次加入会先发现 topic；只保留 HTTP 和 UDP 接收线程，没有周期心跳或后台同步。
- 每个 `(topic_id, agent_id)` 有一个共享内存收件箱。能打开并校验目标收件箱就走共享内存，否则向目标网络地址发送。
- 网络接收只写入自己的同一个收件箱，不广播或再次转发；`receive` 因此统一接收本地和远程消息。

队列使用进程锁保护的 JSON 消息列表，每次读写更新整个列表。默认每个收件箱分配 1 MiB，本机最多有 64 个公告槽，每槽 64 KiB。
超过共享内存容量抛出 `BufferError`。参数或订阅错误使用 `ValueError` / `TypeError`，关闭后调用或协议错误使用 `RuntimeError`。
发送逐个投递，不自动重试；中途报错时，之前的接收方可能已经收到消息。

使用 `with Comm(...)` 或在上层退出时调用 `close()`。MVP 只处理正常资源清理，不包含异常进程退出后的资源回收、消息持久化或故障恢复。
