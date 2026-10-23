# Comm 接口测试

使用 Python 3.14+ 自带的 `unittest`，不需要安装测试依赖。请在项目根目录运行，支持 macOS/Linux：

```bash
python3 -m unittest discover -s comm/test -t . -v
```

测试按顺序执行，每次使用独立节点 ID 和收件箱，结束时调用 `close()` 清理自己创建的资源。

| 文件 | 覆盖内容 |
| --- | --- |
| `test_comm.py` | 六个业务接口、topic 隔离、payload 原样传递、加入/退出语义、关闭及参数边界 |
| `test_transports.py` | 共享内存收发与资源释放、真实 HTTP 公告交换和收发、旧收件箱拒收、容量溢出 |
| `test_integration.py` | 独立进程无 seeds 发现与通信、同一 topic 混合传输、可选的真实 UDP 组播发现 |

默认执行 22 个测试，跳过 1 个需要局域网接口及组播支持的测试。启用全部 23 个测试：

```bash
COMM_TEST_LAN=1 python3 -m unittest discover -s comm/test -t . -v
```

组播测试使用临时发现端口，启动两个独立进程，屏蔽共享内存公告读取并模拟远程收件箱不可直接访问，验证节点确实通过 UDP 自动发现、HTTP 交换 topic 并双向收发。没有可用局域网 IPv4 路由时会明确跳过；有路由但发现失败时会报告失败。

混合传输测试同样只模拟一个成员的共享内存不可访问，实际执行共享内存和 HTTP 投递，并检查每个成员仅收到一次消息。这些测试在同一台机器上运行，不替代两台物理机器之间的部署验证。
