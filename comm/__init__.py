"""供每个 agent-group 进程独立使用的 topic 通信组件。"""

from .comm import Comm
from .models import Message, TopicInfo

__all__ = ["Comm", "Message", "TopicInfo"]
