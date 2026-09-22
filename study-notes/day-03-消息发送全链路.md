# Day3：消息发送全链路（3～4h）

## 今日目标

跟完「Producer.send → Broker 收包 → 准备写入存储」这条链。  
今天先走到写入入口，落盘细节留给 Day4。

## 关联面试题

- 3. RocketMQ 如何保证消息不丢失？（发送侧）
- 8. 消息怎么分发？
- 14. 丢消息可能原因（发送阶段）

---

## 1. 用人话讲发送

```text
1) Producer 问 NameServer：TopicTest 有哪些队列？
2) 选一个 MessageQueue（默认轮询）
3) 把消息发给对应 Broker
4) Broker 的 SendMessageProcessor 处理
5) 调用 MessageStore.putMessage / asyncPutMessage
6) 返回 SendResult（SEND_OK 等）
```

三种发送方式：

| 模式 | 含义 | 可靠性 |
|------|------|--------|
| 同步 SYNC | 等 Broker 返回 | 最高（发送侧） |
| 异步 ASYNC | 回调通知 | 中高，看回调怎么处理 |
| 单向 ONEWAY | 发出去不管 | 最低，日志类可用 |

---

## 2. 源码入口（按断点顺序）

### 客户端

1. Example：`org.apache.rocketmq.example.quickstart.Producer#main`
2. `DefaultMQProducer#send(Message)`
3. **核心**：`org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#sendDefaultImpl`

在 `sendDefaultImpl` 里你会看到大致逻辑：

- 找 Topic 路由（没有就问 NameServer）
- `selectOneMessageQueue` 选队列
- 按同步/异步/单向调用 `sendKernelImpl`
- 失败可能重试（换队列）

路由刷新相关：

`MQClientInstance#updateTopicRouteInfoFromNameServer`

### Broker 端

1. Netty 收到请求后进 Processor
2. **核心**：`org.apache.rocketmq.broker.processor.SendMessageProcessor#processRequest`
3. 同步路径：`sendMessage(...)`
4. 异步路径：`asyncSendMessage(...)`
5. 最终：`messageStore.putMessage(...)` 或 `asyncPutMessage(...)`

事务消息会走：

`TransactionalMessageService.prepareMessage / asyncPrepareMessage`  
（Day8 再细看，今天知道分叉即可）

---

## 3. 设计思想与模式

1. **无状态发送 + 客户端负载均衡**：分发决策在 Producer，不在 NameServer
2. **失败重试换队列**：一个 Broker 不行，试同 Topic 其他队列
3. **钩子（Hook）**：发送前后可插业务逻辑（`SendMessageHook`）

好处：

- NameServer 不成为消息热点
- Producer 可水平扩展
- 发送失败有重试策略，降低瞬时故障影响

---

## 4. 本地操作（今天最重要）

### 准备

1. Debug 启 NameServer、Broker
2. Example Producer 设置：`setNamesrvAddr("127.0.0.1:9876")`
3. 建议先把 `MESSAGE_COUNT` 改成 `10`，方便跟断点

### 断点建议（由外到内）

1. `DefaultMQProducerImpl.sendDefaultImpl`（方法入口）
2. `SendMessageProcessor.processRequest`
3. `SendMessageProcessor.sendMessage` 或 `asyncSendMessage`
4. `DefaultMessageStore.putMessage` / `asyncPutMessage`（进存储的大门）

### 操作步骤

1. 先 Debug 跑 Producer
2. 观察是否先去拉路由，再选定 `brokerName` / `queueId`
3. 断点进到 Broker 后，看 `msgId`、`topic`、`queueId`
4. 看控制台 `SendResult`：

```text
sendStatus=SEND_OK
```

### 再做一个对比实验：ONEWAY

打开：`example/.../simple/OnewayProducer.java`  
设置 namesrv，发送后 **没有** SendResult。  
理解：单向发送丢了你也不知道——这就是「可能丢消息」的一种原因。

---

## 5. 消息分发怎么理解（面试）

「分发」不是 NameServer 把消息转发出去，而是：

1. Topic 被分成多个 MessageQueue
2. Producer 选择队列（轮询 / 指定 / 订单 ID 哈希）
3. 消息进入该队列所属 Broker

所以：

- 提高并发：加队列数、加 Broker
- 要顺序：同一业务键进同一队列（Day7）

---

## 6. 发送侧如何尽量不丢（面试要点）

1. 用同步发送，并判断 `SendStatus.SEND_OK`
2. 开启重试（默认有）；业务要做幂等，因为可能重复
3. 不要用 ONEWAY 传重要数据
4. 发送失败要有本地补偿（落库重试 / 告警）

注意：`SEND_OK` 只表示 Broker 处理成功返回了。  
如果 Broker 是异步刷盘，进程立刻断电仍可能丢——Day4/Day10 继续讲。

---

## 今日检查清单

- [ ] 能说出同步/异步/单向差别
- [ ] 断点走过 `sendDefaultImpl` → `SendMessageProcessor`
- [ ] 知道队列选择发生在客户端
- [ ] 能解释 `SEND_OK` 不等于「绝对已落盘」

下一篇：[day-04-存储CommitLog.md](./day-04-存储CommitLog.md)
