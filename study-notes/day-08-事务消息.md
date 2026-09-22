# Day8：事务消息（3～4h）

## 今日目标

搞清半消息、本地事务、提交/回滚、回查，不把它说成 XA。

## 关联面试题

- 1. RocketMQ 的事务消息是如何实现的？
- 12. 和 Kafka 事务消息有什么区别？

---

## 1. 要解决什么问题

业务典型诉求：

> 本地数据库下单成功，和发 MQ 消息，要么都成功，要么都当没发生。

如果先发消息再改库：库失败，消息脏了。  
如果先改库再发消息：消息失败，库已改。

RocketMQ 事务消息给了一个折中方案（最终一致）：

```text
1) 发「半消息」到 Broker（消费者暂时看不到）
2) 执行本地事务（改数据库）
3) 成功则 Commit 半消息（消费者可见）
   失败则 Rollback（删除/标记，消费者永远看不到）
4) 如果网络断开，Broker 会回查生产者：本地事务到底成功没？
```

---

## 2. 源码入口

### 示例（先跑起来）

- `example/.../transaction/TransactionProducer.java`
- `example/.../transaction/TransactionListenerImpl.java`

`TransactionListener` 两个关键回调：

- `executeLocalTransaction`：执行本地事务，返回 COMMIT/ROLLBACK/UNKNOWN
- `checkLocalTransaction`：Broker 回查时问你

### Broker 侧

`org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl`

关键方法：

| 方法 | 作用 |
|------|------|
| `prepareMessage` / `asyncPrepareMessage` | 存半消息 |
| `commitMessage` / `rollbackMessage` | 提交或回滚 |
| `check(...)` | 扫描半消息并回查生产者 |

发送入口仍在 `SendMessageProcessor`：如果是事务 prepare，会走 `TransactionalMessageService`，不是直接普通 `putMessage`。

半消息会先落到内部 Topic（实现细节面试说到「消费者不可见的半消息」即可，不必背内部 topic 名到一字不差，但源码里能看到 `rmq_sys_TRANS_HALF_TOPIC` 一类常量）。

---

## 3. 和 Kafka 事务的区别（面试高频）

| | RocketMQ 事务消息 | Kafka 事务 |
|--|-------------------|------------|
| 目标 | 本地 DB 与发消息的最终一致 | 多分区原子写入 / 精确一次语义相关 |
| 模型 | 半消息 + 本地事务 + 回查 | 基于 Transaction Coordinator、生产者事务 ID |
| 典型用法 | 下单后发下游通知 | 流处理、多主题原子写出 |
| 回查 | 有，Broker 主动问 Producer | 不是这种 DB 回查模型 |

一句话：

> RocketMQ 事务消息更偏「业务本地事务 + 消息」；Kafka 事务更偏「Kafka 内部多分区原子写」。

---

## 4. 本地操作

### 操作 A：跑事务示例

1. NameServer + Broker 先起来
2. `TransactionListenerImpl` 里看三种返回值分支（成功/失败/未知）
3. 先启一个普通 Consumer 订阅事务示例 Topic（示例里 topic 名以代码为准）
4. 再跑 `TransactionProducer`
5. 观察：本地事务失败时，消费者收不到；成功时收得到

### 操作 B：制造回查

在 `executeLocalTransaction` 返回 `UNKNOW`（或让它超时不二次确认），观察 Broker 是否回调 `checkLocalTransaction`。  
可在这两个方法打断点。

### 操作 C：断点

1. `TransactionListenerImpl.executeLocalTransaction`
2. `TransactionalMessageServiceImpl.prepareMessage`
3. `TransactionalMessageServiceImpl.check`

---

## 5. 面试口述（建议背这个结构）

「RocketMQ 事务消息分三步：先发半消息，消费者不可见；再执行本地事务；根据结果 commit 或 rollback。如果生产者挂了或返回未知，Broker 会按策略回查本地事务状态。它解决的是本地事务和发消息的最终一致性，不是数据库 XA。」

---

## 今日检查清单

- [ ] 能画出半消息流程图
- [ ] 跑过 transaction 示例
- [ ] 知道回查入口 `checkLocalTransaction`
- [ ] 能对比 Kafka 事务（至少 2 点）

下一篇：[day-09-延时消息.md](./day-09-延时消息.md)
