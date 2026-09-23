# Day7：顺序消息（3～4h）

## 今日目标

搞清 RocketMQ 如何保证顺序，以及它保证的是哪种顺序。

## 关联面试题

- 2. RocketMQ 如何保证消息的顺序性？
- 16. 普通消息、顺序消息区别与场景

---

## 1. 先澄清：全局顺序 vs 分区顺序

| 类型 | 含义 | 代价 |
|------|------|------|
| 全局顺序 | 整个 Topic 只有一个队列 | 吞吐低，很少用 |
| 分区顺序 | 同一业务键（如订单号）进同一队列，队列内顺序 | 常用 |

面试一般问的是 **分区顺序**。

```text
订单1001 的创建/支付/发货  ──哈希──▶ 同一个 MessageQueue
订单1002 的创建/支付/发货  ──哈希──▶ 另一个 MessageQueue（可并行）
```

---

## 2. 实现分两半：发送侧 + 消费侧

### 发送侧：锁定队列选择

顺序发送示例：

`example/src/main/java/org/apache/rocketmq/example/ordermessage/Producer.java`

关键点：发送时用 `MessageQueueSelector`，根据业务 ID 选择固定队列。

客户端实现仍在：

`DefaultMQProducerImpl` 的顺序发送相关方法（`send(..., MessageQueueSelector, arg)`）

### 消费侧：同一队列加锁串行消费

`org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService`

关键方法：

- `lockMQPeriodically` / `lockOneMQ`：向 Broker 申请队列锁（集群模式）
- 消费时对本地 `ProcessQueue` 加锁，保证同队列消息一条条处理

Broker 侧也有队列锁相关逻辑（消费者锁定 MessageQueue），避免同组其他实例同时消费该队列。

---

## 3. 为什么这样设计

好处：

- 不同键可以并行，吞吐还行
- 同键严格有序（在不失败乱序重试的前提下）

注意：

- 消费失败重试时，顺序可能被影响（要处理好失败策略）
- 队列锁会导致该队列吞吐受单线程限制
- 扩消费者时，键与队列映射不变才稳（队列数变更会影响哈希）

---

## 4. 本地操作

> 源码里已加 `Day7` 中文注释，IDEA 全局搜 `Day7` 可跳转。

### 操作 A：跑官方顺序示例

1. 打开并设置 namesrv（示例已打开）：
   - `example/.../ordermessage/Producer.java`（Topic=`OrderTopic`）
   - `example/.../ordermessage/Consumer.java`
2. 先启 Consumer，再启 Producer
3. 观察控制台：同一 `orderId`（KEY）的消息是否按顺序出现

### 操作 B：对比普通并发消费

用 `quickstart` 的并发 Consumer：同一 Topic 多线程消费，**不保证**全局顺序，甚至同队列也可能并发（并发消费服务）。

理解：普通消息追求吞吐；顺序消息追求同键有序。

### 操作 C：断点

1. `MessageQueueSelector.select` / `DefaultMQProducerImpl.sendSelectImpl`
2. `ConsumeMessageOrderlyService.lockMQPeriodically` / `lockOneMQ`
3. `ConsumeRequest.run` 里 `processQueue.getConsumeLock()` + `messageListener.consumeMessage`

---

## 5. 使用场景

适合：

- 订单状态机：创建 → 支付 → 完成
- 账户出入账流水
- 库存扣减（同商品键）

不适合：

- 完全无关的日志、通知（用普通并发消息）
- 强行全局顺序（会成为瓶颈）

---

## 6. 面试口述

「RocketMQ 常用分区顺序：生产端用 MessageQueueSelector 把同一业务键哈希到同一队列；消费端用顺序监听器，并对队列加锁串行消费。这样同键有序、不同键并行。全局顺序只能单队列，性能差，一般不用。」

---

## 今日检查清单

- [ ] 能区分全局顺序 / 分区顺序
- [ ] 跑过 `ordermessage` 示例
- [ ] 知道发送选型 + 消费加锁两段式
- [ ] 能说出适用场景

下一篇：[day-08-事务消息.md](./day-08-事务消息.md)
