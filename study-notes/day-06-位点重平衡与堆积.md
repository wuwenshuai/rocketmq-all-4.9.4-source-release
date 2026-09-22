# Day6：位点、重平衡、堆积（3～4h）

## 今日目标

- 搞清消费进度（offset）存在哪
- 搞清集群消费如何给消费者分队列
- 会排查「消息堆积」

## 关联面试题

- 6. 消息堆积了怎么解决？
- 13. RocketMQ 和 Kafka 一样有重平衡问题吗？
- 15. 重复消费可能原因

---

## 1. 位点（offset）是什么

每个 **消费组 + 队列** 都有一个进度：「我消费到哪里了」。

集群模式下，进度存在 Broker 上（默认），对应类：

`org.apache.rocketmq.broker.offset.ConsumerOffsetManager`

本地文件大致在：

```bash
ls ~/store/config/
# consumerOffset.json 之类
cat ~/store/config/consumerOffset.json | head
```

广播模式：每个消费者自己维护进度（本地），互不影响。

---

## 2. 重平衡（Rebalance）：分队列，不是分消息

Kafka 常被吐槽 Stop-The-World 重平衡。RocketMQ 不一样：

- 分配的是 **MessageQueue**
- 一个队列同一时刻只被同组内一个消费者处理（集群模式）
- 消费者上下线时重新分配队列

```text
Topic 有 4 个队列：Q0 Q1 Q2 Q3
消费者组有 2 个实例：C1 C2

可能分配：
C1 -> Q0 Q1
C2 -> Q2 Q3
```

源码：

`org.apache.rocketmq.client.impl.consumer.RebalanceImpl#doRebalance`

触发入口：

`DefaultMQPushConsumerImpl#doRebalance`

分配策略常见：平均分配（`AllocateMessageQueueAveragely`）等。

### 和 Kafka 比（面试）

| | RocketMQ | Kafka |
|--|----------|-------|
| 分配单位 | 队列 | 分区（类似） |
| 典型痛点 | 队列数 < 消费者数时有人空闲 | 重平衡期间可能暂停消费较明显 |
| 顺序 | 队列内顺序 + 消费者锁队列 | 分区内顺序 |

RocketMQ 也有重平衡，但机制更轻；**队列数要 ≥ 消费者实例数**，否则浪费实例。

---

## 3. 堆积怎么理解

堆积 ≈ 队列最大位点 - 消费组已提交位点（对每个队列求和/观察）。

常见原因：

1. 消费太慢（业务接口慢、线程数太少）
2. 消费失败反复重试
3. 某个消费者挂了，队列堆积在别人身上不均
4. 发送突刺，消费来不及（削峰场景，Day11）

---

## 4. 本地操作

### 操作 A：制造一点堆积

1. Consumer 里把消费逻辑改成 `Thread.sleep(2000)`（故意变慢）
2. Producer 快速发 500 条
3. 另开终端：

```bash
cd /Users/wl/Desktop/code/rocketmq-all-4.9.4-source-release/distribution/target/rocketmq-4.9.4/rocketmq-4.9.4
sh bin/mqadmin consumerProgress -n 127.0.0.1:9876 -g <你的consumerGroup>
```

观察 diff（堆积数）是否变大，再变小。

### 操作 B：断点看重平衡

1. 断点：`RebalanceImpl.doRebalance`
2. 启动第一个 Consumer
3. 再启动第二个同组 Consumer（改个实例，同 group）
4. 看日志里队列重新分配

### 操作 C：看位点文件变化

消费一段时间后：

```bash
cat ~/store/config/consumerOffset.json
```

对照你的 group / topic / queue。

---

## 5. 堆积怎么解决（面试实战答法）

临时：

1. 扩消费者实例（同时加队列数，否则扩不动）
2. 加大消费线程（`consumeThreadMin/Max`）
3. 优化业务耗时；能批量就批量

治本：

1. 评估 Topic 队列数是否合理
2. 失败消息隔离（改逻辑 / 死信）
3. 观察是否单队列热点（顺序消息场景常见）

命令排查三件套：

```bash
sh bin/mqadmin consumerProgress -n 127.0.0.1:9876 -g group
sh bin/mqadmin brokerStatus -n 127.0.0.1:9876 -b 127.0.0.1:10911
sh bin/mqadmin topicStatus -n 127.0.0.1:9876 -t TopicTest
```

---

## 6. 重复消费常见原因（先记结论）

1. 业务处理成功了，但提交 offset 失败 / 进程被杀
2. 消费返回失败，Broker/客户端重投
3. 生产者重试导致消息本身重复（要业务幂等）
4. 重平衡时旧消费者还在处理，新消费者又拉到重叠区间（少见但要有幂等）

RocketMQ **不保证恰好一次**，保证的是「至少一次」倾向，业务必须幂等。

---

## 今日检查清单

- [ ] 知道 offset 默认落在 Broker 的 `ConsumerOffsetManager`
- [ ] 断点或日志见过 `doRebalance`
- [ ] 会用 `consumerProgress` 看堆积
- [ ] 能对比 RocketMQ / Kafka 重平衡差异（各说 2 句）

下一篇：[day-07-顺序消息.md](./day-07-顺序消息.md)
