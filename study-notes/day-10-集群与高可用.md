# Day10：集群与高可用（3～4h）

## 今日目标

把刷盘、复制、主从、DLedger 和「消息不丢失」串起来。  
今天偏原理 + 配置，本地单机也能学，不必真搭多机。

## 关联面试题

- 5. 有几种集群方式？
- 3. 如何保证消息不丢失？（端到端完整版）

---

## 1. 可靠性的三道闸

```text
Producer 同步发送成功
        │
        ▼
Broker 刷盘（SYNC_FLUSH 更稳）
        │
        ▼
复制到 Slave（SYNC 复制更稳）
        │
        ▼
Consumer 消费成功并提交位点
```

任何一环用「异步/单向」，都可能在对应故障下丢或重复。

---

## 2. 主从复制源码入口

`org.apache.rocketmq.store.ha.HAService`

作用直观理解：

- Master 把 CommitLog 增量发给 Slave
- Slave 落盘后汇报进度
- 同步复制时，Master 要等到 Slave 确认才给 Producer 成功（视配置）

角色配置：

- `brokerRole=ASYNC_MASTER`：异步复制 Master
- `brokerRole=SYNC_MASTER`：同步复制 Master
- `brokerRole=SLAVE`：从节点

---

## 3. 集群方式（完整口述）

### 单 Master

研发调试够用。生产有单点。

### 多 Master

无 Slave。某 Master 挂了，它上面的队列暂时不可用，但其它 Master 仍可用。  
注意：消息本身不在其它 Master 上有副本（除非业务多写）。

### 多 Master 多 Slave（异步复制）

有副本，性能好；Master 宕机瞬间，Slave 可能少最后几条。

### 多 Master 多 Slave（同步复制）

更安全，延迟更高。

### DLedger（了解）

基于 Raft 的 CommitLog 复制与选主，故障时可自动切换。  
你这份 4.9.4 依赖了 dledger，深入可读 `store` 与 conf 下 dledger 示例配置。面试说到「支持基于 Raft 的自动故障转移」即可。

NameServer：

- 建议 2 台以上
- 本身无状态，Broker 向所有 NameServer 注册

---

## 4. 本地操作（单机模拟理解）

### 操作 A：读默认配置，标出关键项

打开发行包配置目录：

`distribution/target/rocketmq-4.9.4/rocketmq-4.9.4/conf/`

对比：

- `broker.conf`
- `broker-a.properties` / `broker-b.properties`（如果有）
- `2m-2s-async` / `2m-2s-sync` 目录（有的话）

把下面几项抄到笔记：

```properties
brokerClusterName
brokerName
brokerId          # 0=Master，非0=Slave
brokerRole
flushDiskType
```

### 操作 B：端到端「不丢失」对照表

自己填空：

| 环节 | 你的选择 | 故障时会怎样 |
|------|----------|--------------|
| 发送 | SYNC / ASYNC / ONEWAY | |
| 刷盘 | SYNC / ASYNC | |
| 复制 | SYNC / ASYNC / 无 | |
| 消费 | 成功后提交位点 | 失败重试/重复 |

### 操作 C：看 HA 类（不必跑双机）

IDEA 打开 `HAService.java`，浏览内部类名：

- 接受 Slave 连接
- 写传输
- 读 Slave 进度

把类图用文字记下来即可。

---

## 5. 面试口述（不丢失完整版）

「不丢失要看全链路：生产用同步发送并处理失败重试；Broker 用同步刷盘，重要场景再加同步复制或 DLedger；消费成功后再提交位点，并且业务幂等。任何一环异步，都要接受对应风险。」

---

## 今日检查清单

- [ ] 能画出「发送-刷盘-复制-消费」四道闸
- [ ] 能说出 4 种经典集群 + DLedger 一句话
- [ ] 打开过 `HAService`
- [ ] 能把配置项 `brokerRole`/`flushDiskType` 对上含义

下一篇：[day-11-丢消息重复削峰坑.md](./day-11-丢消息重复削峰坑.md)
