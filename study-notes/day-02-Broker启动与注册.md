# Day2：Broker 启动与注册（3～4h）

## 今日目标

- 搞清 Broker 启动后做了哪些事
- 搞清「定时注册」如何保活
- 能说清常见集群部署方式（概念层）

## 关联面试题

- 5. RocketMQ 有几种集群方式？
- 7. 工作流程（Broker 这一侧补齐）

---

## 1. 架构思想

Broker 是真正干活的节点。启动时大致做三件事：

1. **加载本地存储**（CommitLog / ConsumeQueue）
2. **启动网络服务**（收 Producer/Consumer 请求）
3. **向 NameServer 注册**，并定时续约

```text
BrokerStartup.main
   └─ createBrokerController
   └─ start(BrokerController)
         ├─ messageStore.start()      // 存储
         ├─ remotingServer.start()    // 10911
         ├─ registerBrokerAll(...)    // 立刻注册一次
         └─ 定时任务：每隔一段时间再 registerBrokerAll
```

### 设计模式

1. **门面（Facade）**：`BrokerController` 把存储、网络、注册、事务等子系统拼起来
2. **模板式启动**：`BrokerStartup` 负责解析参数，`BrokerController` 负责生命周期

---

## 2. 源码入口（按这个顺序点）

### 入口

`org.apache.rocketmq.broker.BrokerStartup`

- `main`
- `createBrokerController(args)`：读 `-n`、`-c` 配置
- `start(controller)`

### 控制器

`org.apache.rocketmq.broker.BrokerController`

重点方法：

| 方法 | 含义 |
|------|------|
| `initialize()` | 创建 MessageStore、注册各种 Processor |
| `start()` | 启动存储和网络，并 `registerBrokerAll` |
| `registerBrokerAll(...)` | 组装 Topic 配置，调用 OuterAPI 注册 |

注册真正发请求的地方：

`org.apache.rocketmq.broker.out.BrokerOuterAPI`

- `registerBrokerAll(...)`
- 内部再调 `registerBroker(...)` 向每个 NameServer 发请求

NameServer 端接住：

`RouteInfoManager.registerBroker(...)`

---

## 3. 为什么要定时注册

Broker 不是「注册一次就永远在线」。

- 大约每 **30 秒** 向 NameServer 再注册一次（心跳）
- NameServer 如果大约 **120 秒** 没收到，就认为 Broker 挂了，从路由表摘掉

好处：

- 不用复杂的集群共识
- 故障自动摘除（最终一致，有短暂窗口）

坏处：

- 摘除有延迟，可能短时间路由指向死人
- 客户端还要靠自己的路由缓存刷新来感知

---

## 4. 集群方式（面试必背，结合配置理解）

RocketMQ 常说的几种：

### 1）单 Master

只有一个 Broker。简单，但挂了就全挂。本地学习就是这种。

### 2）多 Master

多个 Broker 都是 Master。Topic 队列分散到多个 Broker，提高吞吐和可用性（某个 Master 挂了，其他还在）。

### 3）多 Master 多 Slave（异步复制）

每个 Master 有 Slave。Master 挂了还能从 Slave 读（默认不能自动切主写，4.x 传统模式需人工/外部工具；DLedger 模式可自动选主）。

### 4）多 Master 多 Slave（同步复制）

写入要等 Slave 也成功才返回。更安全，更慢。

配置关键词（知道即可）：

- `brokerRole=ASYNC_MASTER / SYNC_MASTER / SLAVE`
- `flushDiskType=ASYNC_FLUSH / SYNC_FLUSH`
- DLedger：基于 Raft 的自动选主（你这版源码有相关依赖，深入放到 Day10）

---

## 5. 本地操作

> 源码里已加 `Day2` 中文注释，IDEA 全局搜 `Day2` 可跳转。

### 操作 A：观察启动断点

1. 断点打在：
   - `BrokerStartup.main` / `createBrokerController`
   - `BrokerController.initialize`
   - `BrokerController.start`
   - `BrokerController.registerBrokerAll`
   - `BrokerOuterAPI.registerBrokerAll`
2. Debug 启动 Broker
3. 单步看：先 `messageStore.start`，再网络 start，再注册

### 操作 B：故意制造「注册失败」

1. **先别启 NameServer**，只启 Broker
2. 看控制台/日志会出现连不上 NameServer 的告警
3. 再启 NameServer，等一会儿（或重启 Broker）
4. 再执行：

```bash
sh bin/mqadmin clusterList -n 127.0.0.1:9876
```

理解：Broker 可以启动，但没注册成功时，客户端找不到它。

### 操作 C：确认你的本地配置

打开：

`distribution/target/rocketmq-4.9.4/rocketmq-4.9.4/conf/broker-local.conf`

确认有：

```properties
brokerIP1 = 127.0.0.1
brokerName = broker-a
brokerClusterName = DefaultCluster
```

如果注册成 `10.x.x.x`，本机客户端经常连不上。这是很多人第一次踩的坑。

---

## 6. 面试怎么答（集群方式）

「常见四种：单 Master；多 Master；多 Master 多 Slave 异步复制；多 Master 多 Slave 同步复制。生产一般多 Master，重要数据再加同步复制或 DLedger。NameServer 建议至少两台，Broker 向所有 NameServer 注册。」

---

## 今日检查清单

- [ ] 能说出 Broker 启动三步：存储、网络、注册
- [ ] 找到 `registerBrokerAll` 并打断点跟过一次
- [ ] 能说出 30s 注册 / 120s 过期的大致含义
- [ ] 能背出 4 种集群方式及适用场景

下一篇：[day-03-消息发送全链路.md](./day-03-消息发送全链路.md)
