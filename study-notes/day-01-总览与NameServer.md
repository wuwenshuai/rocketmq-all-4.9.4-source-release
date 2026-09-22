# Day1：总览 + NameServer 路由（3～4h）

## 今日目标

- 能画出 RocketMQ 四大角色关系
- 知道 NameServer **不存消息**，只存路由
- 能在源码里找到：Broker 注册、客户端查路由

## 关联面试题

- 7. 介绍一下 RocketMQ 的工作流程？
- 8. RocketMQ 怎么实现消息分发的？（先理解「先找路由再发」）

---

## 1. 架构思想（先用人话讲）

把 RocketMQ 想成「外卖平台」：

| 角色 | 类比 | 干什么 |
|------|------|--------|
| NameServer | 商家黄页 | 记着哪个 Broker 在哪个地址、有哪些 Topic |
| Broker | 商家后厨 | 真正收消息、存消息、给消费者 |
| Producer | 顾客下单 | 问黄页要地址，再把订单送到后厨 |
| Consumer | 骑手取餐 | 问黄页有哪些队列，再去后厨拉单 |

**设计思想：计算和路由分离。**

- Kafka 有 Controller / ZooKeeper 管元数据
- RocketMQ 用无状态的 NameServer：挂一台，别的 NameServer 还在；Broker 会重新注册

```text
          注册/心跳(约30秒一次)
Broker ─────────────────────▶ NameServer1
   │                          NameServer2
   │                          NameServer3
   │
   │   客户端自己缓存路由，定期刷新
   ▼
Producer / Consumer
```

### 这里用到的设计模式

1. **无中心注册表（Registry）**：NameServer 之间不互相同步（4.x），Broker 向所有 NameServer 注册
2. **请求处理器模式（Processor）**：各种请求码对应不同处理方法（后面 Remoting 层常见）

---

## 2. 源码入口：NameServer 怎么启动

### 入口类

`org.apache.rocketmq.namesrv.NamesrvStartup`

关键方法：

1. `main` → `main0`
2. `createNamesrvController`：读配置、创建控制器
3. `start(NamesrvController)`：真正启动网络服务

然后看：

`org.apache.rocketmq.namesrv.NamesrvController`

关键方法：

- `initialize()`：初始化路由管理、KV 配置、Netty 服务端、注册请求处理器
- `start()`：启动 Netty，开始监听 `9876`

### 路由核心类（今天最重要）

`org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager`

它内存里大概有这些表（名字以源码字段为准，读类开头注释/字段即可）：

- `topicQueueTable`：Topic → 队列信息
- `brokerAddrTable`：Broker 名 → 地址
- `clusterAddrTable`：集群 → Broker 集合
- `brokerLiveTable`：Broker 是否还活着（上次心跳时间）

关键方法：

| 方法 | 干什么 |
|------|--------|
| `registerBroker(...)` | Broker 注册/心跳时更新路由 |
| `pickupTopicRouteData(topic)` | 客户端来查 Topic 路由 |
| `scanNotActiveBroker()` | 超时没心跳的 Broker 踢掉 |

---

## 3. 为什么这样写（好处）

1. **NameServer 很轻**：挂了不影响已经连上 Broker 的收发（短暂影响新路由发现）
2. **水平扩展简单**：多挂几台 NameServer，Broker 全注册一遍即可
3. **路由在内存**：查询快；代价是重启 NameServer 后要等 Broker 重新注册

---

## 4. 本地操作（必做）

### 操作 A：只起 NameServer，看监听端口

1. Debug 启动 `NamesrvStartup`
2. 终端执行：

```bash
lsof -nP -iTCP:9876 -sTCP:LISTEN
```

### 操作 B：起 Broker，看注册日志

1. 在 `RouteInfoManager.registerBroker` 方法第一行打断点
2. Debug 启动 Broker（带 `-n 127.0.0.1:9876`）
3. 断点应停下；按 F8/F9 继续
4. 看日志：

```bash
grep -i "register" ~/logs/rocketmqlogs/namesrv.log | tail -20
```

你会看到类似：`new broker registered, 127.0.0.1:10911`

### 操作 C：用 mqadmin 看路由

```bash
cd /Users/wl/Desktop/code/rocketmq-all-4.9.4-source-release/distribution/target/rocketmq-4.9.4/rocketmq-4.9.4
sh bin/mqadmin clusterList -n 127.0.0.1:9876
sh bin/mqadmin topicList -n 127.0.0.1:9876
```

---

## 5. 工作流程（面试口述版）

1. 先启动 NameServer
2. Broker 启动后向 NameServer 注册自己的地址和 Topic 配置
3. Producer 启动后问 NameServer：这个 Topic 在哪些 Broker、哪些队列
4. Producer 按负载策略选一个队列，把消息直接发给 Broker
5. Consumer 同样问 NameServer 要队列列表，再去 Broker 拉消息

**重点句：客户端不经过 NameServer 转发消息，NameServer 只提供发现能力。**

---

## 6. 今日检查清单

- [ ] 能画出 Producer / NameServer / Broker / Consumer 关系
- [ ] 知道 `RouteInfoManager.registerBroker` 和 `pickupTopicRouteData` 的作用
- [ ] 本地看到过 `new broker registered`
- [ ] 能用 1 分钟讲清「工作流程」

下一篇：[day-02-Broker启动与注册.md](./day-02-Broker启动与注册.md)
