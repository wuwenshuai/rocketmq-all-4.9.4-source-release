# Day1：总览 + NameServer 路由（3～4h）

## 今日目标

- 能画出 RocketMQ 四大角色关系
- 知道 NameServer **不存消息**，只存路由
- 能跟着断点看懂 Broker 注册、客户端查路由主流程

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

## 2. NameServer 启动入口

`org.apache.rocketmq.namesrv.NamesrvStartup`

关键方法：

1. `main` → `main0`
2. `createNamesrvController`：读配置、创建控制器
3. `start(NamesrvController)`：真正启动网络服务

然后看：

`org.apache.rocketmq.namesrv.NamesrvController`

- `initialize()`：初始化路由管理、KV 配置、Netty 服务端、注册请求处理器
- `start()`：启动 Netty，开始监听 `9876`
- `registerProcessor()`：把 `DefaultRequestProcessor` 挂到 Netty 上

---

## 3. RouteInfoManager：电话簿怎么工作（今天核心）

打开：

`namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/RouteInfoManager.java`

不要求每行都懂。你只要搞清两件事：

1. Broker 注册时，NameServer 怎么改表
2. 客户端查路由时，NameServer 怎么拼返回结果

### 3.1 先用本地例子建立直觉

你本地常见值：

- 集群名：`DefaultCluster`
- Broker 名：`broker-a`
- Broker 地址：`127.0.0.1:10911`
- BrokerId：`0`（0 = Master）

NameServer **不存消息**，只回答：

1. 这个 Broker 还活着吗？地址是啥？
2. 这个 Topic 在哪些 Broker、有几个队列？

### 3.2 四张表存什么（约第 57～61 行）

```java
private final HashMap<String/* topic */, Map<String /* brokerName */ , QueueData>> topicQueueTable;
private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;
private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;
private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
```

| 表 | 白话 | 本地注册成功后大概长这样 |
|----|------|--------------------------|
| `clusterAddrTable` | 这个小区有哪些快递柜品牌 | `DefaultCluster -> {broker-a}` |
| `brokerAddrTable` | 某个品牌的 Master/Slave 地址 | `broker-a -> {0: 127.0.0.1:10911}` |
| `topicQueueTable` | 某个货架（Topic）在哪些柜子、几个格子 | `TopicTest -> {broker-a: 读8写8}` |
| `brokerLiveTable` | 这个地址上次心跳时间 | `127.0.0.1:10911 -> lastUpdate=现在` |

先可忽略：`filterServerTable`（过滤服务器，本地单机基本用不到）。

为什么拆成 4 张：

- 问「集群里有哪些 Broker」→ `clusterAddrTable`
- 问「TopicTest 怎么发」→ 先 `topicQueueTable`，再 `brokerAddrTable` 找 IP
- 问「Broker 挂了没」→ `brokerLiveTable`

### 3.3 请求怎么进到 RouteInfoManager

```text
Broker / Client 发请求
   │
   ▼
Netty 收到包
   │
   ▼
DefaultRequestProcessor.processRequest(...)
   │  根据 requestCode 分发
   ├─ REGISTER_BROKER        → registerBroker(...)
   └─ GET_ROUTEINFO_BY_TOPIC → getRouteInfoByTopic(...)
         │
         ▼
RouteInfoManager.registerBroker(...) 或 pickupTopicRouteData(...)
```

对应代码：

1. `NamesrvController#registerProcessor`：挂上 `DefaultRequestProcessor`
2. `DefaultRequestProcessor#processRequest`：按 `RequestCode` 分发
3. `RouteInfoManager#registerBroker` / `#pickupTopicRouteData`：改表 / 查表

---

## 4. 主流程 A：Broker 注册 `registerBroker`

方法：`RouteInfoManager#registerBroker(...)`（约 138 行）

参数先认这些：

| 参数 | 含义 | 本地常见值 |
|------|------|------------|
| `clusterName` | 集群名 | `DefaultCluster` |
| `brokerAddr` | Broker IP:端口 | `127.0.0.1:10911` |
| `brokerName` | Broker 逻辑名 | `broker-a` |
| `brokerId` | 0=Master，非0=Slave | `0` |
| `topicConfigWrapper` | Broker 带来的 Topic 配置 | 一堆 TopicConfig |
| `channel` | 网络连接 | Netty Channel |

### 第 0 步：加写锁

```java
this.lock.writeLock().lockInterruptibly();
```

注册会改多张表，写锁保证同一时刻只有一个注册在改。

### 第 1 步：登记集群成员

```java
Set<String> brokerNames = this.clusterAddrTable.computeIfAbsent(clusterName, k -> new HashSet<>());
brokerNames.add(brokerName);
```

效果：

```text
clusterAddrTable:
  DefaultCluster -> [broker-a]
```

### 第 2 步：登记地址本

```java
BrokerData brokerData = this.brokerAddrTable.get(brokerName);
if (null == brokerData) {
    registerFirst = true;
    brokerData = new BrokerData(...);
    this.brokerAddrTable.put(brokerName, brokerData);
}
String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
```

效果：

```text
brokerAddrTable:
  broker-a -> BrokerData {
      brokerAddrs: {
          0 -> 127.0.0.1:10911   // Master
      }
  }
```

白话：

- `brokerName` = 一家店的名字
- `brokerId + addr` = 主店 / 分店地址

中间有一段删「同 IP 不同 brokerId」的逻辑，是防主从切换脏数据。本地单 Master 扫一眼即可。

### 第 3 步：Master 才更新 Topic 队列

```java
if (null != topicConfigWrapper && MixAll.MASTER_ID == brokerId) {
    if (this.isBrokerTopicConfigChanged(...) || registerFirst) {
        for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
            this.createAndUpdateQueueData(brokerName, entry.getValue());
        }
    }
}
```

重点：

1. 只有 Master（`brokerId==0`）更新 Topic 路由
2. 用 `DataVersion` 判断配置有没有变；没变就跳过
3. 真正写入靠 `createAndUpdateQueueData(brokerName, topicConfig)`

`createAndUpdateQueueData` 做的事：

```text
TopicConfig → QueueData（写队列数/读队列数/权限）
放入 topicQueueTable[topicName][brokerName] = queueData
```

效果：

```text
topicQueueTable:
  TopicTest -> {
     broker-a -> QueueData{writeQueueNums=8, readQueueNums=8, ...}
  }
```

日志 `new topic registered, TopicTest ...` 就是这里打的。

### 第 4 步：盖心跳时间

```java
this.brokerLiveTable.put(brokerAddr,
    new BrokerLiveInfo(System.currentTimeMillis(), ...));
```

后面 `scanNotActiveBroker()` 会扫这张表：超过约 **120 秒**（`BROKER_CHANNEL_EXPIRED_TIME`）没打卡，就摘掉 Broker。

### 第 5 步：如果是 Slave，返回 Master 地址

本地单 Master 通常走不到。知道「Slave 注册时，NameServer 会告诉它 Master 在哪」即可。

### 注册流程小结

```text
加写锁
  ├─ clusterAddrTable 记下 cluster → brokerName
  ├─ brokerAddrTable  记下 brokerName → (brokerId → addr)
  ├─ 若是 Master 且 Topic 有变化：
  │     topicQueueTable 记下 topic → (brokerName → QueueData)
  └─ brokerLiveTable  盖心跳时间戳
解锁，返回结果
```

一句话：

> 注册 = 更新「集群成员 + 地址本 + Topic 队列说明书 + 存活打卡」。

---

## 5. 主流程 B：客户端查路由 `pickupTopicRouteData`

方法：`RouteInfoManager#pickupTopicRouteData(String topic)`（约 409 行）

入口：`DefaultRequestProcessor#getRouteInfoByTopic`

### 按顺序干什么

1. 加读锁，从 `topicQueueTable` 取出该 Topic 的队列信息  
2. 用这些 `brokerName` 去 `brokerAddrTable` 找 IP  
3. 两样都找到才返回 `TopicRouteData`，否则返回 `null`

返回 `null` 时客户端常见报错：

```text
No route info of this topic
```

常见原因：Broker 还没注册成功 / Topic 不存在 / 连错 NameServer。

### 客户端拿到后怎么用

```text
TopicRouteData
  ├─ queueDatas:  [ QueueData(broker-a, 8个写队列) ]
  └─ brokerDatas: [ BrokerData(broker-a, 0->127.0.0.1:10911) ]

Producer 选一个 queueId（比如 3）
然后直接连 127.0.0.1:10911 发消息
（不经过 NameServer 转发消息）
```

---

## 6. 主流程 C：过期清理（知道即可）

`scanNotActiveBroker()`：

```text
遍历 brokerLiveTable
如果 now - lastUpdate > 120秒
  → 关闭连接
  → 从路由表摘掉这个 Broker
```

面试常说：

- Broker 大约每 30 秒注册/心跳一次
- NameServer 大约 120 秒没心跳就摘除

---

## 7. 为什么这样写（好处）

1. **NameServer 很轻**：挂了不影响已经连上 Broker 的收发（短暂影响新路由发现）
2. **水平扩展简单**：多挂几台 NameServer，Broker 全注册一遍即可
3. **路由在内存**：查询快；代价是重启后要等 Broker 重新注册

---

## 8. 本地操作（必做）

### 操作 A：只起 NameServer，看端口

1. Debug 启动 `NamesrvStartup`
2. 终端：

```bash
lsof -nP -iTCP:9876 -sTCP:LISTEN
```

### 操作 B：注册断点三连

> 源码里已加 `Day1` 中文注释，可直接搜 `Day1` 跳转。

1. Debug 启 NameServer
2. 断点：
   - `DefaultRequestProcessor.processRequest`
   - `DefaultRequestProcessor.registerBrokerWithFilterServer`（4.9.4 通常走这）或 `registerBroker`
   - `RouteInfoManager.registerBroker`
3. Debug 启 Broker（`-n 127.0.0.1:9876`）
4. 停在 `processRequest` 时，看 `request.getCode()` 是否 `REGISTER_BROKER`
5. 进 `registerBroker` 后，Variables 里看：
   - `clusterName` / `brokerName` / `brokerAddr` / `brokerId`
6. 单步进 `createAndUpdateQueueData`，看 `topicConfig.getTopicName()`
7. 日志确认：

```bash
grep -i "register" ~/logs/rocketmqlogs/namesrv.log | tail -20
```

应看到：`new broker registered, 127.0.0.1:10911`

### 操作 C：查路由断点

1. 断点：
   - `DefaultRequestProcessor.getRouteInfoByTopic`
   - `RouteInfoManager.pickupTopicRouteData`
2. 跑 `quickstart.Producer`（`namesrvAddr=127.0.0.1:9876`）
3. 看 `topic` 是否 `TopicTest`
4. 看返回的 `brokerDatas` / `queueDatas`

### 操作 D：命令对照

```bash
cd /Users/wl/Desktop/code/rocketmq-all-4.9.4-source-release/distribution/target/rocketmq-4.9.4/rocketmq-4.9.4

sh bin/mqadmin clusterList -n 127.0.0.1:9876
sh bin/mqadmin topicList -n 127.0.0.1:9876
sh bin/mqadmin topicRoute -n 127.0.0.1:9876 -t TopicTest
```

把命令输出和断点里的对象对上，就通了。

---

## 9. 读源码时怎么跳着看

不要通读 `RouteInfoManager` 800 行。顺序：

1. 字段定义（四张表）
2. `registerBroker`
3. `createAndUpdateQueueData`
4. `pickupTopicRouteData`
5. `scanNotActiveBroker`

其它删 Topic、擦写权限等方法，面试前知道存在即可。

---

## 10. 工作流程 / 面试口述

### 工作流程

1. 先启动 NameServer  
2. Broker 启动后向 NameServer 注册地址和 Topic 配置  
3. Producer 问 NameServer：Topic 在哪些 Broker、哪些队列  
4. Producer 选一个队列，直接发给 Broker  
5. Consumer 同样问路由，再去 Broker 拉消息  

**重点句：客户端不经过 NameServer 转发消息，NameServer 只提供发现能力。**

### RouteInfoManager 口述版

「NameServer 核心是 RouteInfoManager，内存里维护集群、Broker 地址、Topic 队列、心跳四张表。Broker 发 REGISTER_BROKER，DefaultRequestProcessor 转到 registerBroker：写入集群成员、地址、Master 的 Topic 队列，并更新心跳。客户端发 GET_ROUTEINFO_BY_TOPIC，pickupTopicRouteData 先按 Topic 找队列，再补 Broker 地址，返回 TopicRouteData。之后客户端直连 Broker，NameServer 不转发消息。」

---

## 11. 今日检查清单

- [ ] 能画出 Producer / NameServer / Broker / Consumer 关系
- [ ] 能说出四张表各自干什么
- [ ] 断点跟过 `registerBroker` 主步骤
- [ ] 断点或命令看过 `topicRoute`
- [ ] 能用 1～2 分钟讲清工作流程 + 路由注册

下一篇：[day-02-Broker启动与注册.md](./day-02-Broker启动与注册.md)
