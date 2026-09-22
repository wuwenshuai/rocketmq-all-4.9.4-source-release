# Day5：ConsumeQueue + Push 本质是拉（3～4h）

## 今日目标

- 搞清 ConsumeQueue 是什么
- 搞清 RocketMQ 的 PushConsumer 其实是客户端在拉
- 跟一次拉消息请求

## 关联面试题

- 9. RocketMQ 的消息是推还是拉？
- 8. 消息分发（消费侧如何拿到消息）

---

## 1. ConsumeQueue：货架标签

CommitLog 是一本厚流水账。消费时如果每次都扫整本账，太慢。

于是有了 ConsumeQueue：

```text
ConsumeQueue 每条索引大约固定长度，内容类似：
[ CommitLog物理偏移 | 消息大小 | Tag哈希 ]
```

目录结构本地可见：

```bash
ls ~/store/consumequeue/
# 下面按 topic / queueId 分层
find ~/store/consumequeue -type f | head
```

### 谁负责从 CommitLog 建索引？

`DefaultMessageStore` 内部类：

`ReputMessageService`

它在后台顺着 CommitLog 往前「转发/分发」，生成：

- ConsumeQueue
- IndexFile（按 key/时间查询用，消费主路径不靠它）

可以简单记：

**写消息时先落 CommitLog，再异步（很快）建 ConsumeQueue。**

极端情况下，消息已写入但索引还没建好，会短暂对消费不可见——一般很快追上。

---

## 2. Push 其实是拉

面试标准答法：

> RocketMQ 对外提供 PushConsumer API，但实现上是客户端开线程主动向 Broker Pull，Broker 可以长轮询挂起请求，有消息再返回。所以是「伪推真拉」。

```text
Consumer 线程循环：
  发 Pull 请求 ──▶ Broker
                   │ 没消息：先挂起一会儿（长轮询）
                   │ 有消息：从 ConsumeQueue 找到偏移，读 CommitLog，返回
  ◀── 消息列表
  提交到消费线程池执行业务
  再继续 Pull
```

好处：

- 消费速度由消费者自己控制（反压自然）
- Broker 不被慢消费者拖死推送风暴

---

## 3. 源码入口

### 客户端 Push

`org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl`

关键方法：

| 方法 | 作用 |
|------|------|
| `start()` | 启动拉取与消费服务 |
| `pullMessage(PullRequest)` | 真正向 Broker 拉 |
| `doRebalance()` | 触发队列重新分配（Day6） |

拉取服务线程：

`org.apache.rocketmq.client.impl.consumer.PullMessageService`

并发消费：

`ConsumeMessageConcurrentlyService`

### Broker 拉消息处理

`org.apache.rocketmq.broker.processor.PullMessageProcessor#processRequest`

内部会调用 MessageStore 按队列 offset 取消息。

### 索引相关

`org.apache.rocketmq.store.ConsumeQueue`  
`DefaultMessageStore.ReputMessageService`

---

## 4. 本地操作

### 操作 A：先看磁盘索引

1. 确保之前发过消息到 `TopicTest`
2. 执行：

```bash
find ~/store/consumequeue -type f | head -20
ls -l ~/store/consumequeue/TopicTest 2>/dev/null || ls ~/store/consumequeue | head
```

### 操作 B：断点看拉消息

1. Consumer 设置 `namesrvAddr=127.0.0.1:9876`
2. 断点：
   - `DefaultMQPushConsumerImpl.pullMessage`
   - `PullMessageProcessor.processRequest`
3. 先启 Consumer（Debug），再启 Producer 发几条
4. 观察请求里的 `queueOffset`、返回的消息条数

### 操作 C：体会长轮询

Consumer 挂着但先不发消息：你的 `pullMessage` 可能隔一段时间才返回空/超时再拉。  
然后突然发消息，很快就有回调消费日志。

---

## 5. 面试怎么答（推还是拉）

「API 上看是 Push，实现是客户端 Pull + Broker 长轮询。这样消费端能反压，Broker 实现也更简单。和真正服务端推送相比，RocketMQ 更偏向拉模型。」

---

## 今日检查清单

- [ ] 能解释 CommitLog 与 ConsumeQueue 分工
- [ ] 知道 `ReputMessageService` 负责建索引
- [ ] 断点看过 `pullMessage` / `PullMessageProcessor`
- [ ] 能清楚回答「伪推真拉」

下一篇：[day-06-位点重平衡与堆积.md](./day-06-位点重平衡与堆积.md)
