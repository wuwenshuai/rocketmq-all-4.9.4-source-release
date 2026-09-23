# Day9：延时消息（3～4h）

## 今日目标

搞清 4.x 延时消息为什么是「固定等级」，源码怎么投递。

## 关联面试题

- 4. RocketMQ 如何实现延时消息？

---

## 1. 用人话讲

你希望：消息现在发，**过一会儿**才让消费者看见。

4.x 实现思路：

1. 发送时带上 delay level（延时等级）
2. Broker **先不放到真实 Topic 队列给消费者**
3. 放到内部调度 Topic（`SCHEDULE_TOPIC_XXXX`）对应等级的队列
4. 定时任务到期后，把消息 **重新写入真实 Topic**
5. 消费者这时才能拉到

注意：4.x 通常是 **18 个固定等级**（1s、5s、10s…2h 等），不是任意时间。  
任意时间延时更多是 5.x 能力或其它方案。

---

## 2. 源码入口

核心类：

`org.apache.rocketmq.store.schedule.ScheduleMessageService`

关键方法：

| 方法 | 作用 |
|------|------|
| `parseDelayLevel()` | 解析延时等级对应的时间 |
| `start()` | 启动各等级定时投递任务 |
| 内部类 `DeliverDelayedMessageTimerTask` | 到期扫描并投递 |
| `messageTimeup(...)` | 到期后恢复成正常消息再写入 |

示例：

- `example/.../schedule/ScheduledMessageProducer.java`
- `example/.../schedule/ScheduledMessageConsumer.java`

---

## 3. 设计好处与限制

好处：

- 实现简单，和主存储共用 CommitLog
- 定时任务按 level 分队列，互不影响

限制：

- 不是任意延迟（4.x）
- 到期瞬间有大量消息时，可能造成投递尖刺
- 精度受定时任务周期影响

---

## 4. 本地操作

> 源码里已加 `Day9` 中文注释，IDEA 全局搜 `Day9` 可跳转。

### 操作 A：跑延时示例

1. 打开 `ScheduledMessageProducer`（已设 namesrv，`setDelayTimeLevel(3)` ≈ 10s）
2. 先启 `ScheduledMessageConsumer`
3. 再启 Producer，看表计时：消息是否明显晚到（打印 `ms later`）

### 操作 B：看内部调度 Topic

```bash
cd /Users/wl/Desktop/code/rocketmq-all-4.9.4-source-release/distribution/target/rocketmq-4.9.4/rocketmq-4.9.4
sh bin/mqadmin topicList -n 127.0.0.1:9876 | grep -i schedule
```

也可能在 `~/store/consumequeue/` 下看到 schedule 相关目录。

### 操作 C：断点

1. `CommitLog.asyncPutMessage` 里 delayLevel 改写 SCHEDULE_TOPIC
2. `ScheduleMessageService.start` / `parseDelayLevel`
3. `DeliverDelayedMessageTimerTask.executeOnTimeup`
4. `messageTimeup`

发一条 delay level 较小的消息（例如 1～3 级），更容易等到断点。

---

## 5. 面试口述

「RocketMQ 4.x 延时消息用固定 delay level。消息先写到内部调度 Topic，由 ScheduleMessageService 到期后再投递到真实 Topic。实现简单，但只支持预设等级；要任意时间延时需要 5.x 或其它方案。」

---

## 今日检查清单

- [ ] 知道固定等级，不是任意秒数
- [ ] 跑过 schedule 示例并肉眼看出延迟
- [ ] 找到 `ScheduleMessageService`
- [ ] 能说出「先调度 Topic，后真实 Topic」

下一篇：[day-10-集群与高可用.md](./day-10-集群与高可用.md)
