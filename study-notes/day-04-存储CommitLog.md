# Day4：存储 CommitLog（3～4h）

## 今日目标

搞清消息落在磁盘哪里、怎么写得快、刷盘是什么意思。

## 关联面试题

- 3. 如何保证消息不丢失？（Broker 存储侧）
- 14. 丢消息可能原因（刷盘/复制）

---

## 1. 核心思想：所有 Topic 混写一个 CommitLog

很多中间件按 Topic 分文件。RocketMQ 反过来：

```text
TopicA 的消息 \
TopicB 的消息  }  ──顺序追加──▶  CommitLog（大文件顺序写）
TopicC 的消息 /
```

然后另外建 **ConsumeQueue** 当索引（Day5）。

### 为什么这样快？

磁盘顺序写比随机写快很多。所有消息追加到同一个文件末尾，像记流水账。

### 代价

消费时不能直接扫 CommitLog，所以需要 ConsumeQueue：只存「在 CommitLog 的哪个位置」。

---

## 2. 关键类（今天盯这些）

| 类 | 路径 | 作用 |
|----|------|------|
| `DefaultMessageStore` | `store/.../DefaultMessageStore.java` | 存储门面 |
| `CommitLog` | `store/.../CommitLog.java` | 消息顺序写 |
| `MappedFile` | `store/.../MappedFile.java` | 一个物理文件的 mmap 封装 |
| `MappedFileQueue` | `store/.../MappedFileQueue.java` | 多个 MappedFile 组成队列 |

### 关键写入方法

`CommitLog#asyncPutMessage(MessageExtBrokerInner msg)`  
（同步版还有 `putMessage`，看你走的路径）

里面典型步骤：

1. 编码消息
2. `putMessageLock.lock()` 保证写入串行（或自旋锁）
3. 拿到当前 `MappedFile`
4. `mappedFile.appendMessage(...)`
5. 解锁
6. 根据刷盘配置提交 flush 请求

文件大小默认约 **1GB** 一个 MappedFile，写满滚动下一个。

本地可看：

```bash
ls -lh ~/store/commitlog/
```

---

## 3. 刷盘：ASYNC vs SYNC

| 模式 | 含义 | 性能 | 安全性 |
|------|------|------|--------|
| ASYNC_FLUSH | 先写 PageCache，后台刷盘 | 快 | 机器掉电可能丢最后一点 |
| SYNC_FLUSH | 等刷到磁盘再返回 | 慢 | 更稳 |

配置在 Broker：`flushDiskType=ASYNC_FLUSH / SYNC_FLUSH`

### 设计模式

- **内存映射（mmap）**：`MappedFile` 把文件映射到内存地址，减少用户态拷贝
- **串行化写入 + 并发读**：写加锁，读靠索引并行

---

## 4. 本地操作

### 操作 A：发消息前后对比文件

```bash
# 启动前/后都可以看
ls -lh ~/store/commitlog/
du -sh ~/store/commitlog/*
```

1. 记录当前 commitlog 文件大小
2. Debug/Run Producer 发 1000 条
3. 再看文件大小是否增长

### 操作 B：断点看写入

断点：

1. `SendMessageProcessor` 调用 `putMessage/asyncPutMessage` 附近
2. `CommitLog.asyncPutMessage` 入口
3. `MappedFile.appendMessage`（如果跟得动）

观察：

- `msg.getTopic()`
- 写入后的 `AppendMessageResult` / `PutMessageResult` 状态

### 操作 C：用工具查消息（验证真的存了）

先从 Producer 控制台抄一个 `offsetMsgId` 或 `msgId`，然后：

```bash
cd /Users/wl/Desktop/code/rocketmq-all-4.9.4-source-release/distribution/target/rocketmq-4.9.4/rocketmq-4.9.4
sh bin/mqadmin queryMsgById -n 127.0.0.1:9876 -i <你的msgId>
```

能查到 body，说明存储链路通了。

---

## 5. 面试怎么答（不丢失-存储侧）

「RocketMQ 把消息顺序写入 CommitLog，利用 PageCache 和顺序写提高吞吐。重要场景用同步刷盘，甚至同步复制到 Slave。异步刷盘时，Broker 在刷盘前宕机会丢尚未落盘的数据。所以‘不丢失’是端到端一起做：生产者同步发送、Broker 同步刷盘/复制、消费者确认消费位点。」

---

## 今日检查清单

- [ ] 能解释「混写 CommitLog + ConsumeQueue 索引」
- [ ] 找到 `CommitLog.asyncPutMessage`
- [ ] 看过 `~/store/commitlog` 文件变大
- [ ] 能说出 SYNC_FLUSH / ASYNC_FLUSH 差别

下一篇：[day-05-ConsumeQueue与Push本质是拉.md](./day-05-ConsumeQueue与Push本质是拉.md)
