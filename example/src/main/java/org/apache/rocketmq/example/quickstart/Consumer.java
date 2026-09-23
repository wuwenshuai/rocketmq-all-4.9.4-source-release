/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 * Day5/6：本地调试入口。设 namesrvAddr 后，跟断点 pullMessage / doRebalance。
 */
public class Consumer {

    public static final String CONSUMER_GROUP = "please_rename_unique_group_name_4";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "TopicTest";

    public static void main(String[] args) throws InterruptedException, MQClientException {

        /*
         * Instantiate with specified consumer group name.
         * Day6：同组多实例会重平衡分队列；不同组互不影响各收全量。
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        // Day5：本地调试必须打开，否则不知道去哪拉路由
        consumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        /*
         * Specify where to start in case the specific consumer group is a brand-new one.
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        /*
         * Subscribe one more topic to consume.
         */
        consumer.subscribe(TOPIC, "*");

        /*
         *  Register callback to execute on arrival of messages fetched from brokers.
         * Day5：看起来像 Push 回调，实际是客户端拉到消息后线程池调用这里。
         */
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
            // Day6 练堆积时可故意 Thread.sleep(2000)，再用 mqadmin consumerProgress 看 diff
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        /*
         *  Launch the consumer instance.
         * Day5 start → 注册 + 拉服务；Day6 随后 doRebalance 分到队列才开始真正 pull。
         */
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
