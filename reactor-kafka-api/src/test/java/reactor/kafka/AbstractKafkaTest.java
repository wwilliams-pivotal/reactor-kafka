/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package reactor.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

public class AbstractKafkaTest {

    protected final String topic = "testtopic";
    protected final int partitions = 4;
    protected long receiveTimeoutMillis = 20000;
    protected final long requestTimeoutMillis = 1000;
    protected final long sessionTimeoutMillis = 6000;
    protected final long heartbeatIntervalMillis = 2000;

    @Rule
    public KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, partitions, topic);
    @Rule
    public TestName testName = new TestName();

    protected FluxConfig<Integer, String> fluxConfig;
    protected SenderConfig<Integer, String> senderConfig;

    protected final List<List<Integer>> expectedMessages = new ArrayList<List<Integer>>(partitions);
    protected final List<List<Integer>> receivedMessages = new ArrayList<List<Integer>>(partitions);

    public void setUp() throws Exception {
        System.out.println("********** RUNNING " + getClass().getName() + "." + testName.getMethodName());
        for (int i = 0; i < partitions; i++)
            expectedMessages.add(new ArrayList<>());
        for (int i = 0; i < partitions; i++)
            receivedMessages.add(new ArrayList<>());

        senderConfig = createKafkaProducerConfig();
        fluxConfig = createKafkaConsumerConfig();
        embeddedKafka.waitUntilSynced(topic, 0);
    }

    public FluxConfig<Integer, String> createKafkaConsumerConfig() {
        fluxConfig = createKafkaFluxConfig(null, testName.getMethodName());
        return fluxConfig;
    }

    public SenderConfig<Integer, String> createKafkaProducerConfig() {
        Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMillis));
        senderConfig = new SenderConfig<>(props);
        return senderConfig;
    }

    public FluxConfig<Integer, String> createKafkaFluxConfig(Map<String, Object> propsOverride, String groupId) {
        Map<String, Object> props = KafkaTestUtils.consumerProps("", "false", embeddedKafka);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMillis));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(heartbeatIntervalMillis));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2");
        if (propsOverride != null)
            props.putAll(propsOverride);
        fluxConfig = new FluxConfig<>(props);
        fluxConfig.commitInterval(Duration.ofMillis(50));
        fluxConfig.maxAutoCommitAttempts(1);
        return fluxConfig;
    }

    public ProducerRecord<Integer, String> createProducerRecord(int index, boolean expectSuccess) {
        int partition = index % partitions;
        if (expectSuccess) expectedMessages.get(partition).add(index);
        return new ProducerRecord<Integer, String>(topic, partition, index, "Message " + index);
    }

    public void onReceive(ConsumerRecord<Integer, String> record) {
        receivedMessages.get(record.partition()).add(record.key());
    }

    public void checkConsumedMessages() {
        assertEquals(expectedMessages, receivedMessages);
    }
    public void checkConsumedMessages(int receiveStartIndex, int receiveCount) {
        for (int i = 0; i < partitions; i++)
            checkConsumedMessages(i, receiveStartIndex, receiveStartIndex + receiveCount);
    }

    public void checkConsumedMessages(int partition, int receiveStartIndex, int receiveEndIndex) {
        // Remove the messages still in the send list which should not be consumed
        List<Integer> expected = new ArrayList<Integer>(expectedMessages.get(partition));
        Iterator<Integer> it = expected.iterator();
        while (it.hasNext()) {
            int index = it.next();
            if (index < receiveStartIndex || index > receiveEndIndex)
                it.remove();
        }
        assertEquals(expected, receivedMessages.get(partition));
    }

    public Collection<TopicPartition> getTopicPartitions() {
        Collection<TopicPartition> topicParts = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            topicParts.add(new TopicPartition(topic, i));
        }
        return topicParts;
    }

    public void deleteTopic(String topic) {
        ZkUtils zkUtils = new ZkUtils(embeddedKafka.getZkClient(), null, false);
        AdminUtils.deleteTopic(zkUtils, topic);
    }

    public void clearReceivedMessages() {
        receivedMessages.forEach(l -> l.clear());
    }

    protected int count(List<List<Integer>> list) {
        return list.stream().mapToInt(l -> l.size()).sum();
    }
}
