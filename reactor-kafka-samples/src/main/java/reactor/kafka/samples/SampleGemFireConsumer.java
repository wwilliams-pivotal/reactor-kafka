/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package reactor.kafka.samples;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.gemfire.GemfireTemplate;

import com.gemstone.gemfire.internal.util.StopWatch;

import reactor.core.Cancellation;
import reactor.kafka.ConsumerOffset;
import reactor.kafka.FluxConfig;
import reactor.kafka.KafkaFlux;

/**
 * Sample consumer application using Reactive API for Java.
 * To run sample consumer
 * <ol>
 *   <li> Start Zookeeper and Kafka server
 *   <li> Create Kafka topic {@link #TOPIC}
 *   <li> Update {@link #BOOTSTRAP_SERVERS} and {@link #TOPIC} if required
 *   <li> Run {@link SampleGemFireConsumer} as Java application will all dependent jars in the CLASSPATH (eg. from IDE).
 *   <li> Shutdown Kafka server and Zookeeper when no longer required
 * </ol>
 */
public class SampleGemFireConsumer {

    private static final Logger log = LoggerFactory.getLogger(SampleGemFireConsumer.class.getName());

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "demo-topic";

    private final FluxConfig<Integer, String> fluxConfig;
    private final SimpleDateFormat dateFormat;

    ApplicationContext context = new ClassPathXmlApplicationContext("cache-config.xml");
    GemfireTemplate region = (GemfireTemplate) context.getBean("regionATemplate");
    private static int recordCount = 0;
    private static StopWatch sw = new StopWatch();

    
    public SampleGemFireConsumer(String bootstrapServers) {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        fluxConfig = new FluxConfig<>(props);

        dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");
    }

    public Cancellation consumeMessages(String topic, CountDownLatch latch) {

        KafkaFlux<Integer, String> kafkaFlux =
                KafkaFlux.listenOn(fluxConfig, Collections.singleton(topic))
                         .doOnPartitionsAssigned(partitions -> log.debug("onPartitionsAssigned {}", partitions))
                         .doOnPartitionsRevoked(partitions -> log.debug("onPartitionsRevoked {}", partitions));
 
 
        sw.start();
        return kafkaFlux.subscribe(message -> {
                ConsumerOffset offset = message.consumerOffset();
                ConsumerRecord<Integer, String> record = message.consumerRecord();
                
				/*
				 * Write to GemFire
				 */
                recordCount++;
                Integer recordKey = record.key() % 100;
                region.put(recordKey.toString(), record.value());
                
                if (record.key() % 10000 == 0) {
	                System.out.printf("Received message: topic-partition=%s offset=%d timestamp=%s key=%d value=%s rate=%s\n",
	                        offset.topicPartition(),
	                        offset.offset(),
	                        dateFormat.format(new Date(record.timestamp())),
	                        record.key(),
	                        record.value(),
	                        recordCount / (sw.elapsedTimeMillis() / 1000));
//			                recordCount,
//			                sw.elapsedTimeMillis());
	                }
                latch.countDown();
            });
    }

    public static void main(String[] args) throws Exception {
        int count = 200000;
        CountDownLatch latch = new CountDownLatch(count);
        SampleGemFireConsumer consumer = new SampleGemFireConsumer(BOOTSTRAP_SERVERS);
        
        StopWatch sw = new StopWatch(true);
        
        Cancellation cancellation = consumer.consumeMessages(TOPIC, latch);
        System.out.println("Hit Latch"); 
        latch.await(120, TimeUnit.SECONDS);
        System.out.println("Count=" + recordCount + "; ms=" + sw.elapsedTimeMillis() + "; avg=" +  (double)recordCount / sw.elapsedTimeMillis() * 1000 + "/sec"); 
        sw.stop();
        cancellation.dispose();
    }

}
