package org.example;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class));

        String bootstrapServer = "10.128.0.121:9092";
        String topic = "retention-1-min-3-part";

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "console-1");

        List<TopicPartition> topicPartitions = new ArrayList<>();
        int partitionCount = 3;
        for (int i = 0; i < partitionCount; i++) {
            topicPartitions.add(new TopicPartition(topic, i));
        }

        pipeline
                .apply("Read from Kafka",
                        KafkaIO.<String, String>read()
                                .withBootstrapServers(bootstrapServer)
                                .withTopicPartitions(topicPartitions)
                                .withKeyDeserializerAndCoder(StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                                .withValueDeserializerAndCoder(StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                                .withConsumerConfigUpdates(ImmutableMap.copyOf(consumerConfig))
//                                .withReadCommitted()
//                                .commitOffsetsInFinalize()
                )
                .apply("Log Records",
                        ParDo.of(new DoFn<KafkaRecord<String, String>, Void>() {
                            @ProcessElement
                            public void processElement(@Element KafkaRecord<String, String> e) {
                                LOG.info(
                                        "Received element: topic = {}, partition = {}, offset = {}, timestamp = {}, key = {}, value = {}",
                                        e.getTopic(), e.getPartition(), e.getOffset(), e.getTimestamp(), e.getKV().getKey(),
                                        e.getKV().getValue());
                            }
                        }));

        pipeline.run();
    }
}