package de.cau.se;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.List;
import java.util.Properties;

public class FlinkPipeline {

    public static void main(String[] args) throws Exception {
        // TODO#1 Insert your bootstrap server url
        new FlinkPipeline().start("minikube:31343", "group", "input", "model");
    }

    public void start(final String bootstrapServer, final String group, final String inTopic, final String outTopic) throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers(bootstrapServer)
                .setTopics(inTopic)
                .setGroupId(group)
                .setProperty("enable.auto.commit", "true")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Event.class))
                .build();

        KafkaSink<List<CountedDirectlyFollowsRelations>> sink = KafkaSink.<List<CountedDirectlyFollowsRelations>>builder()
                .setBootstrapServers(bootstrapServer)
                .setRecordSerializer(KafkaRecordSerializationSchema.<List<CountedDirectlyFollowsRelations>>builder()
                        .setTopic(outTopic)
                        .setValueSerializationSchema(new JsonSerializationSchema<>())
                        .build()
                ).build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO#2 Extend the pipeline
        env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "input")
                .keyBy(Event::getCaseId)
                .flatMap(new DirectlyFollowsBuilder())
                .flatMap(new DirectlyFollowsRelationCounter())
                // TODO#3 Uncomment line below for testing
                //.map(new MockModelBuilder(100))
                .sinkTo(sink);
        env.execute();
    }
}