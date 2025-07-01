package de.cau.se;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.flink.cep.pattern.Pattern;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class FlinkPipeline {

    public static void main(String[] args) throws Exception {
        // TODO#1 Insert your bootstrap server url
        new FlinkPipeline().start("kube1-1:31189", "group", "input", "model");
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

        DataStream<Event> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "input")
                .map(event -> {
                    event.setProcessingStartTime(System.currentTimeMillis());
                    return event;
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> {
                                    // Parse the timestamp string
                                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                                    LocalDateTime localDateTime = LocalDateTime.parse(event.getTimestamp(), formatter);
                                    return System.currentTimeMillis(); //localDateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
                                })
                ).keyBy(Event::getCaseId);

//        sourceStream.keyBy(Event::getCaseId).window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1))).process(
//                new ProcessWindowFunction<Event, Object, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, ProcessWindowFunction<Event, Object, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<Object> out) throws Exception {
//                        System.out.println((int) StreamSupport
//                                .stream(elements.spliterator(), false).count());
//                        //System.out.println(new ArrayList<>(elements)).size());
//                        for (Event element : elements) {
//                            System.out.println(element);
//                            out.collect(element);
//                        }
//                    }
//                }
//        ).print();


//        Pattern<Event, ?> alarmPattern = Pattern.<Event>begin("first")
//             .where(SimpleCondition.of(evt -> evt.getActivity().equals("Waiting for Material")))
//             .next("match")
//             .where(SimpleCondition.of(evt -> evt.getActivity().equals("Waiting for Material")));
//
//
//        DataStream<String> cepResult = CEP.pattern(sourceStream, alarmPattern)
//                .select(
//                        (PatternSelectFunction<Event, String>) pattern -> {
//                            long now = System.currentTimeMillis();
//
//                            Event lastEvent = pattern.get("match").get(0);
//                            long latency = now - lastEvent.getProcessingStartTime();
//
//                            return "Match erkannt nach " + latency + " ms: " + lastEvent.toString();
//                        }
//                );
//
 //      cepResult.print();

        OutputTag<String> timeoutTag = new OutputTag<>("timeout-events"){};

        DataStream<String> result1 = CEP.pattern(sourceStream, PatternUtils.queryInit("Store"))
                .select(PatternUtils.latencyReportingSelect("Init(A)", "middle"));

        SingleOutputStreamOperator<String> result2 = CEP.pattern(sourceStream, PatternUtils.queryExistence("Store"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("Existence(A)", "start"),      // Timeout-Verarbeitung
                        PatternUtils.latencyReportingSelect("Existence(A)", "match")  // Normale Matches
                );

        SingleOutputStreamOperator<String> result3 = CEP.pattern(sourceStream, PatternUtils.queryExistence2("MaterialPreparation - Finished"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("Existence2(A)", "start"),
                        PatternUtils.latencyReportingSelect("Existence2(A)", "match")
                );

        SingleOutputStreamOperator<String> result4 = CEP.pattern(sourceStream, PatternUtils.queryExistence3("Package waits for sending"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("Existence3(A)", "start"),
                        PatternUtils.latencyReportingSelect("Existence3(A)", "match")
                );

        SingleOutputStreamOperator<String> result5 = CEP.pattern(sourceStream, PatternUtils.queryAbsence("Reject"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("Absence(A)", "start"),
                        PatternUtils.latencyReportingSelect("Absence(A)", "start")
                );

        SingleOutputStreamOperator<String> result6 = CEP.pattern(sourceStream, PatternUtils.queryAbsence2("Item Needs Corrections"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("Absence2(A)", "start"),
                        PatternUtils.latencyReportingSelect("Absence2(A)", "notA")
                );

        SingleOutputStreamOperator<String> result7 = CEP.pattern(sourceStream, PatternUtils.queryAbsence3("Waiting for Material"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("Absence3(A)", "start"),
                        PatternUtils.latencyReportingSelect("Absence3(A)", "notA")
                );

        SingleOutputStreamOperator<String> result8 = CEP.pattern(sourceStream, PatternUtils.queryExactly1("Packaging completed"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("Exactly1(A)", "start"),
                        PatternUtils.latencyReportingSelect("Exactly1(A)", "notA")
                );

        SingleOutputStreamOperator<String> result9 = CEP.pattern(sourceStream, PatternUtils.queryExactly2("Internal Error"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("Exactly2(A)", "start"),
                        PatternUtils.latencyReportingSelect("Exactly2(A)", "notA")
                );

        DataStream<String> result10 = CEP.pattern(sourceStream, PatternUtils.queryChoice("Reject", "Pass To Production"))
                .select(PatternUtils.latencyReportingSelect("Choice(A,B)", "match"));

        SingleOutputStreamOperator<String> result11 = CEP.pattern(sourceStream, PatternUtils.queryExclusiveChoiceA("Overheating", "Item broke"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("ExclusiveChoice(A,B)", "start"),
                        PatternUtils.latencyReportingSelect("ExclusiveChoice(A,B)", "notB")
                );

        SingleOutputStreamOperator<String> result12 = CEP.pattern(sourceStream, PatternUtils.queryExclusiveChoiceB("Overheating", "Item broke"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("ExclusiveChoice(A,B)", "start"),
                        PatternUtils.latencyReportingSelect("ExclusiveChoice2(A,B)", "notA")
                );

        SingleOutputStreamOperator<String> result13 = CEP.pattern(sourceStream, PatternUtils.queryRespondedExistence("Pass to production", "Assembly Line Setup successfully"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("RespondedExistence(A,B)", "start"),
                        PatternUtils.latencyReportingSelect("RespondedExistence(A,B)", "match"));

        DataStream<String> result14 = CEP.pattern(sourceStream, PatternUtils.queryCoExistence("Quality check passed", "Packaging completed"))
                .select(PatternUtils.latencyReportingSelect("Co-Existence(A,B)", "match"));

        SingleOutputStreamOperator<String> result15 = CEP.pattern(sourceStream, PatternUtils.queryResponse("Quality check passed", "Packaging completed"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("Response(A,B)", "start"),
                        PatternUtils.latencyReportingSelect("Response(A,B)", "match")
                );

        DataStream<String> result16 = CEP.pattern(sourceStream, PatternUtils.queryPrecedence("Quality check passed", "Packaging completed"))
                .select(PatternUtils.latencyReportingSelect("Precedence(A,B)", "match"));

        SingleOutputStreamOperator<String> result17 = CEP.pattern(sourceStream, PatternUtils.querySuccession("Pass To Production", "MaterialPreparation - Finished"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("Succession(A,B)", "start"),
                        PatternUtils.latencyReportingSelect("Succession(A,B)", "match")
                );

        SingleOutputStreamOperator<String> result18 = CEP.pattern(sourceStream, PatternUtils.queryAlternateResponse("Store", "Package sent"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("AlternateResponse(A,B)", "start"),
                        PatternUtils.latencyReportingSelect("AlternateResponse(A,B)", "match")
                );

        SingleOutputStreamOperator<String> result19 = CEP.pattern(sourceStream, PatternUtils.queryAlternatePrecedence("Material Not Set Up as expected", "MaterialPreparation - Finished"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("AlternatePrecedence(A,B)", "start"),
                        PatternUtils.latencyReportingSelect("AlternatePrecedence(A,B)", "match")
                );

        SingleOutputStreamOperator<String> result20 = CEP.pattern(sourceStream, PatternUtils.queryAlternateSuccession("Assembling completed", "Item Needs Corrections"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("AlternateSuccession(A,B)", "start"),
                        PatternUtils.latencyReportingSelect("AlternateSuccession(A,B)", "match")
                );

        DataStream<String> result21 = CEP.pattern(sourceStream, PatternUtils.queryChainResponse("Waiting for Material", "MaterialPreparation - Finished"))
                .select(PatternUtils.latencyReportingSelect("ChainResponse(A,B)", "match"));

        DataStream<String> result22 = CEP.pattern(sourceStream, PatternUtils.queryChainPrecedence("Material Not Set Up as expected", "Internal error"))
                .select(PatternUtils.latencyReportingSelect("ChainPrecedence(A,B)", "match"));

        DataStream<String> result23 = CEP.pattern(sourceStream, PatternUtils.queryChainSuccession("Assembling completed", "Item Needs Corrections"))
                .select(PatternUtils.latencyReportingSelect("ChainSuccession(A,B)", "match"));

        SingleOutputStreamOperator<String> result24 = CEP.pattern(sourceStream, PatternUtils.queryNotCoExistence("Quality check passed", "Quality insufficient"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("NotCoExistence(A,B)", "start"),
                        PatternUtils.latencyReportingSelect("NotCoExistence(A,B)", "notB")
                );

        SingleOutputStreamOperator<String> result25 = CEP.pattern(sourceStream, PatternUtils.queryNotSuccession("Quality check passed", "Item Needs Corrections"))
                .select(
                        timeoutTag,
                        PatternUtils.timeoutReporter("NotSuccession(A,B)", "start"),
                        PatternUtils.latencyReportingSelect("NotSuccession(A,B)", "notB")
                );

        DataStream<String> result26 = CEP.pattern(sourceStream, PatternUtils.queryNotChainSuccession("Quality check passed", "Packaging completed"))
                .select(PatternUtils.latencyReportingSelect("NotChainSuccession(A,B)", "match"));


//         result1.print();
//         DataStream<String> timeoutResult2 = result2.getSideOutput(timeoutTag);
//         timeoutResult2.print();
//         result2.print();
//        DataStream<String> timeoutResult3 = result3.getSideOutput(timeoutTag);
//        timeoutResult3.print();
//         result3.print();
//         result4.print();
//         result5.print();
//         result6.print();
//         result7.print();
//         result8.print();
//         result9.print();
//         result10.print();
//         result11.print();
//         result12.print();
//         result13.print();
//         result14.print();
//         result15.print();
//         result16.print();
//         result17.print();
//         result18.print();
//         result19.print();
//         result20.print();
//         result21.print();
//         result22.print();
//         result23.print();
//         result24.print();
//         result25.print();
//         result26.print();

         //TODO#2 Extend the pipeline
        //env
        //        .fromSource(source, WatermarkStrategy.noWatermarks(), "input")
        //        .keyBy(Event::getCaseId)
        //        .flatMap(new DirectlyFollowsBuilder())
        //        .flatMap(new DirectlyFollowsRelationCounter())
        //        // TODO#3 Uncomment line below for testing
        //        //.map(new MockModelBuilder(100))
        //        .sinkTo(sink);
        env.execute();
    }
}