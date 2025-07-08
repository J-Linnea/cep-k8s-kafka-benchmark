package de.cau.se;

import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class PatternUtils {

    public static PatternSelectFunction<Event, String> latencyReportingSelect(String label, String matchName) {
        return pattern -> {
            long now = System.currentTimeMillis();
            List<Event> matchedEvents = pattern.get(matchName);

            if (matchedEvents == null || matchedEvents.isEmpty()) {
                return label + " erkannt, aber keine Events gefunden für Match-Name: " + matchName;
            }

            Event lastEvent = matchedEvents.get(matchedEvents.size() - 1);
            long latency = now - lastEvent.getProcessingStartTime();

            String matchType = "MATCH";
            String logLine = String.format("%s,%s,%s,%d",
                    java.time.Instant.ofEpochMilli(now).toString(),
                    label,
                    matchType,
                    latency);

            writeLogLineSync(logLine);

            return label + " erkannt nach " + latency + " ms: " + lastEvent;
        };
    }

    public static PatternTimeoutFunction<Event, String> timeoutReporter(String label, String matchName) {
        return (Map<String, List<Event>> pattern, long timeoutTimestamp) -> {
            long now = System.currentTimeMillis();
            List<Event> matchedEvents = pattern.get(matchName);

            if (matchedEvents == null || matchedEvents.isEmpty()) {
                return "TIMEOUT bei " + label + ": keine Events gefunden für '" + matchName + "'";
            }

            Event firstEvent = matchedEvents.get(0);
            long latency = now - firstEvent.getProcessingStartTime();

            String matchType = "TIMEOUT";
            String logLine = String.format("%s,%s,%s,%d",
                    java.time.Instant.ofEpochMilli(now).toString(),
                    label,
                    matchType,
                    latency);

            writeLogLineSync(logLine);

            return "TIMEOUT bei " + label + " mit latency " + latency + " ms: " + firstEvent;
        };
    }

    private static final Object fileLock = new Object();

    static void writeLogLineSync(String logLine) {
        synchronized (fileLock) {
            try {
                Files.write(
                        Paths.get("metrics.csv"),
                        (logLine + "\n").getBytes(),
                        StandardOpenOption.APPEND,
                        StandardOpenOption.CREATE);
            } catch (IOException e) {
                System.err.println("Fehler beim Schreiben: " + e.getMessage());
            }
        }
    }

    public static Pattern<Event, ?> queryInit(String activity) {

        Pattern<Event, Event> events = Pattern.<Event>begin("middle")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                .subtype(Event.class);
        // System.out.println(events.getName());
        return events.where(new IterativeCondition<>() {
            @Override
            public boolean filter(Event value, Context<Event> ctx) throws Exception {
                String caseId = value.getCaseId();
                for (Event event : ctx.getEventsForPattern("middle")) {
                    if (caseId.equals(event.getCaseId())) {
                        // System.out.println("Not Matched "+event.getCaseId());
                        return false;
                    }
                }
                return true;
            }
        })
                .oneOrMore();
        // .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryExistence(String activity) {
        return Pattern.<Event>begin("start")
                .oneOrMore()
                .optional()
                .followedBy("match")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryExistence2(String activity) {
        return Pattern.<Event>begin("start")
                .oneOrMore()
                .optional()
                .next("A1")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                .followedBy("match")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryExistence3(String activity) {
        return Pattern.<Event>begin("start")
                .oneOrMore()
                .optional()
                .next("A1")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                .followedBy("A2")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                .followedBy("match")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryAbsence(String activity) {
        return Pattern.<Event>begin("start")
                .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                .oneOrMore()
                .notFollowedBy("A")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryAbsence2(String activity) {
        return Pattern.<Event>begin("start")
                .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                .oneOrMore()
                .optional()
                .next("match")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                .next("notA")
                .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                .oneOrMore()
                .notFollowedBy("A2")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryAbsence3(String activity) {
        return Pattern.<Event>begin("start")
                .where(SimpleCondition.of(e -> !e.getActivity().equals(activity)))
                .oneOrMore()
                .next("A1").where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .followedBy("match").where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .next("notA")
                .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                .oneOrMore()
                .notFollowedBy("A3").where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryExactly1(String activity) {
        return Pattern.<Event>begin("start")
                .where(SimpleCondition.of(e -> !e.getActivity().equals(activity)))
                .oneOrMore()
                .optional()
                .next("match").where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .next("notA")
                .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                .oneOrMore()
                .notFollowedBy("A2").where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryExactly2(String activity) {
        return Pattern.<Event>begin("start")
                .where(SimpleCondition.of(e -> !e.getActivity().equals(activity)))
                .oneOrMore()
                .optional()
                .next("A1").where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .followedBy("match").where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .next("notA")
                .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                .oneOrMore()
                .notFollowedBy("A3").where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryChoice(String activity, String activity2) {
        return Pattern.<Event>begin("start")
                .where(SimpleCondition.of(e -> !e.getActivity().equals(activity)))
                .where(SimpleCondition.of(e -> !e.getActivity().equals(activity2)))
                .oneOrMore().optional()
                .next("match")
                .where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .or(SimpleCondition.of(e -> e.getActivity().equals(activity2)));
    }

    public static Pattern<Event, ?> queryExclusiveChoiceA(String activity, String activity2) {
        return Pattern.<Event>begin("start")
                .where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .next("notB")
                .where(SimpleCondition.of(event -> !event.getActivity().equals(activity2)))
                .oneOrMore()
                .notFollowedBy("B").where(SimpleCondition.of(e -> e.getActivity().equals(activity2)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryExclusiveChoiceB(String activity, String activity2) {
        return Pattern.<Event>begin("start")
                .where(SimpleCondition.of(e -> e.getActivity().equals(activity2)))
                .next("notA")
                .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                .oneOrMore()
                .notFollowedBy("A").where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryRespondedExistence(String activity, String activity2) {
        return Pattern.<Event>begin("start")
                .where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .followedBy("match")
                .where(SimpleCondition.of(e -> e.getActivity().equals(activity2)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryCoExistence(String activity, String activity2) {
        return Pattern.<Event>begin("start")
                .where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .next("match")
                .where(SimpleCondition.of(e -> e.getActivity().equals(activity2)));
    }

    public static Pattern<Event, ?> queryResponse(String activity, String activity2) {
        return Pattern.<Event>begin("start").where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                .followedBy("match")
                .where(SimpleCondition.of(e -> e.getActivity().equals(activity2)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryPrecedence(String activity, String activity2) {
        return Pattern.<Event>begin("noB")
                .where(SimpleCondition.of(e -> !e.getActivity().equals(activity2)))
                .oneOrMore()
                .optional()
                .next("match")
                .where(SimpleCondition.of(e -> e.getActivity().equals(activity)));
    }

    public static Pattern<Event, ?> querySuccession(String activity, String activity2) {
        return Pattern.begin(
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(e -> !e.getActivity().equals(activity)))
                        .where(SimpleCondition.of(e -> !e.getActivity().equals(activity2)))
                        .oneOrMore()
                        .optional()
                        .next("A").where(SimpleCondition.of(e -> e.getActivity().equals(activity)))
                        .followedBy("match")
                        .where(SimpleCondition.of(e -> e.getActivity().equals(activity2))))
                .oneOrMore()
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryAlternateResponse(String activity, String activity2) {
        return Pattern.begin(
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                        .notFollowedBy("A")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                        .followedBy("match")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity2))))
                .oneOrMore()
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryAlternatePrecedence(String activity, String activity2) {
        return Pattern.begin(
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals(activity2)))
                        .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                        .oneOrMore()
                        .optional()
                        .next("A")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                        .notFollowedBy("notA")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                        .followedBy("match")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity2))))
                .oneOrMore()
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryAlternateSuccession(String activity, String activity2) {
        return Pattern.begin(
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals(activity2)))
                        .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                        .oneOrMore()
                        .optional()
                        .next("A")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                        .notFollowedBy("notA")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                        .followedBy("match")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity2))))
                .oneOrMore()
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryChainResponse(String activity, String activity2) {
        return Pattern.begin(
                Pattern.<Event>begin("notA")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                        .oneOrMore()
                        .optional()
                        .next("A")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                        .next("match")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity2))))
                .oneOrMore();
    }

    public static Pattern<Event, ?> queryChainPrecedence(String activity, String activity2) {
        return Pattern.begin(
                Pattern.<Event>begin("notB")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals(activity2)))
                        .oneOrMore()
                        .optional()
                        .next("match")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                        .next("B")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity2)))
                        .optional())
                .oneOrMore();
    }

    public static Pattern<Event, ?> queryChainSuccession(String activity, String activity2) {
        return Pattern.begin(
                Pattern.<Event>begin("notB")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                        .where(SimpleCondition.of(event -> !event.getActivity().equals(activity2)))
                        .oneOrMore()
                        .optional()
                        .next("A")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                        .next("match")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity2))))
                .oneOrMore();
    }

    public static Pattern<Event, ?> queryNotCoExistence(String activity, String activity2) {
        return Pattern.<Event>begin("start")
                .where(SimpleCondition.of(event -> !event.getActivity().equals(activity2)))
                .oneOrMore()
                .optional()
                .next("match")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                .next("notB")
                .oneOrMore()
                .where(SimpleCondition.of(event -> !event.getActivity().equals(activity2)))
                .notFollowedBy("B")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity2)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryNotSuccession(String activity, String activity2) {
        return Pattern.<Event>begin("start")
                .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                .oneOrMore()
                .optional()
                .next("match")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                .next("notB")
                .where(SimpleCondition.of(event -> !event.getActivity().equals(activity2)))
                .oneOrMore()
                .notFollowedBy("B")
                .where(SimpleCondition.of(event -> event.getActivity().equals(activity2)))
                .within(Time.seconds(10));
    }

    public static Pattern<Event, ?> queryNotChainSuccession(String activity, String activity2) {
        return Pattern.begin(
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(event -> !event.getActivity().equals(activity)))
                        .oneOrMore()
                        .optional()
                        .next("match")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity)))
                        .notNext("B")
                        .where(SimpleCondition.of(event -> event.getActivity().equals(activity2))))
                .oneOrMore();
    }
}
