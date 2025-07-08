package de.cau.se;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import static de.cau.se.PatternUtils.writeLogLineSync;

public class ThroughputCounter extends RichMapFunction<Event, Event> {
    private transient long count;
    private transient long lastTimestamp;

    @Override
    public void open(Configuration parameters) throws Exception {
        count = 0;
        lastTimestamp = System.currentTimeMillis();
    }

    @Override
    public Event map(Event event) throws Exception {
        count++;
        long now = System.currentTimeMillis();
        if (now - lastTimestamp >= 1000) {
            String logLine = String.format("%s,THROUGHPUT,,%d",
                    java.time.Instant.ofEpochMilli(now).toString(),
                    count // throughput
            );
            writeLogLineSync(logLine);
            count = 0;
            lastTimestamp = now;
        }
        return event;
    }
}
