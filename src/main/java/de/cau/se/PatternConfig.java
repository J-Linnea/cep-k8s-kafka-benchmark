package de.cau.se;

import org.apache.flink.cep.pattern.Pattern;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

class PatternConfig {
    String name;
    Pattern<Event, ?> pattern;
    Function<Map<String, List<Event>>, String> selectFn;

    PatternConfig(String name, Pattern<Event, ?> pattern, Function<Map<String, List<Event>>, String> selectFn) {
        this.name = name;
        this.pattern = pattern;
        this.selectFn = selectFn;
    }
}

