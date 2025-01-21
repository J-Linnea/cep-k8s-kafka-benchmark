package de.cau.se;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class DirectlyFollowsBuilder extends RichFlatMapFunction<Event, DirectlyFollowsRelation> {

    final CaseIdMap<String> caseIdMap = new CaseIdMap<>();

    @Override
    public void flatMap(Event value, Collector<DirectlyFollowsRelation> out) throws InterruptedException {
        String lastActivity = caseIdMap.accept(
                value.getCaseId(),
                value.getActivity());
        if (lastActivity != null) {
            out.collect(
                    new DirectlyFollowsRelation(
                            lastActivity,
                            value.getActivity()
                    )
            );
        }
    }
}