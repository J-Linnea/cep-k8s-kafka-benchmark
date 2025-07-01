package de.cau.se;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DirectlyFollowsRelationCounter extends RichFlatMapFunction<DirectlyFollowsRelation, List<CountedDirectlyFollowsRelations>> {

    @Override
    public void flatMap(DirectlyFollowsRelation directlyFollowsRelation, Collector<List<CountedDirectlyFollowsRelations>> counter) {
        System.out.println(directlyFollowsRelation);
        // TODO#2 Implement
    }
}
