package de.cau.se;

import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.List;

public class MockModelBuilder extends RichMapFunction<List<CountedDirectlyFollowsRelations>, List<CountedDirectlyFollowsRelations>> {

    private int computationDuration;

    public MockModelBuilder(int computationDuration) {
        this.computationDuration = computationDuration;
    }

    @Override
    public List<CountedDirectlyFollowsRelations> map(final List<CountedDirectlyFollowsRelations> value) throws Exception {
        Thread.sleep(computationDuration);
        return value;
    }
}
