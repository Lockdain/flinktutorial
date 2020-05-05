package com.alexsergeenko.flink.datastream.split;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class Split {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.readTextFile("/Users/user/IdeaProjects/Flink/flinktutorial/src/main/resources/split/oddeven");
        SplitStream<Integer> evenOddStream = text.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        })
                .split(new OutputSelector<Integer>() {
                    @Override
                    public Iterable<String> select(Integer value) {
                        List<String> out = new ArrayList<String>();
                        if (value % 2 == 0) {
                            out.add("even");
                        } else {
                            out.add("odd");
                        }
                        return out;
                    }
                });
        DataStream<Integer> oddStream = evenOddStream.select("odd");
        DataStream<Integer> evenStream = evenOddStream.select("even");

        oddStream.print();
        //evenStream.print();

        env.execute("Split Example");
    }
}
