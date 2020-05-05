package com.alexsergeenko.flink.fold;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FoldOperation {

    public static void main(String[] args) throws Exception {
        // Streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("/Users/user/IdeaProjects/Flink/flinktutorial/src/main/resources/reduce/avg");

        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter());

        DataStream<Tuple4<String, String, Integer, Integer>> reduced = mapped.keyBy(0)
                .fold(new Tuple4<String, String, Integer, Integer>("", "", 0, 0), new FoldFunction1());
        //reduced.print();

        // Month avg profit
        DataStream<Tuple2<String, Double>> profitPerMonth = reduced.map(new MapFunction<Tuple4<String, String, Integer, Integer>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Tuple4<String, String, Integer, Integer> input) throws Exception {
                return new Tuple2<String, Double>(input.f0, new Double(input.f2 * 1.0) / input.f3);
            }
        });

        profitPerMonth.print();

        env.execute("Reduce Example");

    }

    public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {

        @Override
        public Tuple5<String, String, String, Integer, Integer> map(String s) {
            String[] strings = s.split(",");
            return new Tuple5<String, String, String, Integer, Integer>(strings[1], strings[2], strings[3], Integer.parseInt(strings[4]), 1);
        }
    }

    public static class FoldFunction1 implements org.apache.flink.api.common.functions.FoldFunction<Tuple5<String,
            String, String, Integer, Integer>, Tuple4<String, String, Integer, Integer>> {

        @Override
        public Tuple4<String, String, Integer, Integer> fold(Tuple4<String, String, Integer, Integer> defaultIn,
                                                             Tuple5<String, String, String, Integer, Integer> curr) {
            defaultIn.f0 = curr.f0;
            defaultIn.f1 = curr.f1;
            defaultIn.f2 += curr.f3;
            defaultIn.f3 += curr.f4;

            return defaultIn;
        }
    }
}

