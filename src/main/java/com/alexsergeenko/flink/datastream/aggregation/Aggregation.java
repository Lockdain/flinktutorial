package com.alexsergeenko.flink.datastream.aggregation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregation {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("/Users/user/IdeaProjects/Flink/flinktutorial/src/main/resources/aggregation/source/avg");

        DataStream<Tuple4<String, String, String, Integer>> mapped = data.map(new Splitter());
        mapped
                .keyBy(0)
                .sum(3)
                .writeAsText("/Users/user/IdeaProjects/Flink/flinktutorial/src/main/resources/aggregation/sum.out", FileSystem.WriteMode.OVERWRITE);

        mapped
                .keyBy(0)
                .min(3)
                .writeAsText("/Users/user/IdeaProjects/Flink/flinktutorial/src/main/resources/aggregation/min.out", FileSystem.WriteMode.OVERWRITE);

        // minBy wil save all the previous state fields of the tuple (min - will not)
        mapped
                .keyBy(0)
                .minBy(3)
                .writeAsText("/Users/user/IdeaProjects/Flink/flinktutorial/src/main/resources/aggregation/minBy.out", FileSystem.WriteMode.OVERWRITE);

        // The same for max and maxBy

        env.execute("Aggregation Example");

    }

    public static class Splitter implements MapFunction<String, Tuple4<String, String, String, Integer>> {

        @Override
        public Tuple4<String, String, String, Integer> map(String value) {
            String[] strings = value.split(",");

            return new Tuple4<>(strings[1], strings[2], strings[3], Integer.parseInt(strings[4]));
        }
    }
}
