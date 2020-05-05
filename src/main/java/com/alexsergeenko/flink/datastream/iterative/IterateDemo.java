package com.alexsergeenko.flink.datastream.iterative;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Long, Integer>> data = env.generateSequence(0, 5)
                .map(new MapFunction<Long, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> map(Long value) throws Exception {
                        return new Tuple2<>(value, 0);
                    }
                });

        IterativeStream<Tuple2<Long, Integer>> iterateStream = data.iterate(5000);
        DataStream<Tuple2<Long, Integer>> mapped = iterateStream
                .map(new MapFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> map(Tuple2<Long, Integer> value) throws Exception {
                        if (value.f0 == 10)
                            return value;
                        else
                            return new Tuple2<Long, Integer>(value.f0 + 1, value.f1 + 1);
                    }
                });

        // Part of the stream to use in the next iteration
        DataStream<Tuple2<Long, Integer>> notEqualToTen = mapped
                .filter(new FilterFunction<Tuple2<Long, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<Long, Integer> value) throws Exception {
                        if (value.f0 == 10)
                            return false;
                        else
                            return true;
                    }
                });

        // Feed data back to the next iteration
        iterateStream.closeWith(notEqualToTen);

        // Data not to feedback to the next iteration
        DataStream<Tuple2<Long, Integer>> equalToTen = mapped
                .filter(new FilterFunction<Tuple2<Long, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<Long, Integer> value) throws Exception {
                        if (value.f0 == 10)
                            return true;
                        else return false;
                    }
                });

        equalToTen.writeAsText("/Users/user/IdeaProjects/Flink/flinktutorial/src/main/resources/iterate/eqToTen");
        notEqualToTen.writeAsText("/Users/user/IdeaProjects/Flink/flinktutorial/src/main/resources/iterate/notEqToTen");

        env.execute("Iterate Demo");
    }
}
