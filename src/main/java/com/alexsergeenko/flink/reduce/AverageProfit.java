package com.alexsergeenko.flink.reduce;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfit {

    public static void main(String[] args) throws Exception {
        // Streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("/Users/user/IdeaProjects/Flink/flinktutorial/src/main/resources/reduce/avg");



        env.execute("ReduceExample");
    }
}
