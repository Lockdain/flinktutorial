package com.alexsergeenko.flink.dataset.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

public class LeftOuterJoinExample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> personSet = env.readTextFile(params.get("input1"));
        DataSet<String> locationSet = env.readTextFile(params.get("input2"));

        // Read a file of "1, Mike 2, Joe" and map it to tuples
        MapOperator<String, Tuple2<Integer, String>> persons = personSet
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String value) {
                        String[] words = value.split(",");
                        return new Tuple2<>(Integer.parseInt(words[0]), words[1]);
                    }});

        // Read a locations file
        DataSet<Tuple2<Integer, String>> locations = locationSet
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String value) throws Exception {
                        String[] words = value.split(",");
                        return new Tuple2<>(Integer.parseInt(words[0]), words[1]);
                    }
                });

        // Join datasets on personId
        // The result will be <id, personName, state>
        DataSet<Tuple3<Integer, String, String>> joined =
                persons
                        .leftOuterJoin(locations)
                        .where(0)
                        .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) {
                        if (location == null) {
                            return new Tuple3<>(person.f0, person.f1, "NULL");
                        }
                        return new Tuple3<>(person.f0, person.f1, location.f1);
                    }
                });

        joined.writeAsCsv(params.get("output"), "\n", " ", FileSystem.WriteMode.OVERWRITE);
        env.execute("Left Outer Join Example");
    }
}
