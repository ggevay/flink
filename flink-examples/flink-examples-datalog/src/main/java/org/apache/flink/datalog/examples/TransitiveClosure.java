/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.datalog.examples;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.datalog.BatchDatalogEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.IntValue;

import java.io.File;
import java.io.PrintWriter;

public class TransitiveClosure {
    public static void main(String[] args) throws Exception {
        String testFilePath = null;

        if (args.length > 0) {
            testFilePath = args[0].trim();
        } else
            throw new Exception("Please provide input dataset. ");
        String inputProgram =
                          "tc(X,Y) :- graph(X,Y).\n"
                        + "tc(X,Y) :- tc(X,Z),graph(Z,Y).\n";
        String query = "tc(X,Y)?";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        Configuration conf = new Configuration();
//        conf.setString("taskmanager.memory.managed.size","8g"); //(8g orig), 1400m crashed, 1500m finishes // After memory management changes: 500m crashed, 600m finishes
//        conf.setString("taskmanager.numberOfTaskSlots","6");
//        conf.setBoolean("pipeline.object-reuse",true);
//        conf.setBoolean("datalog-merge",true);
//        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(conf);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useDatalogPlanner()
                .inBatchMode()
                .build();
        BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
        DataSet<Tuple2<IntValue, IntValue>> dataSet = env.readCsvFile(testFilePath).fieldDelimiter(",").types(IntValue.class, IntValue.class);

        datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
        DataSet<Tuple2<IntValue, IntValue>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());

        long start = System.currentTimeMillis();

        resultDS.output(new DiscardingOutputFormat<>());
        System.out.println(env.getExecutionPlan());


//        resultDS.writeAsCsv(testFilePath+"_output");
        System.out.println(resultDS.count());

        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time: " + (double)timeElapsed/1000);

    }
}
