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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datalog.BatchDatalogEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.IntValue;

public class SameGeneration {
    public static void main(String[] args) throws Exception {
        String testFilePath = null;

        if (args.length > 0) {
            testFilePath = args[0].trim();
        } else
            throw new Exception("Please provide input dataset. ");
        String inputProgram =
                "sg(X,Y):-arc(P,X),arc(P,Y),X!=Y.\n" +
                "sg(X,Y):-arc(A,X),sg(A,B),arc(B,Y).\n";
        String query = "sg(X,Y)?";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useDatalogPlanner()
                .inBatchMode()
                .build();
        BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);

        DataSet<Tuple2<IntValue, IntValue>> dataSet = env.readCsvFile(testFilePath).fieldDelimiter(",").types(IntValue.class, IntValue.class);

        datalogEnv.registerDataSet("arc", dataSet, "v1,v2");
        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
        DataSet<Tuple2<IntValue, IntValue>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());

        System.out.println(resultDS.count());
//        resultDS.writeAsCsv(testFilePath + "_output");

    }
}
