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

package org.apache.flink.datalog;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datalog.BatchDatalogEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * This class contains test cases for non recursive queries.
 */
//public class NonRecursiveQueriesTest {
//    /**
//     *
//     */
//    @Before
//    public void initEnvs() {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .useDatalogPlanner()
//                .inBatchMode()
//                .build();
//		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
//    }
//
//    /**
//     * @throws Exception
//     */
//    @Test
//    public void testSelection() throws Exception {
//        String inputProgram = "sel(X,Y) :- graph(X,Y).\n";
//        String query = "sel(X,Y)?";
//
//		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		EnvironmentSettings settings = EnvironmentSettings
//				.newInstance()
//				.useDatalogPlanner()
//				.inBatchMode()
//				.build();
//		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
//
//		DataSet<Tuple2<String, String>> dataSet = env.fromElements(
//                new Tuple2<>("a", "b"),
//                new Tuple2<>("b", "c"),
//                new Tuple2<>("c", "c"),
//                new Tuple2<>("c", "d"));
//        datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
//
//        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
//        DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
//        List<Tuple2<String, String>> actual = resultDS.collect();
//        List<Tuple2<String, String>> expected = List
//                .of(new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""));
//        assertTrue(CollectionUtils.isEqualCollection(actual, expected));
//    }
//
//    /**
//     * @throws Exception
//     */
//    @Test
//    public void testSelectionAndFilering() throws Exception {
//        String inputProgram = "sel(X,Y) :- graph(X,Y), X!=a.\n";
//        String query = "sel(X,Y)?";
//
//		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		EnvironmentSettings settings = EnvironmentSettings
//				.newInstance()
//				.useDatalogPlanner()
//				.inBatchMode()
//				.build();
//		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
//
//		DataSet<Tuple2<String, String>> dataSet = env.fromElements(
//                new Tuple2<>("a", "b"),
//                new Tuple2<>("b", "c"),
//                new Tuple2<>("c", "c"),
//                new Tuple2<>("c", "d"));
//        datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
//        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
//        DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
//        List<Tuple2<String, String>> actual = resultDS.collect();
//        List<Tuple2<String, String>> expected = List
//                .of(new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""));
//        assertTrue(CollectionUtils.isEqualCollection(actual, expected));
//    }
//
//    /**
//     * @throws Exception
//     */
//    @Test
//    public void testSimpleJoin() throws Exception {
//        String inputProgram = "sel(X,Y) :- graph(X,Z), graph(Z,Y).\n";
//        String query = "sel(X,Y)?";
//
//		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		EnvironmentSettings settings = EnvironmentSettings
//				.newInstance()
//				.useDatalogPlanner()
//				.inBatchMode()
//				.build();
//		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
//
//		DataSet<Tuple2<String, String>> dataSet = env.fromElements(
//                new Tuple2<>("a", "b"),
//                new Tuple2<>("b", "c"),
//                new Tuple2<>("c", "c"),
//                new Tuple2<>("c", "d"));
//        datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
//        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
//        DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
//        List<Tuple2<String, String>> actual = resultDS.collect();
//        List<Tuple2<String, String>> expected = List
//                .of(new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""));
//        assertTrue(CollectionUtils.isEqualCollection(actual, expected));
//    }
//
//    /**
//     * @throws Exception
//     */
//    @Test
//    public void testSimpleJoinAndFilter() throws Exception {
//        String inputProgram = "sel(X,Y) :- graph(X,Z), graph(Z,Y), X!=Y.\n";
//        String query = "sel(X,Y)?";
//
//		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		EnvironmentSettings settings = EnvironmentSettings
//				.newInstance()
//				.useDatalogPlanner()
//				.inBatchMode()
//				.build();
//		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
//
//		DataSet<Tuple2<String, String>> dataSet = env.fromElements(
//                new Tuple2<>("a", "b"),
//                new Tuple2<>("b", "c"),
//                new Tuple2<>("c", "c"),
//                new Tuple2<>("c", "d"));
//        datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
//        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
//        DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
//        List<Tuple2<String, String>> actual = resultDS.collect();
//        List<Tuple2<String, String>> expected = List
//                .of(new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""));
//        assertTrue(CollectionUtils.isEqualCollection(actual, expected));
//    }
//
//    /**
//     * @throws Exception
//     */
//    @Test
//    public void testSimpleUnion() throws Exception {
//        String inputProgram = "";//todo
//        String query = "sel(X,Y)?";
//
//		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		EnvironmentSettings settings = EnvironmentSettings
//				.newInstance()
//				.useDatalogPlanner()
//				.inBatchMode()
//				.build();
//		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
//
//		DataSet<Tuple2<String, String>> dataSet = env.fromElements(
//                new Tuple2<>("a", "b"),
//                new Tuple2<>("b", "c"),
//                new Tuple2<>("c", "c"),
//                new Tuple2<>("c", "d"));
//        datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
//        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
//        DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
//        List<Tuple2<String, String>> actual = resultDS.collect();
//        List<Tuple2<String, String>> expected = List
//                .of(new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""));
//        assertTrue(CollectionUtils.isEqualCollection(actual, expected));
//    }
//
//    /**
//     * @throws Exception
//     */
//    @Test
//    public void testUnionAndJoin() throws Exception {
//        String inputProgram = "";//todo
//        String query = "sel(X,Y)?";
//
//		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		EnvironmentSettings settings = EnvironmentSettings
//				.newInstance()
//				.useDatalogPlanner()
//				.inBatchMode()
//				.build();
//		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
//
//		DataSet<Tuple2<String, String>> dataSet = env.fromElements(
//                new Tuple2<>("a", "b"),
//                new Tuple2<>("b", "c"),
//                new Tuple2<>("c", "c"),
//                new Tuple2<>("c", "d"));
//        datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
//        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
//        DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
//        List<Tuple2<String, String>> actual = resultDS.collect();
//        List<Tuple2<String, String>> expected = List
//                .of(new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""));
//        assertTrue(CollectionUtils.isEqualCollection(actual, expected));
//    }
//
//    /**
//     * @throws Exception
//     */
//    @Test
//    public void testUnionJoinFilter() throws Exception {
//        String inputProgram = "";//todo
//        String query = "sel(X,Y)?";
//
//		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		EnvironmentSettings settings = EnvironmentSettings
//				.newInstance()
//				.useDatalogPlanner()
//				.inBatchMode()
//				.build();
//		BatchDatalogEnvironment datalogEnv = BatchDatalogEnvironment.create(env, settings);
//
//        DataSet<Tuple2<String, String>> dataSet = env.fromElements(
//                new Tuple2<>("a", "b"),
//                new Tuple2<>("b", "c"),
//                new Tuple2<>("c", "c"),
//                new Tuple2<>("c", "d"));
//        datalogEnv.registerDataSet("graph", dataSet, "v1,v2");
//        Table queryResult = datalogEnv.datalogQuery(inputProgram, query);
//        DataSet<Tuple2<String, String>> resultDS = datalogEnv.toDataSet(queryResult, dataSet.getType());
//        List<Tuple2<String, String>> actual = resultDS.collect();
//        List<Tuple2<String, String>> expected = List
//                .of(new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""),
//                        new Tuple2<>("", ""));
//        assertTrue(CollectionUtils.isEqualCollection(actual, expected));
//    }
//}
