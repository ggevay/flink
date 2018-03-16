/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.examples.java.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class ManyMaps {

	private static int n = 1;

	private static long[] times = new long[n];

	public static void main(String[] args) throws Exception {

		for (int i=0; i<n; i++) {
			run(i);
		}

		for (int i=0; i<n; i++) {
			System.out.println(times[i]);
		}
	}



	public static void run(int i) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataSet<Long> xs = env.generateSequence(1, 100*1000*1000);

		xs.map(new org.apache.flink.runtime.operators.Map1()).output(new DiscardingOutputFormat<>());
		xs.map(new org.apache.flink.runtime.operators.Map2()).output(new DiscardingOutputFormat<>());
		xs.map(new org.apache.flink.runtime.operators.Map3()).output(new DiscardingOutputFormat<>());


		long start = System.nanoTime();
		env.execute();
		long end = System.nanoTime();
		long elapsed = end - start;
		//System.out.println(elapsed);
		times[i] = elapsed;
	}

}
