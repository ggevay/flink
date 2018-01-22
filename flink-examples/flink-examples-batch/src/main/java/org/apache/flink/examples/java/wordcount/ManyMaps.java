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


	public static void main(String[] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);




		DataSet<Long> xs = env.generateSequence(1, 10*1000*1000);

		//System.out.println(xs.count());

//		DataSet<Long> ys = xs
//				.map(new org.apache.flink.runtime.operators.Map1())
//				.map(new org.apache.flink.runtime.operators.Map2())
//				.map(new org.apache.flink.runtime.operators.Map3());
//
//		System.out.println(ys.count());



		xs.map(new org.apache.flink.runtime.operators.Map1()).output(new DiscardingOutputFormat<>());
		xs.map(new org.apache.flink.runtime.operators.Map2()).output(new DiscardingOutputFormat<>());
		xs.map(new org.apache.flink.runtime.operators.Map3()).output(new DiscardingOutputFormat<>());


		long start = System.currentTimeMillis();
		env.execute();
		long end = System.currentTimeMillis();
		System.out.println(end - start);
	}



//	public static final class Map1 implements MapFunction<Long, Long> {
//
//		@Override
//		public Long map(Long value) {
//			return value + 1;
//		}
//	}
//
//	public static final class Map2 implements MapFunction<Long, Long> {
//
//		@Override
//		public Long map(Long value) {
//			return value + 2;
//		}
//	}
//
//	public static final class Map3 implements MapFunction<Long, Long> {
//
//		@Override
//		public Long map(Long value) {
//			return value + 3;
//		}
//	}

}
