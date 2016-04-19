/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.examples.wordcount.WordCount;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static javafx.scene.input.KeyCode.T;

/**
 * Implements a windowed version of the streaming "WordCount" program.
 *
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 *
 * <p>
 * Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt; --window &lt;n&gt; --slide &lt;n&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link org.apache.flink.examples.java.wordcount.util.WordCountData}.
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>use basic windowing abstractions.
 * </ul>
 *
 */
public class WindowWordCount {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {


		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.disableOperatorChaining();

		DataStreamSource<String> source1 = env.addSource(new InfiniteSource());
//		DataStreamSource<String> source1 = env.fromElements("Hello", "Here");
//		DataStreamSource<String> source2 = env.fromElements("foo", "bar");
//		DataStreamSource<String> source3 = env.fromElements("baz", "bat");

		source1
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
//						System.out.println("GOT: " + value);
						return value;
					}
				})
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
//						System.out.println("GOT: " + value);
						return value;
					}
				})
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
//						System.out.println("GOT: " + value);
						return value;
					}
				})
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
//						System.out.println("GOT: " + value);
						return value;
					}
				})
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
//						System.out.println("GOT: " + value);
						return value;
					}
				})
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
//						System.out.println("GOT: " + value);
						return value;
					}
				})
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
//						System.out.println("GOT: " + value);
						return value;
					}
				})
//				.keyBy(new KeySelector<String, String>() {
//					@Override
//					public String getKey(String value) throws Exception {
//						return value;
//					}
//				})
				.map(new RichMapFunction<String, Object>() {

					long count = 0;
					long startTime = System.currentTimeMillis();

					@Override
					public Object map(String value) throws Exception {
//						ValueState<Long> countState = getRuntimeContext().getState(new ValueStateDescriptor<>(
//								"count",
//								LongSerializer.INSTANCE,
//								0L));
//						countState.update(countState.value() + 1);
//						System.out.println("COUNT STATE: " + countState.value());
						this.count++;
						if (this.count > 10_000_000) {
							long currentTime = System.currentTimeMillis();

							System.out.println("e/s: " + ((float) this.count / (currentTime - startTime) * 1000));

							this.count = 0;
							startTime = currentTime;
						}
						return null;
					}
				});


//		MyOperator myOperator = new MyOperator();
//
//		OperatorTransformation<String> transform = new OperatorTransformation<>("My Operator",
//				myOperator,
//				BasicTypeInfo.STRING_TYPE_INFO,
//				env.getParallelism());
//
//		transform.setInput(myOperator.input1, source1.getTransformation());
//		transform.setInput(myOperator.input2, source2.getTransformation());
//		transform.setInput(myOperator.input3, source3.getTransformation());
//
//		env.addOperator(transform);

		// execute program
		env.execute("operator NG");
	}

	public static class InfiniteSource implements SourceFunction<String> {
		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (true) {
				ctx.collect("hello");
			}
		}

		@Override
		public void cancel() {

		}
	}


//	private static class MyOperator extends StreamOperatorNG<String> {
//
//		Input<String> input1 = new Input<String>() {
//			@Override
//			public void processElement(StreamRecord<String> element) throws Exception {
//				System.out.println("GOT (ON INPUT 1): " + element.getValue());
//				output.collect(new StreamRecord<>(element.getValue()));
//			}
//
//			@Override
//			public void processWatermark(Watermark watermark) {
//
//			}
//		};
//
//		Input<String> input2 = new Input<String>() {
//			@Override
//			public void processElement(StreamRecord<String> element) throws Exception {
//				System.out.println("GOT (ON INPUT 2): " + element.getValue());
//				output.collect(new StreamRecord<>(element.getValue()));
//			}
//
//			@Override
//			public void processWatermark(Watermark watermark) {
//
//			}
//		};
//
//		Input<String> input3 = new Input<String>() {
//			@Override
//			public void processElement(StreamRecord<String> element) throws Exception {
//				System.out.println("GOT (ON INPUT 3): " + element.getValue());
//				output.collect(new StreamRecord<>(element.getValue()));
//			}
//
//			@Override
//			public void processWatermark(Watermark watermark) {
//
//			}
//		};
//
//
//
//		@Override
//		public Collection<Input<?>> getInputs() {
//			List<Input<?>> result = new LinkedList<>();
//			result.add(input1);
//			result.add(input2);
//			result.add(input3);
//			return result;
//		}
//	}
}
