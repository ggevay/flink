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

package org.apache.flink.streaming.api.operators;

import com.google.common.collect.Lists;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

@Internal
public class StreamMap<IN, OUT>
		extends AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	public StreamMap(MapFunction<IN, OUT> mapper) {
		super(mapper);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		output.collect(element.replace(userFunction.map(element.getValue())));
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		output.emitWatermark(mark);
	}

	public static class Unkeyed<I, O> extends StreamOperatorNG<O> {

		private final MapFunction<I, O> mapper;

		public Unkeyed(MapFunction<I, O> mapper) {
			this.mapper = mapper;
		}

		public Input<I> input = new Input<I>() {
			@Override
			public void processElement(StreamRecord<I> element) throws Exception {
				output.collect(new StreamRecord<>(mapper.map(element.getValue())));
			}

			@Override
			public void processWatermark(Watermark watermark) {

			}
		};

		@Override
		public Collection<Input<?>> getInputs() {
			return (Collection) Collections.singletonList(input);

		}
	}

	public static class Keyed<K, I, O> extends KeyedStreamOperatorNG<K, O> {

		private final MapFunction<I, O> mapper;
		private final KeySelector<I, K> keySelector;

		public Keyed(KeySelector<I, K> keySelector, TypeSerializer<K> keySerializer, MapFunction<I, O> mapper) {
			super(keySerializer);
			this.mapper = mapper;
			this.keySelector = keySelector;
		}

		@Override
		public void setup(OperatorStreamTask<?> containingTask,
				StreamConfig config,
				Output<StreamRecord<O>> output) {
			super.setup(containingTask, config, output);
			FunctionUtils.setFunctionRuntimeContext(mapper, getRuntimeContext());

		}

		@Override
		public void open() throws Exception {
			super.open();
			FunctionUtils.openFunction(mapper, new Configuration());
		}

		@Override
		public void close() throws Exception {
			super.close();
			FunctionUtils.closeFunction(mapper);
		}

		public Input<I> input = new Input<I>() {
			@Override
			public void processElement(StreamRecord<I> element) throws Exception {
				stateBackend.setCurrentKey(keySelector.getKey(element.getValue()));
				output.collect(new StreamRecord<>(mapper.map(element.getValue())));
				stateBackend.setCurrentKey(null);
			}

			@Override
			public void processWatermark(Watermark watermark) {

			}
		};

		@Override
		public Collection<Input<?>> getInputs() {
			return (Collection) Collections.singletonList(input);

		}
	}
}
