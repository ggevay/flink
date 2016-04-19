/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.transformations;

import com.google.common.collect.Lists;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorNG;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @param <OUT> The type of the elements that result from this {@code OneInputTransformation}
 */
@Internal
public class OperatorTransformation<OUT> extends StreamTransformation<OUT> {

	private final Map<StreamOperatorNG.Input<?>, StreamTransformation<?>> inputs;

	private final StreamOperatorNG<OUT> operator;

	/**
	 * Creates a new {@code OneInputTransformation} from the given input and operator.
	 *
	 * @param name The name of the {@code StreamTransformation}, this will be shown in Visualizations and the Log
	 * @param operator The {@code TwoInputStreamOperator}
	 * @param outputType The type of the elements produced by this {@code OneInputTransformation}
	 * @param parallelism The parallelism of this {@code OneInputTransformation}
	 */
	public OperatorTransformation(String name,
			StreamOperatorNG<OUT> operator,
			TypeInformation<OUT> outputType,
			int parallelism) {
		super(name, outputType, parallelism);
		this.operator = operator;
		this.inputs = new HashMap<>();
	}

	public <T> void setInput(StreamOperatorNG.Input<T> input, StreamTransformation<T> transformation) {
		if (inputs.containsKey(input)) {
			throw new IllegalArgumentException("Transformation already has input for " + input);
		}
		inputs.put(input, transformation);
	}

	/**
	 * Returns the input {@code StreamTransformation} of this {@code OneInputTransformation}.
	 */
	public Map<StreamOperatorNG.Input<?>, StreamTransformation<?>> getInputs() {
		return inputs;
	}

	/**
	 * Returns the {@code TwoInputStreamOperator} of this Transformation.
	 */
	public StreamOperatorNG<OUT> getOperator() {
		return operator;
	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		List<StreamTransformation<?>> result = Lists.newArrayList();
		result.add(this);
		for (StreamTransformation<?> input: inputs.values()) {
			result.addAll(input.getTransitivePredecessors());
		}
		return result;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		operator.setChainingStrategy(strategy);
	}
}
