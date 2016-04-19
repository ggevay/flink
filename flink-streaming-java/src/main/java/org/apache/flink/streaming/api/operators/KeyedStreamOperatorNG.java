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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;

/**
 * Basic interface for stream operators.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with
 * methods on {@code StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public abstract class KeyedStreamOperatorNG<K, OUT> extends StreamOperatorNG<OUT> {

	private final TypeSerializer<K> keySerializer;

	public KeyedStreamOperatorNG(TypeSerializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}

	@Override
	public void setup(OperatorStreamTask<?> containingTask,
			StreamConfig config,
			Output<StreamRecord<OUT>> output) {

		this.container = containingTask;
		this.config = config;
		this.output = output;

		try {
			String operatorIdentifier = getClass().getSimpleName() + "_" + config.getVertexID() + "_" + container.getEnvironment().getTaskInfo().getIndexOfThisSubtask();
			stateBackend = container.createStateBackend(operatorIdentifier, keySerializer);
		} catch (Exception e) {
			throw new RuntimeException("Could not initialize state backend. ", e);
		}

		this.runtimeContext = new StreamingRuntimeContextNG(this, stateBackend, container.getEnvironment(), container.getAccumulatorMap());

	}

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId,
			long timestamp) throws Exception {
		StreamTaskState state = super.snapshotOperatorState(checkpointId, timestamp);

		if (stateBackend != null) {
			HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>> partitionedSnapshots =
					stateBackend.snapshotPartitionedState(checkpointId, timestamp);
			if (partitionedSnapshots != null) {
				state.setKvStates(partitionedSnapshots);
			}
		}
		return state;

	}

	@Override
	public void restoreState(StreamTaskState state, long recoveryTimestamp) throws Exception {
		super.restoreState(state, recoveryTimestamp);

		// restore the key/value state. the actual restore happens lazily, when the function requests
		// the state again, because the restore method needs information provided by the user function
		stateBackend.injectKeyValueStateSnapshots((HashMap)state.getKvStates(), recoveryTimestamp);
	}

	@Override
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		super.notifyOfCompletedCheckpoint(checkpointId);

		stateBackend.notifyOfCompletedCheckpoint(checkpointId);
	}
}
