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
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import java.io.Serializable;
import java.util.Collection;
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
public abstract class StreamOperatorNG<OUT> implements Serializable {

	// ----------- configuration properties -------------

	// A sane default for most operators
	protected ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;

	private boolean inputCopyDisabled = false;

	// ---------------- runtime fields ------------------

	/** The task that contains this operator (and other operators in the same chain) */
	protected transient OperatorStreamTask<?> container;

	protected transient StreamConfig config;

	protected transient Output<StreamRecord<OUT>> output;

	/** The runtime context for UDFs */
	protected transient StreamingRuntimeContextNG runtimeContext;

	/** The state backend that stores the state and checkpoints for this task */
	protected AbstractStateBackend stateBackend = null;

	// ------------------------------------------------------------------------
	//  life cycle
	// ------------------------------------------------------------------------

	public abstract Collection<Input<?>> getInputs();

	/**
	 * Initializes the operator. Sets access to the context and the output.
	 */
	public void setup(OperatorStreamTask<?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		this.container = containingTask;
		this.config = config;
		this.output = output;

		try {
			String operatorIdentifier = getClass().getSimpleName() + "_" + config.getVertexID() + "_" + container.getEnvironment().getTaskInfo().getIndexOfThisSubtask();
			stateBackend = container.createStateBackend(operatorIdentifier, null);
		} catch (Exception e) {
			throw new RuntimeException("Could not initialize state backend. ", e);
		}

		this.runtimeContext = new StreamingRuntimeContextNG(this, stateBackend, container.getEnvironment(), container.getAccumulatorMap());
	}

	/**
	 * This method is called immediately before any elements are processed, it should contain the
	 * operator's initialization logic.
	 *
	 * @throws Exception An exception in this method causes the operator to fail.
	 */
	public void open() throws Exception {}

	/**
	 * This method is called after all records have been added to the operators via the methods
	 * {@link OneInputStreamOperator#processElement(StreamRecord)}, or
	 * {@link TwoInputStreamOperator#processElement1(StreamRecord)} and
	 * {@link TwoInputStreamOperator#processElement2(StreamRecord)}.
	 *
	 * <p>
	 * The method is expected to flush all remaining buffered data. Exceptions during this flushing
	 * of buffered should be propagated, in order to cause the operation to be recognized asa failed,
	 * because the last data items are not processed properly.
	 *
	 * @throws Exception An exception in this method causes the operator to fail.
	 */
	public void close() throws Exception {}

	/**
	 * This method is called at the very end of the operator's life, both in the case of a successful
	 * completion of the operation, and in the case of a failure and canceling.
	 * 
	 * This method is expected to make a thorough effort to release all resources
	 * that the operator has acquired.
	 */
	public void dispose() {}

	// ------------------------------------------------------------------------
	//  state snapshots
	// ------------------------------------------------------------------------

	/**
	 * Called to draw a state snapshot from the operator. This method snapshots the operator state
	 * (if the operator is stateful) and the key/value state (if it is being used and has been
	 * initialized).
	 *
	 * @param checkpointId The ID of the checkpoint.
	 * @param timestamp The timestamp of the checkpoint.
	 *
	 * @return The StreamTaskState object, possibly containing the snapshots for the
	 *         operator and key/value state.
	 *
	 * @throws Exception Forwards exceptions that occur while drawing snapshots from the operator
	 *                   and the key/value state.
	 */
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		return null;
	}
	
	/**
	 * Restores the operator state, if this operator's execution is recovering from a checkpoint.
	 * This method restores the operator state (if the operator is stateful) and the key/value state
	 * (if it had been used and was initialized when the snapshot occurred).
	 *
	 * <p>This method is called after {@link #setup(StreamTask, StreamConfig, Output)}
	 * and before {@link #open()}.
	 *
	 * @param state The state of operator that was snapshotted as part of checkpoint
	 *              from which the execution is restored.
	 * 
	 * @param recoveryTimestamp Global recovery timestamp
	 *
	 * @throws Exception Exceptions during state restore should be forwarded, so that the system can
	 *                   properly react to failed state restore and fail the execution attempt.
	 */
	public void restoreState(StreamTaskState state, long recoveryTimestamp) throws Exception {}

	/**
	 * Called when the checkpoint with the given ID is completed and acknowledged on the JobManager.
	 *
	 * @param checkpointId The ID of the checkpoint that has been completed.
	 *
	 * @throws Exception Exceptions during checkpoint acknowledgement may be forwarded and will cause
	 *                   the program to fail and enter recovery.
	 */
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {}

	// ------------------------------------------------------------------------
	//  Properties and Services
	// ------------------------------------------------------------------------

	/**
	 * Gets the execution config defined on the execution environment of the job to which this
	 * operator belongs.
	 *
	 * @return The job's execution config.
	 */
	public ExecutionConfig getExecutionConfig() {
		return container.getExecutionConfig();
	}

	public StreamConfig getOperatorConfig() {
		return config;
	}

	public OperatorStreamTask<?> getContainingTask() {
		return container;
	}

	public ClassLoader getUserCodeClassloader() {
		return container.getUserCodeClassLoader();
	}

	/**
	 * Returns a context that allows the operator to query information about the execution and also
	 * to interact with systems such as broadcast variables and managed state. This also allows
	 * to register timers.
	 */
	public StreamingRuntimeContextNG getRuntimeContext() {
		return runtimeContext;
	}

	/**
	 * Register a timer callback. At the specified time the {@link Triggerable} will be invoked.
	 * This call is guaranteed to not happen concurrently with method calls on the operator.
	 *
	 * @param time The absolute time in milliseconds.
	 * @param target The target to be triggered.
	 */
	protected void registerTimer(long time, Triggerable target) {
		container.registerTimer(time, target);
	}

	// ------------------------------------------------------------------------
	//  Context and chaining properties
	// ------------------------------------------------------------------------

	public final void setChainingStrategy(ChainingStrategy strategy) {
		this.chainingStrategy = strategy;
	}

	public final ChainingStrategy getChainingStrategy() {
		return chainingStrategy;
	}

	/**
	 * An operator can return true here to disable copying of its input elements. This overrides
	 * the object-reuse setting on the {@link org.apache.flink.api.common.ExecutionConfig}
	 */
	public final boolean isInputCopyingDisabled() {
		return inputCopyDisabled;
	}

	/**
	 * Enable object-reuse for this operator instance. This overrides the setting in
	 * the {@link org.apache.flink.api.common.ExecutionConfig}
	 */
	public final void disableInputCopy() {
		this.inputCopyDisabled = true;
	}

	public static abstract class Input<T> implements Serializable {
		private final UUID uuid = UUID.randomUUID();

		public abstract void processElement(StreamRecord<T> element) throws Exception;

		public abstract void processWatermark(Watermark watermark) throws Exception;

		@Override
		public final boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			Input<?> input = (Input<?>) o;

			return uuid.equals(input.uuid);

		}

		@Override
		public final int hashCode() {
			return uuid.hashCode();
		}
	}
}
