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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.StreamOperatorNG;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;

import java.io.IOException;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask}.
 *
 * <p>
 * This also keeps track of {@link Watermark} events and forwards them to event subscribers
 * once the {@link Watermark} from all inputs advances.
 *
 * <p>
 * Forwarding elements or watermarks must be protected by synchronizing on the given lock
 * object. This ensures that we don't call methods on a {@link TwoInputStreamOperator} concurrently
 * with the timer callback or other things.
 */
@Internal
public class OperatorInputProcessor {

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	// We need to keep track of the channel from which a buffer came, so that we can
	// appropriately map the watermarks to input channels
	private int currentChannel = -1;

	private boolean isFinished;

	private final CheckpointBarrierHandler barrierHandler;

//	private final long[] watermarks1;
//	private long lastEmittedWatermark1;
//
//	private final long[] watermarks2;
//	private long lastEmittedWatermark2;
//
//	private final int numInputChannels1;

	private final int[] edgeMapping;

	private final DeserializationDelegate<StreamElement>[] deserializationDelegates;
	private final StreamOperatorNG.Input<?>[] inputArray;


	@SuppressWarnings({"unchecked", "rawtypes"})
	public OperatorInputProcessor(InputGate[] inputGates,
			int[] edgeMapping,
			TypeSerializer<?>[] serializers,
			StreamOperatorNG.Input<?>[] inputArray,
			EventListener<CheckpointBarrier> checkpointListener,
			CheckpointingMode checkpointMode,
			IOManager ioManager,
			boolean enableWatermarkMultiplexing) throws IOException {

		this.edgeMapping = edgeMapping;
		this.inputArray = inputArray;

		final InputGate inputGate = InputGateUtil.createInputGate(inputGates);

		if (checkpointMode == CheckpointingMode.EXACTLY_ONCE) {
			this.barrierHandler = new BarrierBuffer(inputGate, ioManager);
		}
		else if (checkpointMode == CheckpointingMode.AT_LEAST_ONCE) {
			this.barrierHandler = new BarrierTracker(inputGate);
		}
		else {
			throw new IllegalArgumentException("Unrecognized CheckpointingMode: " + checkpointMode);
		}

		if (checkpointListener != null) {
			this.barrierHandler.registerCheckpointEventHandler(checkpointListener);
		}

		deserializationDelegates = new DeserializationDelegate[serializers.length];
		for (int i = 0; i < serializers.length; i++) {
			if (enableWatermarkMultiplexing) {
				MultiplexingStreamRecordSerializer<?> ser = new MultiplexingStreamRecordSerializer(serializers[i]);
				this.deserializationDelegates[i] = new NonReusingDeserializationDelegate<>(ser);
			}
			else {
				StreamRecordSerializer<?> ser = new StreamRecordSerializer(serializers[i]);
				this.deserializationDelegates[i] = (DeserializationDelegate<StreamElement>)
						(DeserializationDelegate<?>) new NonReusingDeserializationDelegate<>(ser);
			}

		}

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];

		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>();
		}

//		watermarks1 = new long[numInputChannels1];
//		Arrays.fill(watermarks1, Long.MIN_VALUE);
//		lastEmittedWatermark1 = Long.MIN_VALUE;
//
//		watermarks2 = new long[numInputChannels2];
//		Arrays.fill(watermarks2, Long.MIN_VALUE);
//		lastEmittedWatermark2 = Long.MIN_VALUE;
	}

	@SuppressWarnings("unchecked")
	public boolean processInput(Object lock) throws Exception {
		if (isFinished) {
			return false;
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				int inputNum = edgeMapping[currentChannel];
				DeserializationDelegate<StreamElement> deserializationDelegate = deserializationDelegates[inputNum];
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycle();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrWatermark = deserializationDelegate.getInstance();

					if (recordOrWatermark.isWatermark()) {
//						handleWatermark(streamOperator, (Watermark) recordOrWatermark, currentChannel, lock);
						continue;
					}
					else {
						synchronized (lock) {
							inputArray[inputNum].processElement((StreamRecord) recordOrWatermark.asRecord());
						}
						return true;

					}
				}
			}

			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
			if (bufferOrEvent != null) {

				if (bufferOrEvent.isBuffer()) {
					currentChannel = bufferOrEvent.getChannelIndex();
					currentRecordDeserializer = recordDeserializers[currentChannel];
					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());

				} else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}
				}
			}
			else {
				isFinished = true;
				if (!barrierHandler.isEmpty()) {
					throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
				}
				return false;
			}
		}
	}

//	private void handleWatermark(TwoInputStreamOperator<IN1, IN2, ?> operator, Watermark mark, int channelIndex, Object lock) throws Exception {
//		if (channelIndex < numInputChannels1) {
//			long watermarkMillis = mark.getTimestamp();
//			if (watermarkMillis > watermarks1[channelIndex]) {
//				watermarks1[channelIndex] = watermarkMillis;
//				long newMinWatermark = Long.MAX_VALUE;
//				for (long wm : watermarks1) {
//					newMinWatermark = Math.min(wm, newMinWatermark);
//				}
//				if (newMinWatermark > lastEmittedWatermark1) {
//					lastEmittedWatermark1 = newMinWatermark;
//					synchronized (lock) {
//						operator.processWatermark1(new Watermark(lastEmittedWatermark1));
//					}
//				}
//			}
//		} else {
//			channelIndex = channelIndex - numInputChannels1;
//			long watermarkMillis = mark.getTimestamp();
//			if (watermarkMillis > watermarks2[channelIndex]) {
//				watermarks2[channelIndex] = watermarkMillis;
//				long newMinWatermark = Long.MAX_VALUE;
//				for (long wm : watermarks2) {
//					newMinWatermark = Math.min(wm, newMinWatermark);
//				}
//				if (newMinWatermark > lastEmittedWatermark2) {
//					lastEmittedWatermark2 = newMinWatermark;
//					synchronized (lock) {
//						operator.processWatermark2(new Watermark(lastEmittedWatermark2));
//					}
//				}
//			}
//		}
//	}

	public void setReporter(AccumulatorRegistry.Reporter reporter) {
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			deserializer.setReporter(reporter);
		}
	}

	public void cleanup() throws IOException {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycle();
			}
		}

		// cleanup the barrier handler resources
		barrierHandler.cleanup();
	}
}
