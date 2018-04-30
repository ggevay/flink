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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.util.Attribute;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;

import java.util.List;
import java.util.ListIterator;

/**
 * @author Stepan Koltsov
 */
public class BetterWrite<T> {

	private static final AttributeKey<BetterWrite> KEY = new AttributeKey<>(BetterWrite.class.getName());

	private final Channel channel;
	private final LockFreeStackWithSize<QueueItem<T>> queue = new LockFreeStackWithSize<>();
	private final Tasks tasks = new Tasks();

	private static class QueueItem<T> {
		private final T item;
		//@Nullable
		private final ChannelFutureListener listener;

		private QueueItem(T item, ChannelFutureListener listener) {
			this.item = item;
			this.listener = listener;
		}
	}

	public BetterWrite(Channel channel) {
		this.channel = channel;
	}

	private void flush() {
		int i=0;
		while (tasks.fetchTask()) {
			List<QueueItem<T>> items = queue.removeAllReversed();
			ListIterator<QueueItem<T>> iterator = items.listIterator(items.size());
			while (iterator.hasPrevious()) {
				QueueItem<T> item = iterator.previous();
				ChannelFuture future = channel.write(item.item);
				if (item.listener != null) {
					future.addListener(item.listener);
				}
			}

			i++;
			System.out.println("BetterWrite.flush: " + i);

			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				throw new RuntimeException();
			}

		}
		channel.flush();
	}

	public static <T> void write(Channel channel, T value, ChannelFutureListener listener) {
		BetterWrite<T> betterWriter = getBetterWrite(channel);

		betterWriter.queue.add(new QueueItem<>(value, listener));
		if (betterWriter.tasks.addTask()) {
			channel.pipeline().lastContext().executor().execute(betterWriter::flush);
		}
	}

	private static <T> BetterWrite<T> getBetterWrite(final Channel channel) {
		final Attribute<BetterWrite> attr = channel.attr(KEY);
		BetterWrite<T> betterWriter = attr.get();
		if (betterWriter != null) {
			return betterWriter;
		}
		final BetterWrite<T> value = new BetterWrite<>(channel);
		final BetterWrite<T> old = attr.setIfAbsent(value);
		if (old != null) {
			return old;
		}
		return value;
	}

}
