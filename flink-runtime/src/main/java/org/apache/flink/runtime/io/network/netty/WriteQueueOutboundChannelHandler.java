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

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is taken from
 * https://gist.github.com/jadbaz/47d98da0ead2e71659f343b14ef05de6
 */
public class WriteQueueOutboundChannelHandler extends ChannelOutboundHandlerAdapter {
	private static final int QUEUE_SIZE_WARNING = 5000;
	private ChannelHandlerContext ctx;
	private final Queue<Object> messageQueue = new ConcurrentLinkedQueue<>();
	private AtomicInteger qSize = new AtomicInteger();
	private boolean isWriting;

	private final ChannelFutureListener sendListener = new ChannelFutureListener() {
		@Override
		public void operationComplete(ChannelFuture future) {
			if (future.isSuccess()) {
				ctx.fireUserEventTriggered("WRITE_MESSAGE_COMPLETE"); //might want to do a message counter in another handler
				poll();
			} else {
				future.channel().close();
				messageQueue.clear();
			}
		}
	};
	private void poll() {
		isWriting = true;
		if (!messageQueue.isEmpty()) {
			this.ctx.writeAndFlush(messageQueue.poll()).addListener(sendListener);
			qSize.decrementAndGet();
		}
		else {
			isWriting = false;
		}
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		this.ctx = ctx;
		int size = qSize.get();
		if (size > QUEUE_SIZE_WARNING) {
			System.out.println("Queue size: "+size); //should handle somehow
		}
		messageQueue.offer(msg);
		qSize.incrementAndGet();
		if (!isWriting) {
			poll();
		}
	}
}
