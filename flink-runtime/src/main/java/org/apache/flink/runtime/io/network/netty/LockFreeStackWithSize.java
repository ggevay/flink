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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lock-free stack with constant-time {@link #size()} operation.
 *
 * @author Stepan Koltsov
 */
public class LockFreeStackWithSize<T> {

	private static class Node<T> {
		private volatile Node<T> next;
		private int size;
		private T payload;
	}

	private static final Node<?> tail = new Node<Object>();

	private final AtomicReference<Node<T>> root = new AtomicReference<Node<T>>((Node<T>) tail);

	/**
	 * Add element to the stack.
	 * @return <code>true</code>
	 */
	public boolean add(T value) {
		Node<T> newRoot = new Node<T>();
		newRoot.payload = value;
		for (;;) {
			Node<T> oldRoot = this.root.get();
			newRoot.next = oldRoot;
			newRoot.size = oldRoot.size + 1;
			if (root.compareAndSet(oldRoot, newRoot))
				return true;
		}
	}

	/**
	 * Constant-time size operation.
	 */
	public int size() {
		return root.get().size;
	}

	/**
	 * Dequeue all works faster than calling dequeue in loop.
	 */
	public List<T> removeAllReversed() {
		List<T> result = new ArrayList<T>(size() + 100);

		Node<T> r;

		for (;;) {
			r = root.get();
			if (root.compareAndSet(r, (Node<T>) tail))
				break;
		}

		while (r != tail) {
			result.add(r.payload);
			r = r.next;
		}

		return result;
	}
}
