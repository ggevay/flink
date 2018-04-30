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

import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 *
 * @author Stepan Koltsov
 */
public class Tasks {

	private enum State {
		/** actor is not currently running */
		WAITING,
		/** actor is running, and has no more tasks */
		RUNNING_NO_TASKS,
		/** actor is running, but some queues probably updated, actor needs to recheck them */
		RUNNING_GOT_TASKS,
	}

	private final AtomicInteger state = new AtomicInteger();

	/**
	 * @return <code>true</code> iff we have to recheck queues
	 */
	public boolean fetchTask() {
		int old = state.getAndDecrement();
		if (old == State.RUNNING_GOT_TASKS.ordinal()) {
			return true;
		} else if (old == State.RUNNING_NO_TASKS.ordinal()) {
			return false;
		} else {
			throw new AssertionError();
		}
	}

	/**
	 * @return <code>true</code> iff caller has to schedule task execution
	 */
	public boolean addTask() {
		// fast track for high-load applications
		// atomic get is cheaper than atomic swap
		// for both this thread and fetching thread
		if (state.get() == State.RUNNING_GOT_TASKS.ordinal())
			return false;

		int old = state.getAndSet(State.RUNNING_GOT_TASKS.ordinal());
		return old == State.WAITING.ordinal();
	}

}
