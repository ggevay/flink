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

package org.apache.flink.runtime.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;

import java.util.ArrayList;
import java.util.List;

public class GetItFromTheMemoryManagerMemorySegmentSource implements MemorySegmentSource {

	private final MemoryManager memoryManager;
	private final Object owner;
	private final int maxNumPages;
	private int numAllocated = 0;

	public GetItFromTheMemoryManagerMemorySegmentSource(final Object owner, final MemoryManager memoryManager, int maxNumPages) {
		this.memoryManager = memoryManager;
		this.owner = owner;
		this.maxNumPages = maxNumPages;
	}

	@Override
	public MemorySegment nextSegment() {
		try {
			if (numAllocated < maxNumPages) {
				numAllocated++;
				return this.memoryManager.allocatePages(owner, 1).get(0);
			} else {
				return null;
			}
		} catch (MemoryAllocationException e) {
			return null;
		}
	}


}
