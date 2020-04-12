package org.apache.flink.runtime.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class MemorySegmentObjectPoolMap {

	private final ConcurrentHashMap<Object, MemorySegmentObjectPool> map = new ConcurrentHashMap<>();


	public MemorySegment getOrCreateUnpooledSegment(int size, Object owner) {
		return getForOwner(owner).getOrCreateUnpooledSegment(size, owner);
	}

	public MemorySegment getOrCreateOffHeapUnsafeMemory(int size, Object owner) {
		return getForOwner(owner).getOrCreateOffHeapUnsafeMemory(size, owner);
	}

	public void returnToPool(MemorySegment segment) {
		getForOwner(segment.getOwner()).returnToPool(segment);
	}

	public void freeAllAndClear() {
		map.values().forEach(MemorySegmentObjectPool::freeAllAndClear);
	}


	private MemorySegmentObjectPool getForOwner(Object owner) {
		return map.computeIfAbsent(owner, o -> new MemorySegmentObjectPool());
	}
}
