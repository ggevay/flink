package org.apache.flink.runtime.operators.resettable;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.util.ResettableMutableObjectIterator;

import java.io.IOException;
import java.util.List;

public interface SpillingResettableMutableObjectIterator<T> extends ResettableMutableObjectIterator<T> {
    void open();

    @Override
    void reset() throws IOException;

    List<MemorySegment> close() throws IOException;

    @Override
    T next(T reuse) throws IOException;

    @Override
    T next() throws IOException;

    void consumeAndCacheRemainingData() throws IOException;
}
