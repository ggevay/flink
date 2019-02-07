package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.SpecUtil;

import java.io.IOException;

public interface RecordWriter<T extends IOReadableWritable> {
    static RecordWriter createRecordWriter(
            ResultPartitionWriter writer,
            ChannelSelector channelSelector,
            long timeout,
            String taskName) {
        if (channelSelector.isBroadcast()) {
            return new BroadcastRecordWriter<>(writer, channelSelector, timeout, taskName);
// This would need creating an interface for BroadcastRecordWriter
//			return SpecUtil.copyClassAndInstantiate("org.apache.flink.runtime.io.network.api.writer.BroadcastRecordWriter",
//					writer, channelSelector, timeout, taskName);
        } else {
            //return new RecordWriterConcrete<>(writer, channelSelector, timeout, taskName);
			return SpecUtil.copyClassAndInstantiate(taskName, "org.apache.flink.runtime.io.network.api.writer.RecordWriterConcrete",
					writer, channelSelector, timeout, taskName);
        }
    }

    static RecordWriter createRecordWriter(
            ResultPartitionWriter writer,
            ChannelSelector channelSelector,
            String taskName) {
        return createRecordWriter(writer, channelSelector, -1, taskName);
    }

    void emit(T record) throws IOException, InterruptedException;

    void broadcastEmit(T record) throws IOException, InterruptedException;

    void randomEmit(T record) throws IOException, InterruptedException;

    void broadcastEvent(AbstractEvent event) throws IOException;

    void flushAll();

    void clearBuffers();

    void setMetricGroup(TaskIOMetricGroup metrics);

    void close();
}
