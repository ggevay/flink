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

package org.apache.flink.runtime.operators.hash;

import com.google.common.collect.Ordering;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.util.CopyingListCollector;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.testutils.UniformStringPairGenerator;
import org.apache.flink.runtime.operators.testutils.types.*;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

public class ReduceHashTableTest {

	private static final long RANDOM_SEED = 58723953465322L;

	private static final int PAGE_SIZE = 16 * 1024;

	private final TypeSerializer<Tuple2<Long, String>> serializer;
	private final TypeComparator<Tuple2<Long, String>> comparator;

	private final TypeComparator<Long> probeComparator;

	private final TypePairComparator<Long, Tuple2<Long, String>> pairComparator;

	//
	// ------------------ Note: This part was mostly copied from CompactingHashTableTest ------------------

	public ReduceHashTableTest() {
		TypeSerializer<?>[] fieldSerializers = { LongSerializer.INSTANCE, StringSerializer.INSTANCE };
		@SuppressWarnings("unchecked")
		Class<Tuple2<Long, String>> clazz = (Class<Tuple2<Long, String>>) (Class<?>) Tuple2.class;
		this.serializer = new TupleSerializer<Tuple2<Long, String>>(clazz, fieldSerializers);

		TypeComparator<?>[] comparators = { new LongComparator(true) };
		TypeSerializer<?>[] comparatorSerializers = { LongSerializer.INSTANCE };

		this.comparator = new TupleComparator<Tuple2<Long, String>>(new int[] {0}, comparators, comparatorSerializers);

		this.probeComparator = new LongComparator(true);

		this.pairComparator = new TypePairComparator<Long, Tuple2<Long, String>>() {

			private long ref;

			@Override
			public void setReference(Long reference) {
				ref = reference;
			}

			@Override
			public boolean equalToReference(Tuple2<Long, String> candidate) {
				//noinspection UnnecessaryUnboxing
				return candidate.f0.longValue() == ref;
			}

			@Override
			public int compareToReference(Tuple2<Long, String> candidate) {
				long x = ref;
				long y = candidate.f0;
				return (x < y) ? -1 : ((x == y) ? 0 : 1);
			}
		};
	}

	@Test
	public void testHashTableGrowthWithInsert() {
		try {
			final int numElements = 1000000;

			List<MemorySegment> memory = getMemory(10000, 32 * 1024);

			// we create a hash table that thinks the records are super large. that makes it choose initially
			// a lot of memory for the partition buffers, and start with a smaller hash table. that way
			// we trigger a hash table growth early.
			ReduceHashTable<Tuple2<Long, String>> table = new ReduceHashTable<Tuple2<Long, String>>(
				serializer, comparator, memory, null, null, false);
			table.open();

			for (long i = 0; i < numElements; i++) {
				table.insert(new Tuple2<Long, String>(i, String.valueOf(i)));
			}

			// make sure that all elements are contained via the entry iterator
			{
				BitSet bitSet = new BitSet(numElements);
				MutableObjectIterator<Tuple2<Long, String>> iter = table.getEntryIterator();
				Tuple2<Long, String> next;
				while ((next = iter.next()) != null) {
					assertNotNull(next.f0);
					assertNotNull(next.f1);
					assertEquals(next.f0.longValue(), Long.parseLong(next.f1));

					bitSet.set(next.f0.intValue());
				}

				assertEquals(numElements, bitSet.cardinality());
			}

			// make sure all entries are contained via the prober
			{
				ReduceHashTable<Tuple2<Long, String>>.HashTableProber<Long> proper =
					table.getProber(probeComparator, pairComparator);

				Tuple2<Long, String> reuse = new Tuple2<>();

				for (long i = 0; i < numElements; i++) {
					assertNotNull(proper.getMatchFor(i, reuse));
					assertNull(proper.getMatchFor(i + numElements, reuse));
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * This test validates that records are not lost via "insertOrReplace()" as in bug [FLINK-2361]
	 */
	@Test
	public void testHashTableGrowthWithInsertOrReplace() {
		try {
			final int numElements = 1000000;

			List<MemorySegment> memory = getMemory(1000, 32 * 1024);

			// we create a hash table that thinks the records are super large. that makes it choose initially
			// a lot of memory for the partition buffers, and start with a smaller hash table. that way
			// we trigger a hash table growth early.
			ReduceHashTable<Tuple2<Long, String>> table = new ReduceHashTable<Tuple2<Long, String>>(
				serializer, comparator, memory, null, null, false);
			table.open();

			for (long i = 0; i < numElements; i++) {
				table.insertOrReplaceRecord(new Tuple2<Long, String>(i, String.valueOf(i)));
			}

			// make sure that all elements are contained via the entry iterator
			{
				BitSet bitSet = new BitSet(numElements);
				MutableObjectIterator<Tuple2<Long, String>> iter = table.getEntryIterator();
				Tuple2<Long, String> next;
				while ((next = iter.next()) != null) {
					assertNotNull(next.f0);
					assertNotNull(next.f1);
					assertEquals(next.f0.longValue(), Long.parseLong(next.f1));

					bitSet.set(next.f0.intValue());
				}

				assertEquals(numElements, bitSet.cardinality());
			}

			// make sure all entries are contained via the prober
			{
				ReduceHashTable<Tuple2<Long, String>>.HashTableProber<Long> proper =
					table.getProber(probeComparator, pairComparator);

				Tuple2<Long, String> reuse = new Tuple2<>();

				for (long i = 0; i < numElements; i++) {
					assertNotNull(proper.getMatchFor(i, reuse));
					assertNull(proper.getMatchFor(i + numElements, reuse));
				}
			}

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * This test validates that new inserts (rather than updates) in "insertOrReplace()" properly
	 * react to out of memory conditions.
	 *
	 * Btw. this also tests the situation, when records are larger than one memory segment.
	 */
	@Test
	public void testInsertsWithInsertOrReplace() {
		try {
			final int numElements = 1000;

			final String longString = getLongString(100000);
			List<MemorySegment> memory = getMemory(7000, 32 * 1024);

			// we create a hash table that thinks the records are super large. that makes it choose initially
			// a lot of memory for the partition buffers, and start with a smaller hash table. that way
			// we trigger a hash table growth early.
			ReduceHashTable<Tuple2<Long, String>> table = new ReduceHashTable<Tuple2<Long, String>>(
				serializer, comparator, memory, null, null, false);
			table.open();

			// first, we insert some elements
			for (long i = 0; i < numElements; i++) {
				table.insertOrReplaceRecord(new Tuple2<Long, String>(i, longString));
			}

			// now, we replace the same elements, causing fragmentation
			for (long i = 0; i < numElements; i++) {
				table.insertOrReplaceRecord(new Tuple2<Long, String>(i, longString));
			}

			// now we insert an additional set of elements. without compaction during this insertion,
			// the memory will run out
			for (long i = 0; i < numElements; i++) {
				table.insertOrReplaceRecord(new Tuple2<Long, String>(i + numElements, longString));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static String getLongString(int length) {
		StringBuilder bld = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			bld.append('a');
		}
		return bld.toString();
	}


	// ------------------ The following are the ReduceHashTable-specific tests ------------------

	private class ReduceHashTableWithJavaHashMap<T, K> {

		TypeSerializer<T> serializer;

		TypeComparator<T> comparator;

		ReduceFunction<T> reducer;

		Collector<T> outputCollector;

		HashMap<K, T> map = new HashMap<>();

		public ReduceHashTableWithJavaHashMap(TypeSerializer<T> serializer, TypeComparator<T> comparator, ReduceFunction<T> reducer, Collector<T> outputCollector) {
			this.serializer = serializer;
			this.comparator = comparator;
			this.reducer = reducer;
			this.outputCollector = outputCollector;
		}

		public void processRecordWithReduce(T record, K key) throws Exception {
			record = serializer.copy(record);

			if (!map.containsKey(key)) {
				map.put(key, record);
			} else {
				T x = map.get(key);
				x = reducer.reduce(x, record);
				map.put(key, x);
			}
		}

		public void emitAndReset() {
			for (T record: map.values()) {
				outputCollector.collect(record);
			}
			map.clear();
		}
	}

	@Test
	public void testWithIntPair() throws Exception {
		Random rnd = new Random(RANDOM_SEED);

		// varying the keyRange between 1000 and 1000000 can make a 5x speed difference
		// (because of cache misses (also in the segment arrays))
		final int keyRange = 1000000; //////////////////////////////1000000
		final int valueRange = 10;
		final int numRecords = 60000000; //////////////////////////////1000000

		final IntPairSerializer serializer = new IntPairSerializer();
		final TypeComparator<IntPair> comparator = new IntPairComparator();
		final ReduceFunction<IntPair> reducer = new SumReducer();

		// Create the ReduceHashTableWithJavaHashMap, which will provide the correct output.
		List<IntPair> expectedOutput = new ArrayList<>();
		ReduceHashTableWithJavaHashMap<IntPair, Integer> reference = new ReduceHashTableWithJavaHashMap<>(
			serializer, comparator, reducer, new CopyingListCollector<>(expectedOutput, serializer));

		// Create the ReduceHashTable to test
		final int numMemPages = keyRange * 32 / PAGE_SIZE; // memory use is proportional to the number of different keys
		List<IntPair> actualOutput = new ArrayList<>();

		//ReduceHashTable<IntPair> table = new ReduceHashTable<>( ////////////////////////////////////////////
		OpenAddressingHashTable<IntPair> table = new OpenAddressingHashTable<>( ////////////////////////////////////////////
			serializer, comparator, getMemory(numMemPages, PAGE_SIZE), reducer,
			new CopyingListCollector<>(actualOutput, serializer), true);
		table.open();

		// Generate some input
		final int[] inputKeys = new int[numRecords];
		final int[] inputVals = new int[numRecords];
		for(int i = 0; i < numRecords; i++) {
			inputKeys[i] = rnd.nextInt(keyRange);
			inputVals[i] = rnd.nextInt(valueRange);
		}

		System.out.println("start");
		long start = System.currentTimeMillis();

		// Process the generated input
		final int numIntermingledEmits = 5;
		final IntPair record = new IntPair();
		for (int i = 0; i < numRecords; i++) {
			record.setKey(inputKeys[i]);
			record.setValue(inputVals[i]);

			table.processRecordWithReduce(serializer.copy(record));
//			reference.processRecordWithReduce(serializer.copy(record), record.getKey());
//			if(rnd.nextDouble() < 1.0 / ((double)numRecords / numIntermingledEmits)) {
//				// this will fire approx. numIntermingledEmits times
//				reference.emitAndReset();
//				table.emitAndReset();
//			}
		}
		reference.emitAndReset();
		table.emit();
		table.close();

		long end = System.currentTimeMillis();
		System.out.println("stop, time: " + (end - start));

		// Check results

		assertEquals(expectedOutput.size(), actualOutput.size());

		Integer[] expectedValues = new Integer[expectedOutput.size()];
		for (int i = 0; i < expectedOutput.size(); i++) {
			expectedValues[i] = expectedOutput.get(i).getValue();
		}
		Integer[] actualValues = new Integer[actualOutput.size()];
		for (int i = 0; i < actualOutput.size(); i++) {
			actualValues[i] = actualOutput.get(i).getValue();
		}

		Arrays.sort(expectedValues, Ordering.<Integer>natural());
		Arrays.sort(actualValues, Ordering.<Integer>natural());
		assertArrayEquals(expectedValues, actualValues);
	}

	class SumReducer implements ReduceFunction<IntPair> {
		@Override
		public IntPair reduce(IntPair a, IntPair b) throws Exception {
			if (a.getKey() != b.getKey()) {
				throw new RuntimeException("SumReducer was called with two records that have differing keys.");
			}
			a.setValue(a.getValue() + b.getValue());
			return a;
		}
	}


	@Test
	public void testWithLengthChangingReduceFunction() throws Exception {
		Random rnd = new Random(RANDOM_SEED);

		final int numKeys = 10000;
		final int numVals = 10;
		final int numRecords = numKeys * numVals;

		StringPairSerializer serializer = new StringPairSerializer();
		StringPairComparator comparator = new StringPairComparator();
		ReduceFunction<StringPair> reducer = new ConcatReducer();

		// Create the ReduceHashTableWithJavaHashMap, which will provide the correct output.
		List<StringPair> expectedOutput = new ArrayList<>();
		ReduceHashTableWithJavaHashMap<StringPair, String> reference = new ReduceHashTableWithJavaHashMap<>(
			serializer, comparator, reducer, new CopyingListCollector<>(expectedOutput, serializer));

		// Create the ReduceHashTable to test
		final int numMemPages = numRecords * 10 / PAGE_SIZE;

		List<StringPair> actualOutput = new ArrayList<>();

		ReduceHashTable<StringPair> table = new ReduceHashTable<>(
			serializer, comparator, getMemory(numMemPages, PAGE_SIZE), reducer, new CopyingListCollector<>(actualOutput, serializer), true);

		// The loop is for checking the feature that multiple open / close are possible.
		for(int j = 0; j < 3; j++) {
			table.open();

			// Test emit when table is empty
			table.emit();

			// Process some manual stuff
			reference.processRecordWithReduce(serializer.copy(new StringPair("foo", "bar")), "foo");
			reference.processRecordWithReduce(serializer.copy(new StringPair("foo", "baz")), "foo");
			reference.processRecordWithReduce(serializer.copy(new StringPair("alma", "xyz")), "alma");
			table.processRecordWithReduce(serializer.copy(new StringPair("foo", "bar")));
			table.processRecordWithReduce(serializer.copy(new StringPair("foo", "baz")));
			table.processRecordWithReduce(serializer.copy(new StringPair("alma", "xyz")));
			for (int i = 0; i < 5; i++) {
				table.processRecordWithReduce(serializer.copy(new StringPair("korte", "abc")));
				reference.processRecordWithReduce(serializer.copy(new StringPair("korte", "abc")), "korte");
			}
			reference.emitAndReset();
			table.emitAndReset();

			// Generate some input
			UniformStringPairGenerator gen = new UniformStringPairGenerator(numKeys, numVals, true);
			List<StringPair> input = new ArrayList<>();
			StringPair cur = new StringPair();
			while (gen.next(cur) != null) {
				input.add(serializer.copy(cur));
			}
			Collections.shuffle(input, rnd);

			// Process the generated input
			final int numIntermingledEmits = 5;
			for (StringPair record : input) {
				reference.processRecordWithReduce(serializer.copy(record), record.getKey());
				table.processRecordWithReduce(serializer.copy(record));
				if (rnd.nextDouble() < 1.0 / ((double) numRecords / numIntermingledEmits)) {
					// this will fire approx. numIntermingledEmits times
					reference.emitAndReset();
					table.emitAndReset();
				}
			}
			reference.emitAndReset();
			table.emit();
			table.close();

			// Check results

			assertEquals(expectedOutput.size(), actualOutput.size());

			String[] expectedValues = new String[expectedOutput.size()];
			for (int i = 0; i < expectedOutput.size(); i++) {
				expectedValues[i] = expectedOutput.get(i).getValue();
			}
			String[] actualValues = new String[actualOutput.size()];
			for (int i = 0; i < actualOutput.size(); i++) {
				actualValues[i] = actualOutput.get(i).getValue();
			}

			Arrays.sort(expectedValues, Ordering.<String>natural());
			Arrays.sort(actualValues, Ordering.<String>natural());
			assertArrayEquals(expectedValues, actualValues);

			expectedOutput.clear();
			actualOutput.clear();
		}
	}

	// Warning: Generally, reduce wouldn't give deterministic results with non-commutative ReduceFunction,
	// but ReduceHashTable and ReduceHashTableWithJavaHashMap calls it in the same order.
	class ConcatReducer implements ReduceFunction<StringPair> {
		@Override
		public StringPair reduce(StringPair a, StringPair b) throws Exception {
			if (a.getKey().compareTo(b.getKey()) != 0) {
				throw new RuntimeException("ConcatReducer was called with two records that have differing keys.");
			}
			return new StringPair(a.getKey(), a.getValue().concat(b.getValue()));
		}
	}


	private static List<MemorySegment> getMemory(int numPages, int pageSize) {
		List<MemorySegment> memory = new ArrayList<>();

		for (int i = 0; i < numPages; i++) {
			memory.add(MemorySegmentFactory.allocateUnpooledSegment(pageSize));
		}

		return memory;
	}
}
