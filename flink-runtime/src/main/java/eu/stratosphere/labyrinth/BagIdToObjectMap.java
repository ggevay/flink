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

package eu.stratosphere.labyrinth;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public final class BagIdToObjectMap<T> {

    private final Int2ObjectOpenHashMap<Int2ObjectOpenHashMap<T>> map = new Int2ObjectOpenHashMap<>(3000, Hash.VERY_FAST_LOAD_FACTOR);

    T get(BagID bid) {
        Int2ObjectOpenHashMap<T> innerMap = map.get(bid.cflSize);
        if (innerMap == null) {
            return null;
        } else {
            return innerMap.get(bid.opID);
        }
    }

    void put(BagID bid, T v) {
        Int2ObjectOpenHashMap<T> innerMap = map.get(bid.cflSize);
        if (innerMap == null) {
			innerMap = new Int2ObjectOpenHashMap<>(50, Hash.VERY_FAST_LOAD_FACTOR);
            map.put(bid.cflSize, innerMap);
        }
        innerMap.put(bid.opID, v);
    }

    void clear() {
        map.clear();
    }
}
