/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.helper.fieldaccessors;

import org.apache.flink.api.java.tuple.Tuple;

import java.io.Serializable;

public class TupleFieldAccessor<R, F> implements FieldAccessor<R, F>, Serializable {

	private static final long serialVersionUID = 1L;

	int pos;

	TupleFieldAccessor(int pos) {
		this.pos = pos;
	}

	@SuppressWarnings("unchecked")
	@Override
	public F get(R record) {
		Tuple tuple = (Tuple) record;
		return (F)tuple.getField(pos);
	}

	@Override
	public R set(R record, F fieldValue) {
		Tuple tuple = (Tuple) record;
		tuple.setField(fieldValue, pos);
		return record;
	}
}
