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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Resulting {@link DataSet} of a delta iteration operation.
 * @param <ST>
 * @param <WT>
 */
@Public
public class DeltaIterationResultSet<ST, WT> extends DataSet<ST> {

	private final DeltaIteration<ST, WT> iterationHead;

	private final DataSet<ST> nextSolutionSet;

	private final DataSet<WT> nextWorkset;

	private final DataSet<ST> datalogMerge;

	private final Keys<ST> keys;

	private final int maxIterations;

	private final TypeInformation<WT> typeWS;

	DeltaIterationResultSet(ExecutionEnvironment context,
							TypeInformation<ST> typeSS,
							TypeInformation<WT> typeWS,
							DeltaIteration<ST, WT> iterationHead,
							DataSet<ST> nextSolutionSet,
							DataSet<WT> nextWorkset,
							Keys<ST> keys,
							int maxIterations) {
		super(context, typeSS);
		this.iterationHead = iterationHead;

		this.nextWorkset = nextWorkset;
		this.nextSolutionSet = nextSolutionSet;
		this.datalogMerge = null;

		this.keys = keys;
		this.maxIterations = maxIterations;
		this.typeWS = typeWS;
	}

	DeltaIterationResultSet(ExecutionEnvironment context,
							TypeInformation<ST> typeSS,
							TypeInformation<WT> typeWS,
							DeltaIteration<ST, WT> iterationHead,
							DataSet<ST> toMerge,
							Keys<ST> keys,
							int maxIterations) {
		super(context, typeSS);

		if (!typeSS.equals(typeWS))
			throw new RuntimeException("In case of Datalog programs, the type of the initial workset and the solution set should be the same");

		this.iterationHead = iterationHead;

		this.nextWorkset = null;
		this.nextSolutionSet = null;
		this.datalogMerge = toMerge;

		this.keys = keys;
		this.maxIterations = maxIterations;
		this.typeWS = typeWS;
	}

	public DeltaIteration<ST, WT> getIterationHead() {
		return iterationHead;
	}

	public DataSet<ST> getNextSolutionSet() {
		return nextSolutionSet;
	}

	public DataSet<WT> getNextWorkset() {
		return nextWorkset;
	}

	public DataSet<ST> getDatalogMerge() {
		return datalogMerge;
	}

	@Internal
	public int[] getKeyPositions() {
		return keys.computeLogicalKeyPositions();
	}

	@Internal
	public int getMaxIterations() {
		return maxIterations;
	}

	public TypeInformation<WT> getWorksetType() {
		return typeWS;
	}
}
