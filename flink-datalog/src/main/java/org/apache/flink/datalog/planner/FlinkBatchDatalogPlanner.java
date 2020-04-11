/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.datalog.planner;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.datalog.parser.tree.Node;
import org.apache.flink.datalog.plan.logical.LogicalPlan;
import org.apache.flink.datalog.planner.calcite.FlinkDatalogPlannerImpl;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.PlannerExpressionConverter;
import org.apache.flink.table.expressions.PlannerTypeInferenceUtilImpl;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistributionTraitDef;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

/**
 * This class is used to return Datalog parser via {@link #getParser()} and transform a Datalog query
 * into a Table API specific objects
 * e.g. tree of {@link Operation}s</li>.
 */
public class FlinkBatchDatalogPlanner implements Planner {
	private Executor executor;
	private TableConfig tableConfig;
	private FunctionCatalog functionCatalog;
	private CatalogManager catalogManager;
	private ExpressionBridge<PlannerExpression> expressionBridge;
	//	private DatalogPlanningConfigurationBuilder planningConfigurationBuilder;
	private CalciteSchema internalSchema;
	//	private DatalogPlannerContext plannerContext;
	private ObjectIdentifier objectIdentifier;

	public FlinkBatchDatalogPlanner(Executor executor, TableConfig tableConfig, FunctionCatalog functionCatalog, CatalogManager catalogManager, boolean isStreamingMode) {
		this.executor = executor;
		this.tableConfig = tableConfig;
		this.functionCatalog = functionCatalog;
		this.catalogManager = catalogManager;
		functionCatalog.setPlannerTypeInferenceUtil(PlannerTypeInferenceUtilImpl.INSTANCE);
		internalSchema = asRootSchema(new CatalogManagerCalciteSchema(catalogManager, tableConfig,false));
		ExpressionBridge<PlannerExpression> expressionBridge = new ExpressionBridge<PlannerExpression>(PlannerExpressionConverter.INSTANCE());
//		planningConfigurationBuilder =
//			new DatalogPlanningConfigurationBuilder(
//				tableConfig,
//				functionCatalog,
//				internalSchema,
//				expressionBridge);
		List<RelTraitDef> traits = new ArrayList<>();
		traits.add(ConventionTraitDef.INSTANCE);
		traits.add(FlinkRelDistributionTraitDef.INSTANCE());
		traits.add(RelCollationTraitDef.INSTANCE);
//		this.plannerContext = new DatalogPlannerContext(tableConfig, functionCatalog, this.internalSchema, traits.toArray(new RelTraitDef[0]));
//		((DatalogBatchExecutor)this.executor).getExecutionEnvironment();
	}

	private FlinkDatalogPlannerImpl createFlinkDatalogPlanner() {
		String currentCatalogName = catalogManager.getCurrentCatalog();
		String currentDatabase = catalogManager.getCurrentDatabase();
//		return plannerContext.createFlinkDatalogPlanner(currentCatalogName, currentDatabase);
		return null;
	}

	private FlinkRelBuilder getFlinkRelBuilder() {
		//todo:
		return null;
	}

	public List<Operation> parse(String text) {
		FlinkDatalogPlannerImpl planner = createFlinkDatalogPlanner();
		Node andOrTreeRootNode = planner.parse(text, text);

		LogicalPlan plan = new LogicalPlan(getFlinkRelBuilder(), this.catalogManager);
		plan.visit(andOrTreeRootNode);
		RelRoot relRoot = RelRoot.of(plan.getLogicalPlan(), SqlKind.SELECT);
		return Arrays.asList(new Operation[]{new PlannerQueryOperation(relRoot.project())});
//		return List.of(new PlannerQueryOperation(relRoot.project()));
	}

	@Override
	public Parser getParser() {
		return null;
	}

	@Override
	public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
		return modifyOperations
			.stream()
			.map(this::translate)
			.filter(Objects::nonNull).collect(Collectors.toList());
	}

	@Override
	public String explain(List<Operation> operations, boolean extended) {
		return null;
	}

	@Override
	public String[] getCompletionHints(String statement, int position) {
		throw new UnsupportedOperationException("Please learn Datalog on your own!");
	}

	//todo:...
	private Transformation<?> translate(ModifyOperation modifyOperation) {
		return null;
	}
}
