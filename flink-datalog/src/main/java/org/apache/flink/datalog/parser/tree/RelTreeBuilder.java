package org.apache.flink.datalog.parser.tree;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.datalog.BatchDatalogEnvironmentImpl;
import org.apache.flink.datalog.DatalogBaseVisitor;
import org.apache.flink.datalog.DatalogParser;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.calcite.FlinkRelBuilder;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RelTreeBuilder extends DatalogBaseVisitor<RelNode> { //may be we need to use FlinkRelBuilder instead of RelNode
	private FlinkRelBuilder relBuilder;
	private String currentCatalog;
	private String currentDatabase;

	private TableEnvironment environment;

	public RelTreeBuilder(FlinkRelBuilder relBuilder) {
		this.relBuilder = relBuilder;
		this.environment = relBuilder.getCluster().getPlanner().getContext().unwrap(BatchDatalogEnvironmentImpl.class);
		this.currentCatalog = this.environment.getCurrentCatalog();
		this.currentDatabase = this.environment.getCurrentDatabase();
	}

	// DO WE NEED TO IMPLEMENT SEMI NAIVE EVALUATION HERE..
	@Override
	public RelNode visitCompileUnit(DatalogParser.CompileUnitContext ctx) {
		if (ctx.query() != null) {
			return visit(ctx.query());
		} else if (ctx.rules() != null) {
			return visit(ctx.rules());
		} else
			return null;
	}

	@Override
	public RelNode visitRules(DatalogParser.RulesContext ctx) {
		List<RelNode> ruleClauses = new ArrayList<>();
		for (DatalogParser.RuleClauseContext ruleClauseContext : ctx.ruleClause()) {
			// here find a rule without IDB (or do it using predicate connection graph)
			ruleClauses.add(visit(ruleClauseContext));
		}
		return null;
	}

	@Override
	public RelNode visitRuleClause(DatalogParser.RuleClauseContext ctx) {
		RelNode predicates = visit(ctx.predicateList());
		RelNode headPredicate = visit(ctx.headPredicate());
		relBuilder.push(predicates).push(headPredicate).union(true);
		RelNode ruleClauseNode = relBuilder.build();
		System.out.println(RelOptUtil.toString(ruleClauseNode));
		return ruleClauseNode;
	}

	@Override
	public RelNode visitPredicateList(DatalogParser.PredicateListContext ctx) {
		List<RelNode> nodes = new ArrayList<>();
		for (DatalogParser.PredicateContext predicateContext : ctx.predicate()) {
			RelNode predicate = visit(predicateContext);
			nodes.add(predicate);
		}

		if (nodes.size() == 1) {
			relBuilder.pushAll(nodes);
		} else if (nodes.size() > 1) {
			for (int i = 0; i < nodes.size() - 1; i++) {

				List<String> leftPredicateFields = nodes.get(i).getRowType().getFieldNames();
				List<String> rightPredicateFields = nodes.get(i + 1).getRowType().getFieldNames();
				String[] matched = leftPredicateFields.stream()
					.filter(rightPredicateFields::contains).toArray(String[]::new);
				relBuilder.push(nodes.get(i)).push(nodes.get(i + 1)).join(JoinRelType.INNER, matched);
			}
		} else return null;
		RelNode predicateListNode = relBuilder.build();
		System.out.println(RelOptUtil.toString(predicateListNode));
		return predicateListNode;
	}

	@Override
	public RelNode visitHeadPredicate(DatalogParser.HeadPredicateContext ctx) {
		return relBuilder.scan(ctx.predicate().predicateName().getText()).project(relBuilder.fields(IntStream.range(0, ctx.predicate().termList().term().size()).boxed().collect(Collectors.toList()))).build();
	}

	@Override
	public RelNode visitQuery(DatalogParser.QueryContext ctx) {
		return visit(ctx.predicate());
	}

	@Override
	public RelNode visitPredicate(DatalogParser.PredicateContext ctx) {
		String predicateName = ctx.predicateName().getText();
		List<RexNode> filters = new ArrayList<>();
		relBuilder.scan(this.currentCatalog, this.currentDatabase, predicateName);
		int i = 0;
		for (DatalogParser.TermContext termContext : ctx.termList().term()) {
			if (termContext.CONSTANT() != null) {
				filters.add(relBuilder.call(SqlStdOperatorTable.EQUALS,
					relBuilder.field(i),
					relBuilder.literal(termContext.CONSTANT().getText())));
			}
			i++;
		}
		if (filters.size() == 1)
			relBuilder.filter(filters.get(0));
		else if (filters.size() > 1)
			relBuilder.filter(relBuilder.call(SqlStdOperatorTable.AND, filters));

		relBuilder.project(relBuilder.fields(IntStream.range(0, ctx.termList().term().size()).boxed().collect(Collectors.toList())));
		RelNode queryNode = relBuilder.build();
		System.out.println(RelOptUtil.toString(queryNode));
		return queryNode;
	}

	private void addTableToCatalog(String name, List<String> columns) {
//		this.environment.registerTable(name, new QueryOperationCatalogView(new Table()));
	}
}
