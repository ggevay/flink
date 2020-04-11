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

package org.apache.flink.datalog;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.datalog.parser.tree.Node;
import org.apache.flink.datalog.plan.logical.LogicalPlan;
import org.apache.flink.datalog.planner.DatalogPlanningConfigurationBuilder;
import org.apache.flink.datalog.planner.calcite.FlinkDatalogPlannerImpl;
import org.apache.flink.table.api.BatchQueryConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.api.java.internal.BatchTableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.descriptors.BatchTableDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.PlannerExpressionConverter;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.DataSetQueryOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.PlannerQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.typeutils.FieldInfoUtils;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import scala.Option;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

/**
 *
 */
public class BatchDatalogEnvironmentImpl extends BatchTableEnvironmentImpl implements BatchDatalogEnvironment {
    //    private final OperationTreeBuilder operationTreeBuilder;
    private final FunctionCatalog functionCatalog;
    private final List<ModifyOperation> bufferedModifyOperations = new ArrayList<>();
    private CatalogManager catalogManager;
    private Executor executor;
    private TableConfig tableConfig;
    private ExecutionEnvironment executionEnvironment;
    private DatalogPlanningConfigurationBuilder planningConfigurationBuilder;

    public BatchDatalogEnvironmentImpl(CatalogManager catalogManager, TableConfig tableConfig, Executor executor, FunctionCatalog functionCatalog, ExecutionEnvironment executionEnvironment) {
        super(executionEnvironment, tableConfig, catalogManager, new ModuleManager());
        this.executionEnvironment = executionEnvironment;
        this.executor = executor;
        this.tableConfig = tableConfig;
        this.functionCatalog = functionCatalog;
        this.catalogManager = catalogManager;
//        this.operationTreeBuilder = OperationTreeBuilder.create(
//                functionCatalog,
//                path -> {
//                    Optional<CatalogQueryOperation> catalogTableOperation = Optional.ofNullable(scanInternal(path).getOrElse(null));
//                    return catalogTableOperation.map(tableOperation -> new TableReferenceExpression(path, tableOperation));
//                },
//                false
//        );

        ExpressionBridge<PlannerExpression> expressionBridge = new ExpressionBridge<>(PlannerExpressionConverter.INSTANCE());
        this.planningConfigurationBuilder = new DatalogPlanningConfigurationBuilder(
                tableConfig,
                functionCatalog,
                asRootSchema(new CatalogManagerCalciteSchema(catalogManager, tableConfig,false)),
                expressionBridge,
                this);
    }

    /**
     * @param executionEnvironment
     * @param settings
     * @param tableConfig
     * @return
     */
    public static BatchDatalogEnvironment create(ExecutionEnvironment executionEnvironment, EnvironmentSettings settings, TableConfig tableConfig) {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ModuleManager moduleManager = new ModuleManager();

        CatalogManager catalogManager = CatalogManager.newBuilder()
                .classLoader(classLoader)
                .config(tableConfig.getConfiguration())
                .defaultCatalog(
                        settings.getBuiltInCatalogName(),
                        new GenericInMemoryCatalog(
                                settings.getBuiltInCatalogName(),
                                settings.getBuiltInDatabaseName()))
                .build();

        FunctionCatalog functionCatalog = new FunctionCatalog(tableConfig, catalogManager, moduleManager);
        Map<String, String> executorProperties = settings.toExecutorProperties();
        Executor executor = ComponentFactoryService.find(ExecutorFactory.class, executorProperties)
                .create(executorProperties);

        return new BatchDatalogEnvironmentImpl(
                catalogManager,
                tableConfig,
                executor,
                functionCatalog,
                executionEnvironment
        );
    }

    /**
     * @param inputProgram
     * @param query
     * @return
     */
    @Override
    public Table datalogQuery(String inputProgram, String query) {
        FlinkDatalogPlannerImpl datalogPlanner = getFlinkPlanner();
        Node andOrTreeNode = datalogPlanner.parse(inputProgram, query); //node of And-Or Tree

        //todo: update catalog here, because the updated catalog will be needed in creating logical algebra (if we use scan() but may be it is not needed in transientScan()).
        LogicalPlan plan = new LogicalPlan(this.getRelBuilder(), this.catalogManager);
        plan.visit(andOrTreeNode);
        RelNode relataionalAlgebra = plan.getLogicalPlan();

        if (null != relataionalAlgebra) {
            return createTable(new PlannerQueryOperation(relataionalAlgebra));
        } else {
            throw new TableException(
                    "Unsupported Datalog query!");
        }
    }

    /**
     * @param name          The name under which the function is registered.
     * @param tableFunction The TableFunction to register.
     * @param <T>
     */
    @Override
    public <T> void registerFunction(String name, TableFunction<T> tableFunction) {
        throw new UnsupportedOperationException("Not supported."); //there are no functions in datalog
    }

    @Override
    public <T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction) {
        throw new UnsupportedOperationException("Not supported"); //there are no functions in datalog
    }

    @Override
    public <T> Table fromDataSet(DataSet<T> dataSet) {
        return this.createTable(asQueryOperation(dataSet, Optional.ofNullable(null)));
    }

    @Override
    public <T> Table fromDataSet(DataSet<T> dataSet, String fields) {
        List<Expression> exprs = ExpressionParser
                .parseExpressionList(fields);

        return createTable(asQueryOperation(dataSet, Optional.of(exprs)));
    }

    @Override
    public <T> void registerDataSet(String name, DataSet<T> dataSet) {
        registerTable(name, fromDataSet(dataSet));
    }

    @Override
    public <T> void createTemporaryView(String path, DataSet<T> dataSet) {

    }

    @Override
    public <T> void registerDataSet(String name, DataSet<T> dataSet, String fields) {
        registerTable(name, fromDataSet(dataSet, fields));
    }

    @Override
    public <T> void createTemporaryView(String path, DataSet<T> dataSet, String fields) {

    }

    @Override
    public <T> DataSet<T> toDataSet(Table table, Class<T> clazz) {
        return translate(table, TypeExtractor.createTypeInfo(clazz));
    }

    @Override
    public <T> DataSet<T> toDataSet(Table table, TypeInformation<T> typeInfo) {
        return translate(table, typeInfo);
    }

    @Override
    public <T> DataSet<T> toDataSet(Table table, Class<T> clazz, BatchQueryConfig queryConfig) {
        return translate(table, TypeExtractor.createTypeInfo(clazz));
    }

    @Override
    public <T> DataSet<T> toDataSet(Table table, TypeInformation<T> typeInfo, BatchQueryConfig queryConfig) {
        return translate(table, typeInfo);
    }

    @Override
    public void sqlUpdate(String stmt, BatchQueryConfig config) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void insertInto(Table table, BatchQueryConfig queryConfig, String sinkPath, String... sinkPathContinued) {
        throw new UnsupportedOperationException("Not implemented yet. Will implement later.");
    }

    @Override
    public Table fromTableSource(TableSource<?> source) {
        return createTable(new TableSourceQueryOperation<>(source, true));
    }

    @Override
    public void registerCatalog(String catalogName, Catalog catalog) {
        catalogManager.registerCatalog(catalogName, catalog);
    }

    @Override
    public Optional<Catalog> getCatalog(String catalogName) {
        return catalogManager.getCatalog(catalogName);
    }

    @Override
    public void loadModule(String moduleName, Module module) {

    }

    @Override
    public void unloadModule(String moduleName) {

    }

    @Override
    public void registerFunction(String name, ScalarFunction function) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void registerTable(String name, Table table) {
        if (((TableImpl) table).getTableEnvironment() != this) {
            throw new TableException(
                    "Only tables that belong to this TableEnvironment can be registered.");
        }
        CatalogBaseTable view = new QueryOperationCatalogView(table.getQueryOperation());
        catalogManager.createTable(view, catalogManager.qualifyIdentifier(UnresolvedIdentifier.of(catalogManager.getBuiltInCatalogName(),
                catalogManager.getBuiltInDatabaseName(),
                name)
        ), false);
    }

    @Override
    public void registerTableSource(String name, TableSource<?> tableSource) {
        // the implementation of this function is similar to the one in TableEnvImpl, since its purpose is same.
        TableSourceValidation.validateTableSource(tableSource, tableSource.getTableSchema());

        if (!(tableSource instanceof BatchTableSource<?> || tableSource instanceof InputFormatTableSource<?>)) {
            throw new TableException("Only BatchTableSource or InputFormatTableSource are allowed here.");
        }
        //---------------------------------------------------------------------
        Optional<CatalogManager.TableLookupResult> table =
                catalogManager.getTable(catalogManager.qualifyIdentifier(UnresolvedIdentifier.of(catalogManager.getBuiltInCatalogName(),
                        catalogManager.getBuiltInDatabaseName(),
                        name)));
        if (table.isPresent()) {
            if (table.get().getTable() instanceof ConnectorCatalogTable<?, ?>) {
                ConnectorCatalogTable<?, ?> sourceSinkTable = (ConnectorCatalogTable<?, ?>) table.get().getTable();
                if (sourceSinkTable.getTableSource().isPresent()) {
                    throw new ValidationException(String.format("Table '%s' already exists. Please choose a different name.", name));
                } else {
                    ConnectorCatalogTable sourceAndSink = ConnectorCatalogTable.sourceAndSink(
                            tableSource,
                            sourceSinkTable.getTableSink().get(),
                            true);
                    catalogManager.alterTable(sourceAndSink, catalogManager.qualifyIdentifier(UnresolvedIdentifier.of(catalogManager.getBuiltInCatalogName(),
                            catalogManager.getBuiltInDatabaseName(),
                            name)), false);
                }
            } else {
                throw new ValidationException(String.format(
                        "Table '%s' already exists. Please choose a different name.", name));
            }
        } else {
            ConnectorCatalogTable source = ConnectorCatalogTable.source(tableSource, true);
            catalogManager.createTable(source, catalogManager.qualifyIdentifier(
                    UnresolvedIdentifier.of(catalogManager.getBuiltInCatalogName(),
                            catalogManager.getBuiltInDatabaseName(),
                            name)), false);
        }
    }

    @Override
    public void registerTableSink(String name, String[] fieldNames, TypeInformation<?>[] fieldTypes, TableSink<?> tableSink) {
        registerTableSink(name, tableSink.configure(fieldNames, fieldTypes));
    }

    @Override
    public void registerTableSink(String name, TableSink<?> configuredSink) {
        if (configuredSink.getTableSchema().getFieldCount() == 0) {
            throw new TableException("Table schema cannot be empty.");
        }
        registerTableSinkInternal(name, configuredSink);
    }

    @Override
    public Table scan(String... tablePath) {
        return Optional.ofNullable(scanInternal(tablePath)).map(x -> createTable(x.get()))
                .orElseThrow(() -> new ValidationException(String.format(
                        "Table '%s' was not found.",
                        String.join(".", tablePath))));
    }

    @Override
    public void insertInto(Table table, String sinkPath, String... sinkPathContinued) {
        throw new UnsupportedOperationException("Not supported yet. It may be implemented later.");
    }

    @Override
    public BatchTableDescriptor connect(ConnectorDescriptor connectorDescriptor) {
        return null;
    }

    @Override
    public String[] listCatalogs() {
        return this.catalogManager.listCatalogs().toArray(new String[0]);
    }

    @Override
    public String[] listModules() {
        return new String[0];
    }

    @Override
    public String[] listDatabases() {
        List<String> databases = new ArrayList<>();
        for (String c : this.catalogManager.listCatalogs()) {
            boolean isCatalogPresent = this.catalogManager.getCatalog(c).isPresent();
            if (isCatalogPresent) {
                databases.addAll(this.catalogManager.getCatalog(c).get().listDatabases());
            }
        }
        return databases.toArray(new String[0]);
    }

    @Override
    public String[] listTables() {
        List<String> tables = new ArrayList<>();
        for (String catalog : this.catalogManager.listCatalogs()) {
            boolean isCatalogPresent = this.catalogManager.getCatalog(catalog).isPresent();
            if (isCatalogPresent) {
                for (String database : this.catalogManager.getCatalog(catalog).get().listDatabases()) {
                    try {
                        tables.addAll(this.catalogManager.getCatalog(catalog).get().listTables(database));
                    } catch (DatabaseNotExistException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return tables.toArray(new String[0]);
    }

    @Override
    public String[] listUserDefinedFunctions() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String[] listFunctions() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String explain(Table table) {
        return explain(table, false);
    }

    @Override
    public Table sqlQuery(String query) {
        throw new UnsupportedOperationException("This implementation is only for Datalog queries, not SQL queries. Use Table API for SQL queries.");
    }

    @Override
    public void sqlUpdate(String stmt) {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public String getCurrentCatalog() {
        return this.catalogManager.getCurrentCatalog();
    }

    @Override
    public void useCatalog(String catalogName) {
        this.catalogManager.setCurrentCatalog(catalogName);
    }

    @Override
    public String getCurrentDatabase() {
        return this.catalogManager.getCurrentDatabase();
    }

    @Override
    public void useDatabase(String databaseName) {
        this.catalogManager.setCurrentDatabase(databaseName);
    }

    @Override
    public TableConfig getConfig() {
        return this.tableConfig;
    }

    @Override
    public JobExecutionResult execute(String jobName) {
        //translate(bufferedModifyOperations);
        bufferedModifyOperations.clear();
        try {
            return this.executor.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private ObjectIdentifier getTemporaryObjectIdentifier(String name) {
        return catalogManager.qualifyIdentifier(
                UnresolvedIdentifier.of(catalogManager.getBuiltInCatalogName(),
                        catalogManager.getBuiltInDatabaseName(),
                        name)
        );
    }

    private void registerTableSinkInternal(String name, TableSink<?> tableSink) {
        // for now, similar to the one of TableEnvironmentImpl
        Optional<CatalogManager.TableLookupResult> table = getCatalogTable(
                catalogManager.getBuiltInCatalogName(),
                catalogManager.getBuiltInDatabaseName(),
                name);

        if (table.isPresent()) {
            if (table.get().getTable() instanceof ConnectorCatalogTable<?, ?>) {
                ConnectorCatalogTable<?, ?> sourceSinkTable = (ConnectorCatalogTable<?, ?>) table.get().getTable();
                if (sourceSinkTable.getTableSink().isPresent()) {
                    throw new ValidationException(String.format(
                            "Table '%s' already exists. Please choose a different name.", name));
                } else {
                    ConnectorCatalogTable sourceAndSink = ConnectorCatalogTable
                            .sourceAndSink(sourceSinkTable.getTableSource().get(), tableSink, true);
                    catalogManager.alterTable(sourceAndSink, getTemporaryObjectIdentifier(name), false);
                }
            } else {
                throw new ValidationException(String.format(
                        "Table '%s' already exists. Please choose a different name.", name));
            }
        } else {
            ConnectorCatalogTable sink = ConnectorCatalogTable.sink(tableSink, true);
            catalogManager.createTable(sink, getTemporaryObjectIdentifier(name), false);
        }
    }

    private Optional<CatalogManager.TableLookupResult> getCatalogTable(String... name) {
        ObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(UnresolvedIdentifier.of(name));
        return catalogManager.getTable(objectIdentifier);
    }

    public TableImpl createTable(QueryOperation tableOperation) {
        return TableImpl.createTable(
                this,
                tableOperation,
                super.operationTreeBuilder(),
                functionCatalog.asLookup(UnresolvedIdentifier::of));
    }

    public void validateTableSource(TableSource<?> tableSource) {
        TableSourceValidation.validateTableSource(tableSource, tableSource.getTableSchema());
    }

    private Option<CatalogQueryOperation> scanInternal(String... tablePath) {
        return super.scanInternal(UnresolvedIdentifier.of(tablePath));
    }

    private <T> DataSetQueryOperation<T> asQueryOperation(DataSet<T> dataSet, Optional<List<Expression>> fields) {
        TypeInformation<T> dataSetType = dataSet.getType();

        FieldInfoUtils.TypeInfoSchema typeInfoSchema = fields.map(f -> FieldInfoUtils.getFieldsInfo(
                dataSetType,
                f.toArray(new Expression[0]))).orElseGet(() -> FieldInfoUtils.getFieldsInfo(dataSetType));
        return new DataSetQueryOperation<T>(
                dataSet,
                typeInfoSchema.getIndices(),
                typeInfoSchema.toTableSchema());
    }

    @Override
    public FlinkDatalogPlannerImpl getFlinkPlanner() {
        String currentCatalogName = catalogManager.getCurrentCatalog();
        String currentDatabase = catalogManager.getCurrentDatabase();
        return planningConfigurationBuilder.createFlinkPlanner(currentCatalogName, currentDatabase);
    }
}


