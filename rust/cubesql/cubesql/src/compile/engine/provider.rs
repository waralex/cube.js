use std::sync::Arc;

use cubeclient::models::V1CubeMeta;
use datafusion::{
    datasource,
    execution::context::ExecutionContextState,
    physical_plan::{udaf::AggregateUDF, udf::ScalarUDF},
    sql::planner::ContextProvider,
};

use super::information_schema::{
    collations::InfoSchemaCollationsProvider, columns::InfoSchemaColumnsProvider,
    key_column_usage::InfoSchemaKeyColumnUsageProvider,
    referential_constraints::InfoSchemaReferentialConstraintsProvider,
    schemata::InfoSchemaSchemataProvider, statistics::InfoSchemaStatisticsProvider,
    tables::InfoSchemaTableProvider, variables::PerfSchemaVariablesProvider,
};
use crate::transport::{MetaContext, V1CubeMetaExt};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;

#[derive(Clone)]
pub struct CubeContext<'a> {
    /// Internal state for the context (default)
    pub state: &'a ExecutionContextState,
    /// To access cubes
    pub meta: Arc<MetaContext>,
}

impl<'a> CubeContext<'a> {
    pub fn new(state: &'a ExecutionContextState, meta: Arc<MetaContext>) -> Self {
        Self { state, meta }
    }

    pub fn table_name_by_table_provider(
        &self,
        table_provider: Arc<dyn datasource::TableProvider>,
    ) -> Result<String, CubeError> {
        let any = table_provider.as_any();
        Ok(if let Some(t) = any.downcast_ref::<CubeTableProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<InfoSchemaTableProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<InfoSchemaColumnsProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<InfoSchemaStatisticsProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<InfoSchemaKeyColumnUsageProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<InfoSchemaSchemataProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<InfoSchemaReferentialConstraintsProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<InfoSchemaCollationsProvider>() {
            t.table_name().to_string()
        } else if let Some(t) = any.downcast_ref::<PerfSchemaVariablesProvider>() {
            t.table_name().to_string()
        } else {
            return Err(CubeError::internal(format!(
                "Unknown table provider with schema: {:?}",
                table_provider.schema()
            )));
        })
    }
}

impl<'a> ContextProvider for CubeContext<'a> {
    fn get_table_provider(
        &self,
        name: datafusion::catalog::TableReference,
    ) -> Option<std::sync::Arc<dyn datasource::TableProvider>> {
        let table_path = match name {
            datafusion::catalog::TableReference::Partial { schema, table, .. } => {
                Some(format!("{}.{}", schema, table))
            }
            datafusion::catalog::TableReference::Full {
                catalog,
                schema,
                table,
            } => Some(format!("{}.{}.{}", catalog, schema, table)),
            datafusion::catalog::TableReference::Bare { table } => Some(table.to_string()),
        };

        if let Some(tp) = table_path {
            if let Some(cube) = self
                .meta
                .cubes
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(&tp))
            {
                return Some(Arc::new(CubeTableProvider::new(cube.clone()))); // TODO .clone()
            }
            if tp.eq_ignore_ascii_case("information_schema.tables") {
                return Some(Arc::new(InfoSchemaTableProvider::new(self.meta.clone())));
            }

            if tp.eq_ignore_ascii_case("information_schema.columns") {
                return Some(Arc::new(InfoSchemaColumnsProvider::new(self.meta.clone())));
            }

            if tp.eq_ignore_ascii_case("information_schema.statistics") {
                return Some(Arc::new(InfoSchemaStatisticsProvider::new()));
            }

            if tp.eq_ignore_ascii_case("information_schema.key_column_usage") {
                return Some(Arc::new(InfoSchemaKeyColumnUsageProvider::new()));
            }

            if tp.eq_ignore_ascii_case("information_schema.schemata") {
                return Some(Arc::new(InfoSchemaSchemataProvider::new()));
            }

            if tp.eq_ignore_ascii_case("information_schema.referential_constraints") {
                return Some(Arc::new(InfoSchemaReferentialConstraintsProvider::new()));
            }

            if tp.eq_ignore_ascii_case("information_schema.collations") {
                return Some(Arc::new(InfoSchemaCollationsProvider::new()));
            }

            if tp.eq_ignore_ascii_case("performance_schema.global_variables") {
                return Some(Arc::new(PerfSchemaVariablesProvider::new(
                    "performance_schema.global_variables".to_string(),
                )));
            }

            if tp.eq_ignore_ascii_case("performance_schema.session_variables") {
                return Some(Arc::new(PerfSchemaVariablesProvider::new(
                    "performance_schema.session_variables".to_string(),
                )));
            }
        };

        None
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions.get(name).cloned()
    }
}

pub trait TableName {
    fn table_name(&self) -> &str;
}

pub struct CubeTableProvider {
    cube: V1CubeMeta,
}

impl CubeTableProvider {
    pub fn new(cube: V1CubeMeta) -> Self {
        Self { cube }
    }
}

impl TableName for CubeTableProvider {
    fn table_name(&self) -> &str {
        &self.cube.name
    }
}

#[async_trait]
impl TableProvider for CubeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.cube
                .get_columns()
                .into_iter()
                .map(|c| {
                    Field::new(
                        c.get_name(),
                        match c.get_data_type().as_str() {
                            "datetime" => DataType::Timestamp(TimeUnit::Millisecond, None),
                            "boolean" => DataType::Boolean,
                            "int" => DataType::Int64,
                            _ => DataType::Utf8,
                        },
                        true,
                    )
                })
                .collect(),
        ))
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Plan(format!(
            "Not rewritten table scan node for '{}' cube",
            self.cube.name
        )))
    }
}
