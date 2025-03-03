mod analysis;
pub mod converter;
mod cost;
pub mod language;
mod rewriter;
mod rules;

use crate::compile::rewrite::analysis::LogicalPlanAnalysis;
use crate::CubeError;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_plan::window_frames::WindowFrame;
use datafusion::logical_plan::{Column, ExprRewriter};
use datafusion::logical_plan::{DFSchema, Expr, JoinConstraint, JoinType, Operator};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::functions::BuiltinScalarFunction;
use datafusion::physical_plan::window_functions::WindowFunction;
use datafusion::scalar::ScalarValue;
use egg::{rewrite, Applier, Pattern, PatternAst, SearchMatches, Searcher, Subst, Symbol, Var};
use egg::{EGraph, Id, Rewrite};
use std::collections::HashMap;
use std::fmt::Display;
use std::slice::Iter;
use std::str::FromStr;

// trace_macros!(true);

crate::plan_to_language! {
    pub enum LogicalPlanLanguage {
        Projection {
            expr: Vec<Expr>,
            input: Arc<LogicalPlan>,
            schema: DFSchemaRef,
            alias: Option<String>,
        },
        Filter {
            predicate: Expr,
            input: Arc<LogicalPlan>,
        },
        Window {
            input: Arc<LogicalPlan>,
            window_expr: Vec<Expr>,
            schema: DFSchemaRef,
        },
        Aggregate {
            input: Arc<LogicalPlan>,
            group_expr: Vec<Expr>,
            aggr_expr: Vec<Expr>,
            schema: DFSchemaRef,
        },
        Sort {
            exp: Vec<Expr>,
            input: Arc<LogicalPlan>,
        },
        Join {
            left: Arc<LogicalPlan>,
            right: Arc<LogicalPlan>,
            left_on: Vec<Column>,
            right_on: Vec<Column>,
            join_type: JoinType,
            join_constraint: JoinConstraint,
            schema: DFSchemaRef,
        },
        CrossJoin {
            left: Arc<LogicalPlan>,
            right: Arc<LogicalPlan>,
            schema: DFSchemaRef,
        },
        Repartition {
            input: Arc<LogicalPlan>,
        },
        Union {
            inputs: Vec<LogicalPlan>,
            schema: DFSchemaRef,
            alias: Option<String>,
        },
        TableScan {
            source_table_name: String,
            table_name: String,
            projection: Option<Vec<usize>>,
            projected_schema: DFSchemaRef,
            filters: Vec<Expr>,
            limit: Option<usize>,
        },
        EmptyRelation {
            produce_one_row: bool,
            schema: DFSchemaRef,
        },
        Limit {
            n: usize,
            input: Arc<LogicalPlan>,
        },
        CreateExternalTable {
            schema: DFSchemaRef,
            name: String,
            location: String,
            has_header: bool,
        },
        Extension {
            node: Arc<LogicalPlan>,
        },

        AliasExpr {
            expr: Box<Expr>,
            alias: String,
        },
        ColumnExpr {
            column: Column,
        },
        ScalarVariableExpr {
            variable: Vec<String>,
        },
        LiteralExpr { value: ScalarValue, },
        BinaryExpr {
            left: Box<Expr>,
            op: Operator,
            right: Box<Expr>,
        },
        NotExpr { expr: Box<Expr>, },
        IsNotNullExpr { expr: Box<Expr>, },
        IsNullExpr { expr: Box<Expr>, },
        NegativeExpr { expr: Box<Expr>, },
        BetweenExpr {
            expr: Box<Expr>,
            negated: bool,
            low: Box<Expr>,
            high: Box<Expr>,
        },
        CaseExpr {
            expr: Option<Box<Expr>>,
            when_then_expr: Vec<(Box<Expr>, Box<Expr>)>,
            else_expr: Option<Box<Expr>>,
        },
        CastExpr {
            expr: Box<Expr>,
            data_type: DataType,
        },
        TryCastExpr {
            expr: Box<Expr>,
            data_type: DataType,
        },
        SortExpr {
            expr: Box<Expr>,
            asc: bool,
            nulls_first: bool,
        },
        ScalarFunctionExpr {
            fun: BuiltinScalarFunction,
            args: Vec<Expr>,
        },
        ScalarUDFExpr {
            fun: Arc<ScalarUDF>,
            args: Vec<Expr>,
        },
        AggregateFunctionExpr {
            fun: AggregateFunction,
            args: Vec<Expr>,
            distinct: bool,
        },
        WindowFunctionExpr {
            fun: WindowFunction,
            args: Vec<Expr>,
            partition_by: Vec<Expr>,
            order_by: Vec<Expr>,
            window_frame: Option<WindowFrame>,
        },
        AggregateUDFExpr {
            fun: Arc<AggregateUDF>,
            args: Vec<Expr>,
        },
        InListExpr {
            expr: Box<Expr>,
            list: Vec<Expr>,
            negated: bool,
        },
        WildcardExpr {},

        CubeScan {
            cube: Arc<LogicalPlan>,
            members: Vec<LogicalPlan>,
            filters: Vec<LogicalPlan>,
            order: Vec<LogicalPlan>,
            limit: Option<usize>,
            offset: Option<usize>,
            aliases: Option<Vec<String>>,
        },
        Measure {
            name: String,
            expr: Arc<Expr>,
        },
        Dimension {
            name: String,
            expr: Arc<Expr>,
        },
        Order {
            member: String,
            asc: bool,
        },
        FilterMember {
            member: String,
            op: String,
            values: Vec<String>,
        },
        SegmentMember {
            member: String,
        },
        MemberError {
            error: String,
        },
        FilterOp {
            filters: Vec<LogicalPlan>,
            op: String,
        },
        TimeDimension {
            name: String,
            granularity: Option<String>,
            date_range: Option<Vec<String>>,
            expr: Arc<Expr>,
        },
        MemberAlias {
            name: String,
        },
        MemberReplacer {
            members: Vec<LogicalPlan>,
            cube: Arc<LogicalPlan>,
        },
        TimeDimensionDateRangeReplacer {
            members: Vec<LogicalPlan>,
            member: String,
            date_range: Vec<String>,
        },
        FilterReplacer {
            filters: Vec<LogicalPlan>,
            cube: Option<String>,
        },
        OrderReplacer {
            sort_expr: Vec<LogicalPlan>,
            column_name_to_member: Vec<(String, String)>,
            cube: Option<String>,
        },
        ColumnAliasReplacer {
            members: Vec<LogicalPlan>,
            aliases: Vec<(String, String)>,
            cube: Option<String>,
        },
    }
}

// trace_macros!(false);

#[macro_export]
macro_rules! var_iter {
    ($eclass:expr, $field_variant:ident) => {{
        $eclass.nodes.iter().filter_map(|node| match node {
            LogicalPlanLanguage::$field_variant($field_variant(v)) => Some(v),
            _ => None,
        })
    }};
}

#[macro_export]
macro_rules! var {
    ($var_str:expr) => {
        $var_str.parse().unwrap()
    };
}

pub struct WithColumnRelation(String);

impl ExprRewriter for WithColumnRelation {
    fn mutate(&mut self, expr: Expr) -> Result<Expr, DataFusionError> {
        match expr {
            Expr::Column(c) => Ok(Expr::Column(Column {
                name: c.name.to_string(),
                relation: c.relation.or_else(|| Some(self.0.to_string())),
            })),
            e => Ok(e),
        }
    }
}

fn column_name_to_member_name(
    member_name_to_expr: Vec<(String, Expr)>,
    table_name: String,
) -> HashMap<String, String> {
    let mut relation = WithColumnRelation(table_name);
    member_name_to_expr
        .into_iter()
        .map(|(member, expr)| (expr_column_name_with_relation(expr, &mut relation), member))
        .collect::<HashMap<_, _>>()
}

fn referenced_columns(referenced_expr: Vec<Expr>, table_name: String) -> Vec<String> {
    let mut relation = WithColumnRelation(table_name);
    referenced_expr
        .into_iter()
        .map(|expr| expr_column_name_with_relation(expr, &mut relation))
        .collect::<Vec<_>>()
}

fn expr_column_name_with_relation(expr: Expr, relation: &mut WithColumnRelation) -> String {
    expr.rewrite(relation)
        .unwrap()
        .name(&DFSchema::empty())
        .unwrap()
}

fn expr_column_name(expr: Expr, cube: &Option<String>) -> String {
    if let Some(cube) = cube.as_ref() {
        expr_column_name_with_relation(expr, &mut WithColumnRelation(cube.to_string()))
    } else {
        expr.name(&DFSchema::empty()).unwrap()
    }
}

pub fn rewrite(
    name: &str,
    searcher: String,
    applier: String,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
    Rewrite::new(
        name.to_string(),
        searcher.parse::<Pattern<LogicalPlanLanguage>>().unwrap(),
        applier.parse::<Pattern<LogicalPlanLanguage>>().unwrap(),
    )
    .unwrap()
}

pub fn transforming_rewrite<T>(
    name: &str,
    searcher: String,
    applier: String,
    transform_fn: T,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>
where
    T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool
        + Sync
        + Send
        + 'static,
{
    Rewrite::new(
        name.to_string(),
        searcher.parse::<Pattern<LogicalPlanLanguage>>().unwrap(),
        TransformingPattern::new(applier.as_str(), transform_fn),
    )
    .unwrap()
}

pub fn transforming_chain_rewrite<T>(
    name: &str,
    main_searcher: String,
    chain: Vec<(&str, String)>,
    applier: String,
    transform_fn: T,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>
where
    T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool
        + Sync
        + Send
        + 'static,
{
    Rewrite::new(
        name.to_string(),
        ChainSearcher {
            main: main_searcher.parse().unwrap(),
            chain: chain
                .into_iter()
                .map(|(var, pattern)| (var.parse().unwrap(), pattern.parse().unwrap()))
                .collect(),
        },
        TransformingPattern::new(applier.as_str(), transform_fn),
    )
    .unwrap()
}

fn list_expr(list_type: impl Display, list: Vec<impl Display>) -> String {
    let mut current = list_type.to_string();
    for i in list.into_iter().rev() {
        current = format!("({} {} {})", list_type, i, current);
    }
    current
}

fn udf_expr(fun_name: impl Display, args: Vec<impl Display>) -> String {
    format!(
        "(ScalarUDFExpr ScalarUDFExprFun:{} {})",
        fun_name,
        list_expr("ScalarUDFExprArgs", args)
    )
}

fn fun_expr(fun_name: impl Display, args: Vec<impl Display>) -> String {
    format!(
        "(ScalarFunctionExpr {} {})",
        fun_name,
        list_expr("ScalarFunctionExprArgs", args)
    )
}

fn agg_fun_expr(fun_name: impl Display, args: Vec<impl Display>, distinct: impl Display) -> String {
    format!(
        "(AggregateFunctionExpr {} {} {})",
        fun_name,
        list_expr("AggregateFunctionExprArgs", args),
        distinct
    )
}

fn udaf_expr(fun_name: impl Display, args: Vec<impl Display>) -> String {
    format!(
        "(AggregateUDFExpr {} {})",
        fun_name,
        list_expr("AggregateUDFExprArgs", args),
    )
}

fn limit(n: impl Display, input: impl Display) -> String {
    format!("(Limit {} {})", n, input)
}

fn aggregate(input: impl Display, group: impl Display, aggr: impl Display) -> String {
    format!("(Aggregate {} {} {})", input, group, aggr)
}

fn aggr_aggr_expr(left: impl Display, right: impl Display) -> String {
    format!("(AggregateAggrExpr {} {})", left, right)
}

fn aggr_aggr_expr_empty_tail() -> String {
    format!("AggregateAggrExpr")
}

fn aggr_group_expr(left: impl Display, right: impl Display) -> String {
    format!("(AggregateGroupExpr {} {})", left, right)
}

fn projection_expr(left: impl Display, right: impl Display) -> String {
    format!("(ProjectionExpr {} {})", left, right)
}

fn sort_exp(left: impl Display, right: impl Display) -> String {
    format!("(SortExp {} {})", left, right)
}

fn sort_exp_empty_tail() -> String {
    format!("SortExp")
}

fn sort_expr(expr: impl Display, asc: impl Display, nulls_first: impl Display) -> String {
    format!("(SortExpr {} {} {})", expr, asc, nulls_first)
}

fn aggr_group_expr_empty_tail() -> String {
    format!("AggregateGroupExpr")
}

fn projection_expr_empty_tail() -> String {
    format!("ProjectionExpr")
}

fn to_day_interval_expr<D: Display>(period: D, unit: D) -> String {
    fun_expr("ToDayInterval", vec![period, unit])
}

fn binary_expr(left: impl Display, op: impl Display, right: impl Display) -> String {
    format!("(BinaryExpr {} {} {})", left, op, right)
}

fn inlist_expr(expr: impl Display, list: impl Display, negated: impl Display) -> String {
    format!("(InListExpr {} {} {})", expr, list, negated)
}

fn between_expr(
    expr: impl Display,
    negated: impl Display,
    low: impl Display,
    high: impl Display,
) -> String {
    format!("(BetweenExpr {} {} {} {})", expr, negated, low, high)
}

fn is_null_expr(expr: impl Display) -> String {
    format!("(IsNullExpr {})", expr)
}

fn is_not_null_expr(expr: impl Display) -> String {
    format!("(IsNotNullExpr {})", expr)
}

fn literal_expr(literal: impl Display) -> String {
    format!("(LiteralExpr {})", literal)
}

fn column_expr(column: impl Display) -> String {
    format!("(ColumnExpr {})", column)
}

fn alias_expr(column: impl Display, alias: impl Display) -> String {
    format!("(AliasExpr {} {})", column, alias)
}

fn literal_string(literal_str: impl Display) -> String {
    format!("(LiteralExpr LiteralExprValue:{})", literal_str)
}

fn projection(expr: impl Display, input: impl Display, alias: impl Display) -> String {
    format!("(Projection {} {} {})", expr, input, alias)
}

fn sort(expr: impl Display, input: impl Display) -> String {
    format!("(Sort {} {})", expr, input)
}

fn filter(expr: impl Display, input: impl Display) -> String {
    format!("(Filter {} {})", expr, input)
}

fn column_alias_replacer(
    members: impl Display,
    aliases: impl Display,
    cube: impl Display,
) -> String {
    format!("(ColumnAliasReplacer {} {} {})", members, aliases, cube)
}

fn member_replacer(members: impl Display, aliases: impl Display) -> String {
    format!("(MemberReplacer {} {})", members, aliases)
}

fn time_dimension_date_range_replacer(
    members: impl Display,
    time_dimension_member: impl Display,
    date_range: impl Display,
) -> String {
    format!(
        "(TimeDimensionDateRangeReplacer {} {} {})",
        members, time_dimension_member, date_range
    )
}

fn order_replacer(members: impl Display, aliases: impl Display, cube: impl Display) -> String {
    format!("(OrderReplacer {} {} {})", members, aliases, cube)
}

fn filter_replacer(members: impl Display, cube: impl Display) -> String {
    format!("(FilterReplacer {} {})", members, cube)
}

fn cube_scan_members(left: impl Display, right: impl Display) -> String {
    format!("(CubeScanMembers {} {})", left, right)
}

fn cube_scan_members_empty_tail() -> String {
    format!("CubeScanMembers")
}

fn cube_scan_filters(left: impl Display, right: impl Display) -> String {
    format!("(CubeScanFilters {} {})", left, right)
}

fn cube_scan_filters_empty_tail() -> String {
    format!("CubeScanFilters")
}

fn cube_scan_order(left: impl Display, right: impl Display) -> String {
    format!("(CubeScanOrder {} {})", left, right)
}

fn cube_scan_order_empty_tail() -> String {
    format!("CubeScanOrder")
}

fn order(member: impl Display, asc: impl Display) -> String {
    format!("(Order {} {})", member, asc)
}

fn filter_op(filters: impl Display, op: impl Display) -> String {
    format!("(FilterOp {} FilterOpOp:{})", filters, op)
}

fn filter_op_filters(left: impl Display, right: impl Display) -> String {
    format!("(FilterOpFilters {} {})", left, right)
}

fn filter_member(member: impl Display, op: impl Display, values: impl Display) -> String {
    format!("(FilterMember {} {} {})", member, op, values)
}

fn segment_member(member: impl Display) -> String {
    format!("(SegmentMember {})", member)
}

fn measure_expr(measure_name: impl Display, expr: impl Display) -> String {
    format!("(Measure {} {})", measure_name, expr)
}

fn dimension_expr(name: impl Display, expr: impl Display) -> String {
    format!("(Dimension {} {})", name, expr)
}

fn time_dimension_expr(
    name: impl Display,
    granularity: impl Display,
    date_range: impl Display,
    expr: impl Display,
) -> String {
    format!(
        "(TimeDimension {} {} {} {})",
        name, granularity, date_range, expr
    )
}

fn table_scan(
    source_table_name: impl Display,
    table_name: impl Display,
    projection: impl Display,
    filters: impl Display,
    limit: impl Display,
) -> String {
    format!(
        "(TableScan {} {} {} {} {})",
        source_table_name, table_name, projection, filters, limit
    )
}

fn cube_scan(
    source_table_name: impl Display,
    members: impl Display,
    filters: impl Display,
    orders: impl Display,
    limit: impl Display,
    offset: impl Display,
    aliases: impl Display,
) -> String {
    format!(
        "(Extension (CubeScan {} {} {} {} {} {} {}))",
        source_table_name, members, filters, orders, limit, offset, aliases
    )
}

pub struct ChainSearcher {
    main: Pattern<LogicalPlanLanguage>,
    chain: Vec<(Var, Pattern<LogicalPlanLanguage>)>,
}

impl Searcher<LogicalPlanLanguage, LogicalPlanAnalysis> for ChainSearcher {
    fn search(
        &self,
        egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    ) -> Vec<SearchMatches<LogicalPlanLanguage>> {
        let matches = self.main.search(egraph);
        let mut result = Vec::new();
        for m in matches {
            if let Some(m) = self.search_match_chained(egraph, m, self.chain.iter()) {
                result.push(m);
            }
        }
        result
    }

    fn search_eclass(
        &self,
        egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        eclass: Id,
    ) -> Option<SearchMatches<LogicalPlanLanguage>> {
        if let Some(m) = self.main.search_eclass(egraph, eclass) {
            self.search_match_chained(egraph, m, self.chain.iter())
        } else {
            None
        }
    }

    fn vars(&self) -> Vec<Var> {
        let mut vars = self.main.vars();
        for (_, p) in self.chain.iter() {
            vars.extend(p.vars());
        }
        vars
    }
}

impl ChainSearcher {
    fn search_match_chained<'a>(
        &self,
        egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        cur_match: SearchMatches<'a, LogicalPlanLanguage>,
        chain: Iter<(Var, Pattern<LogicalPlanLanguage>)>,
    ) -> Option<SearchMatches<'a, LogicalPlanLanguage>> {
        let mut chain = chain.clone();
        let mut matches_to_merge = Vec::new();
        if let Some((var, pattern)) = chain.next() {
            for subst in cur_match.substs.iter() {
                if let Some(id) = subst.get(var.clone()) {
                    if let Some(next_match) = pattern.search_eclass(egraph, id.clone()) {
                        let chain_matches = self.search_match_chained(
                            egraph,
                            SearchMatches {
                                eclass: cur_match.eclass.clone(),
                                substs: next_match
                                    .substs
                                    .iter()
                                    .map(|next_subst| {
                                        let mut new_subst = subst.clone();
                                        for pattern_var in pattern.vars().into_iter() {
                                            if let Some(pattern_var_value) =
                                                next_subst.get(pattern_var)
                                            {
                                                new_subst
                                                    .insert(pattern_var, pattern_var_value.clone());
                                            }
                                        }
                                        new_subst
                                    })
                                    .collect::<Vec<_>>(),
                                // TODO merge
                                ast: cur_match.ast.clone(),
                            },
                            chain.clone(),
                        );
                        matches_to_merge.extend(chain_matches);
                    }
                }
            }
            if !matches_to_merge.is_empty() {
                let mut substs = Vec::new();
                for m in matches_to_merge {
                    substs.extend(m.substs.clone());
                }
                Some(SearchMatches {
                    eclass: cur_match.eclass.clone(),
                    substs,
                    // TODO merge
                    ast: cur_match.ast.clone(),
                })
            } else {
                None
            }
        } else {
            Some(cur_match)
        }
    }
}

pub struct TransformingPattern<T>
where
    T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool,
{
    pattern: Pattern<LogicalPlanLanguage>,
    vars_to_substitute: T,
}

impl<T> TransformingPattern<T>
where
    T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool,
{
    pub fn new(pattern: &str, vars_to_substitute: T) -> Self {
        Self {
            pattern: pattern.parse().unwrap(),
            vars_to_substitute,
        }
    }
}

impl<T> Applier<LogicalPlanLanguage, LogicalPlanAnalysis> for TransformingPattern<T>
where
    T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool,
{
    fn apply_one(
        &self,
        egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        eclass: Id,
        subst: &Subst,
        searcher_ast: Option<&PatternAst<LogicalPlanLanguage>>,
        rule_name: Symbol,
    ) -> Vec<Id> {
        let mut new_subst = subst.clone();
        if (self.vars_to_substitute)(egraph, &mut new_subst) {
            self.pattern
                .apply_one(egraph, eclass, &new_subst, searcher_ast, rule_name)
        } else {
            Vec::new()
        }
    }
}
