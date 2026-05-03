use postgres_types::ToSql;

use crate::expr::{InOperandKind, WindowFrame, WindowFrameBound, WindowFrameUnit, WindowSpec};
use crate::query::{
    ForLock, InsertConflict, LockStrength, LockWait, SelectDistinct, SelectTail, SetOp,
};
use crate::table::TableSourceKind;
use crate::{
    quote_ident, ArithmeticOp, ArrayQuantifier, Assignment, BinaryOp, BindValue, DeleteQuery,
    Error, ExprNode, FieldRef, InsertQuery, Join, JoinKind, OrderDirection, OrderExpr, Result,
    SelectItem, SelectQuery, Table, TableSourceRef, UnaryOp, UpdateQuery,
};

/// A rendered SQL statement plus owned bind values.
#[derive(Debug, Clone)]
pub struct RenderedQuery {
    sql: String,
    binds: Vec<BindValue>,
}

impl RenderedQuery {
    pub fn new(sql: String, binds: Vec<BindValue>) -> Self {
        Self { sql, binds }
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn binds(&self) -> &[BindValue] {
        &self.binds
    }

    /// Borrow bind values in the shape expected by `tokio-postgres`.
    pub fn bind_refs(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.binds
            .iter()
            .map(|bind| &**bind as &(dyn ToSql + Sync))
            .collect()
    }
}

impl std::fmt::Display for RenderedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.sql)
    }
}

/// Something that can render itself into PostgreSQL SQL and binds.
pub trait RenderQuery {
    fn render(self) -> Result<RenderedQuery>;
}

/// A renderable query that knows its row type.
///
/// Implemented by every typed query (`SelectQuery`, `InsertQuery` with
/// `RETURNING`, etc.). Lets generic runners on `DslContext` infer the
/// decoded row type from the query itself, without an explicit turbofish.
pub trait Query: RenderQuery {
    /// The Rust type each result row decodes into.
    type Row;
}

impl<R, S> Query for SelectQuery<R, S> {
    type Row = R;
}

impl<R> Query for InsertQuery<R> {
    type Row = R;
}

impl<R> Query for UpdateQuery<R> {
    type Row = R;
}

impl<R> Query for DeleteQuery<R> {
    type Row = R;
}

#[derive(Default)]
struct Renderer {
    sql: String,
    binds: Vec<BindValue>,
}

impl Renderer {
    fn finish(self) -> RenderedQuery {
        RenderedQuery::new(self.sql, self.binds)
    }

    fn push_bind(&mut self, value: BindValue) {
        self.binds.push(value);
        self.sql.push('$');
        self.sql.push_str(&self.binds.len().to_string());
    }

    fn push_i64_bind(&mut self, value: i64) {
        self.push_bind(std::sync::Arc::new(value));
    }
}

impl<R, S> RenderQuery for SelectQuery<R, S> {
    fn render(self) -> Result<RenderedQuery> {
        let mut renderer = Renderer::default();
        render_select(self, &mut renderer, ProjectionCastMode::ClientDecoded)?;
        Ok(renderer.finish())
    }
}

impl<R> RenderQuery for InsertQuery<R> {
    fn render(self) -> Result<RenderedQuery> {
        let mut renderer = Renderer::default();
        render_insert(self, &mut renderer)?;
        Ok(renderer.finish())
    }
}

impl<R> RenderQuery for UpdateQuery<R> {
    fn render(self) -> Result<RenderedQuery> {
        let mut renderer = Renderer::default();
        render_update(self, &mut renderer)?;
        Ok(renderer.finish())
    }
}

impl<R> RenderQuery for DeleteQuery<R> {
    fn render(self) -> Result<RenderedQuery> {
        let mut renderer = Renderer::default();
        render_delete(self, &mut renderer)?;
        Ok(renderer.finish())
    }
}

fn render_select<R, S>(
    query: SelectQuery<R, S>,
    renderer: &mut Renderer,
    projection_cast_mode: ProjectionCastMode,
) -> Result<()> {
    if query.selection.is_empty() {
        return Err(Error::invalid_query_shape(
            "SELECT requires at least one item",
        ));
    }
    let from = query
        .from
        .ok_or_else(|| Error::invalid_query_shape("SELECT requires a FROM source"))?;

    if !query.ctes.is_empty() {
        renderer.sql.push_str("with ");
        for (index, cte) in query.ctes.into_iter().enumerate() {
            if index > 0 {
                renderer.sql.push_str(", ");
            }
            renderer.sql.push_str(&quote_ident(cte.name)?);
            renderer.sql.push_str(" as (");
            render_select(cte.query, renderer, ProjectionCastMode::Data)?;
            renderer.sql.push(')');
        }
        renderer.sql.push(' ');
    }

    let has_set_ops = !query.set_ops.is_empty();

    let set_op_group_start = if has_set_ops {
        let start = renderer.sql.len();
        renderer.sql.push('(');
        Some(start)
    } else {
        None
    };

    let order_projection_cast_fields = collect_distinct_order_projection_cast_fields(
        &query.selection,
        query.distinct.as_ref(),
        projection_cast_mode,
    );

    renderer.sql.push_str("select ");
    if let Some(distinct) = query.distinct {
        render_distinct(distinct, renderer)?;
    }
    render_select_items(
        query.selection,
        renderer,
        FieldQualification::Qualified,
        projection_cast_mode,
    )?;
    renderer.sql.push_str(" from ");
    render_table_source(from, renderer)?;

    for join in query.joins {
        render_join(join, renderer)?;
    }

    if let Some(condition) = query.where_clause {
        renderer.sql.push_str(" where ");
        render_expr(
            condition.into_node(),
            renderer,
            FieldQualification::Qualified,
        )?;
    }

    if !query.group_by.is_empty() {
        renderer.sql.push_str(" group by ");
        render_expr_list(query.group_by, renderer, FieldQualification::Qualified)?;
    }

    if let Some(condition) = query.having {
        renderer.sql.push_str(" having ");
        render_expr(
            condition.into_node(),
            renderer,
            FieldQualification::Qualified,
        )?;
    }

    if let Some(group_start) = set_op_group_start {
        for (index, set_op) in query.set_ops.into_iter().enumerate() {
            if index > 0 {
                renderer.sql.insert(group_start, '(');
            }
            let qualification = if index > 0 {
                FieldQualification::Unqualified
            } else {
                FieldQualification::Qualified
            };
            let left_order_projection_cast_fields = if index == 0 {
                order_projection_cast_fields.as_slice()
            } else {
                &[]
            };
            render_select_tail(
                set_op.left_tail,
                renderer,
                qualification,
                left_order_projection_cast_fields,
            )?;
            renderer.sql.push(')');
            renderer.sql.push(' ');
            renderer.sql.push_str(set_op_keyword(set_op.op, set_op.all));
            renderer.sql.push_str(" (");
            render_select(set_op.query, renderer, projection_cast_mode)?;
            renderer.sql.push(')');
        }
    }

    let qualification = if has_set_ops {
        FieldQualification::Unqualified
    } else {
        FieldQualification::Qualified
    };
    let tail_order_projection_cast_fields = if has_set_ops {
        &[]
    } else {
        order_projection_cast_fields.as_slice()
    };
    render_select_tail(
        SelectTail {
            order_by: query.order_by,
            limit: query.limit,
            offset: query.offset,
            for_lock: query.for_lock,
        },
        renderer,
        qualification,
        tail_order_projection_cast_fields,
    )?;

    Ok(())
}

fn render_select_tail(
    tail: SelectTail,
    renderer: &mut Renderer,
    qualification: FieldQualification,
    order_projection_cast_fields: &[FieldRef],
) -> Result<()> {
    if !tail.order_by.is_empty() {
        renderer.sql.push_str(" order by ");
        render_order_by_with_qualification(
            tail.order_by,
            renderer,
            qualification,
            order_projection_cast_fields,
        )?;
    }

    if let Some(limit) = tail.limit {
        if limit < 0 {
            return Err(Error::invalid_query_shape("LIMIT cannot be negative"));
        }
        renderer.sql.push_str(" limit ");
        renderer.push_i64_bind(limit);
    }

    if let Some(offset) = tail.offset {
        if offset < 0 {
            return Err(Error::invalid_query_shape("OFFSET cannot be negative"));
        }
        renderer.sql.push_str(" offset ");
        renderer.push_i64_bind(offset);
    }

    if let Some(for_lock) = tail.for_lock {
        render_for_lock(for_lock, renderer)?;
    }

    Ok(())
}

fn set_op_keyword(op: SetOp, all: bool) -> &'static str {
    match (op, all) {
        (SetOp::Union, false) => "union",
        (SetOp::Union, true) => "union all",
        (SetOp::Except, false) => "except",
        (SetOp::Except, true) => "except all",
        (SetOp::Intersect, false) => "intersect",
        (SetOp::Intersect, true) => "intersect all",
    }
}

fn render_for_lock(lock: ForLock, renderer: &mut Renderer) -> Result<()> {
    renderer.sql.push_str(match lock.strength {
        LockStrength::Update => " for update",
        LockStrength::NoKeyUpdate => " for no key update",
        LockStrength::Share => " for share",
        LockStrength::KeyShare => " for key share",
    });

    if !lock.of.is_empty() {
        renderer.sql.push_str(" of ");
        for (index, table) in lock.of.into_iter().enumerate() {
            if index > 0 {
                renderer.sql.push_str(", ");
            }
            render_lock_target(table, renderer)?;
        }
    }

    match lock.wait {
        LockWait::Wait => {}
        LockWait::NoWait => renderer.sql.push_str(" nowait"),
        LockWait::SkipLocked => renderer.sql.push_str(" skip locked"),
    }

    Ok(())
}

fn render_lock_target(table: Table, renderer: &mut Renderer) -> Result<()> {
    renderer
        .sql
        .push_str(&quote_ident(table.alias().unwrap_or_else(|| table.name()))?);
    Ok(())
}

fn render_distinct(distinct: SelectDistinct, renderer: &mut Renderer) -> Result<()> {
    match distinct {
        SelectDistinct::Distinct => {
            renderer.sql.push_str("distinct ");
        }
        SelectDistinct::DistinctOn(exprs) => {
            if exprs.is_empty() {
                return Err(Error::invalid_query_shape(
                    "DISTINCT ON requires at least one expression",
                ));
            }
            renderer.sql.push_str("distinct on (");
            render_expr_list(exprs, renderer, FieldQualification::Qualified)?;
            renderer.sql.push_str(") ");
        }
    }
    Ok(())
}

fn render_insert<R>(query: InsertQuery<R>, renderer: &mut Renderer) -> Result<()> {
    if let Some(select_source) = query.select_source {
        if query.insert_columns.is_empty() {
            return Err(Error::invalid_query_shape(
                "INSERT ... SELECT requires .columns(...) before .from_select(...)",
            ));
        }
        if !query.rows.is_empty() {
            return Err(Error::invalid_query_shape(
                "INSERT cannot combine .values(...) with .from_select(...)",
            ));
        }
        for column in &query.insert_columns {
            if !column.table().same_identity(query.table) {
                return Err(Error::invalid_query_shape(format!(
                    "insert column {} does not belong to target table {}",
                    column.name(),
                    query.table.name()
                )));
            }
        }
        if query.insert_columns.len() != select_source.selection.len() {
            return Err(Error::invalid_query_shape(format!(
                "INSERT ... SELECT column count mismatch: {} target columns but {} selected expressions",
                query.insert_columns.len(),
                select_source.selection.len()
            )));
        }

        renderer.sql.push_str("insert into ");
        render_table(query.table, renderer)?;
        renderer.sql.push_str(" (");
        for (index, column) in query.insert_columns.iter().enumerate() {
            if index > 0 {
                renderer.sql.push_str(", ");
            }
            renderer.sql.push_str(&quote_ident(column.name())?);
        }
        renderer.sql.push_str(") ");
        render_select(select_source, renderer, ProjectionCastMode::Data)?;

        if let Some(on_conflict) = query.on_conflict {
            render_insert_conflict(query.table, on_conflict, renderer)?;
        }

        if !query.returning.is_empty() {
            renderer.sql.push_str(" returning ");
            render_select_items(
                query.returning,
                renderer,
                FieldQualification::Unqualified,
                ProjectionCastMode::ClientDecoded,
            )?;
        }

        return Ok(());
    }

    if query.rows.is_empty() {
        return Err(Error::invalid_query_shape(
            "INSERT requires at least one value",
        ));
    }
    validate_insert_rows(query.table, &query.rows)?;

    renderer.sql.push_str("insert into ");
    render_table(query.table, renderer)?;
    renderer.sql.push_str(" (");
    for (index, assignment) in query.rows[0].iter().enumerate() {
        if index > 0 {
            renderer.sql.push_str(", ");
        }
        renderer
            .sql
            .push_str(&quote_ident(assignment.field.name())?);
    }

    renderer.sql.push_str(") values ");
    for (row_index, row) in query.rows.into_iter().enumerate() {
        if row_index > 0 {
            renderer.sql.push_str(", ");
        }
        renderer.sql.push('(');
        for (assignment_index, assignment) in row.into_iter().enumerate() {
            if assignment_index > 0 {
                renderer.sql.push_str(", ");
            }
            render_expr(assignment.value, renderer, FieldQualification::Qualified)?;
        }
        renderer.sql.push(')');
    }

    if let Some(on_conflict) = query.on_conflict {
        render_insert_conflict(query.table, on_conflict, renderer)?;
    }

    if !query.returning.is_empty() {
        renderer.sql.push_str(" returning ");
        render_select_items(
            query.returning,
            renderer,
            FieldQualification::Unqualified,
            ProjectionCastMode::ClientDecoded,
        )?;
    }

    Ok(())
}

fn render_insert_conflict(
    table: Table,
    on_conflict: InsertConflict,
    renderer: &mut Renderer,
) -> Result<()> {
    match on_conflict {
        InsertConflict::DoNothing { target } => {
            render_conflict_target(table, target, renderer)?;
            renderer.sql.push_str(" do nothing");
        }
        InsertConflict::DoUpdate {
            target,
            assignments,
        } => {
            if assignments.is_empty() {
                return Err(Error::invalid_query_shape(
                    "ON CONFLICT DO UPDATE requires at least one assignment",
                ));
            }
            validate_assignments_target(table, &assignments)?;
            render_conflict_target(table, target, renderer)?;
            renderer.sql.push_str(" do update set ");
            for (index, assignment) in assignments.into_iter().enumerate() {
                if index > 0 {
                    renderer.sql.push_str(", ");
                }
                renderer
                    .sql
                    .push_str(&quote_ident(assignment.field.name())?);
                renderer.sql.push_str(" = ");
                render_expr_with_excluded_target(assignment.value, renderer, table)?;
            }
        }
    }

    Ok(())
}

fn render_conflict_target(
    table: Table,
    target: Vec<FieldRef>,
    renderer: &mut Renderer,
) -> Result<()> {
    if target.is_empty() {
        return Err(Error::invalid_query_shape(
            "ON CONFLICT requires at least one target field",
        ));
    }
    for field in &target {
        if !field.table().same_identity(table) {
            return Err(Error::invalid_query_shape(format!(
                "conflict target field {} does not belong to target table {}",
                field.name(),
                table.name()
            )));
        }
    }

    renderer.sql.push_str(" on conflict (");
    for (index, field) in target.into_iter().enumerate() {
        if index > 0 {
            renderer.sql.push_str(", ");
        }
        renderer.sql.push_str(&quote_ident(field.name())?);
    }
    renderer.sql.push(')');

    Ok(())
}

fn render_update<R>(query: UpdateQuery<R>, renderer: &mut Renderer) -> Result<()> {
    if query.assignments.is_empty() {
        return Err(Error::invalid_query_shape(
            "UPDATE requires at least one assignment",
        ));
    }
    validate_assignments_target(query.table, &query.assignments)?;
    let returning_qualification = if query.from.is_some() {
        FieldQualification::Qualified
    } else {
        FieldQualification::Unqualified
    };

    renderer.sql.push_str("update ");
    render_table(query.table, renderer)?;
    renderer.sql.push_str(" set ");
    for (index, assignment) in query.assignments.into_iter().enumerate() {
        if index > 0 {
            renderer.sql.push_str(", ");
        }
        renderer
            .sql
            .push_str(&quote_ident(assignment.field.name())?);
        renderer.sql.push_str(" = ");
        render_expr(assignment.value, renderer, FieldQualification::Qualified)?;
    }

    if let Some(from) = query.from {
        renderer.sql.push_str(" from ");
        render_table_source(from, renderer)?;
    }

    if let Some(condition) = query.where_clause {
        renderer.sql.push_str(" where ");
        render_expr(
            condition.into_node(),
            renderer,
            FieldQualification::Qualified,
        )?;
    }

    if !query.returning.is_empty() {
        renderer.sql.push_str(" returning ");
        render_select_items(
            query.returning,
            renderer,
            returning_qualification,
            ProjectionCastMode::ClientDecoded,
        )?;
    }

    Ok(())
}

fn render_delete<R>(query: DeleteQuery<R>, renderer: &mut Renderer) -> Result<()> {
    let returning_qualification = if query.using.is_some() {
        FieldQualification::Qualified
    } else {
        FieldQualification::Unqualified
    };

    renderer.sql.push_str("delete from ");
    render_table(query.table, renderer)?;

    if let Some(using) = query.using {
        renderer.sql.push_str(" using ");
        render_table_source(using, renderer)?;
    }

    if let Some(condition) = query.where_clause {
        renderer.sql.push_str(" where ");
        render_expr(
            condition.into_node(),
            renderer,
            FieldQualification::Qualified,
        )?;
    }

    if !query.returning.is_empty() {
        renderer.sql.push_str(" returning ");
        render_select_items(
            query.returning,
            renderer,
            returning_qualification,
            ProjectionCastMode::ClientDecoded,
        )?;
    }

    Ok(())
}

fn validate_assignments_target(table: Table, assignments: &[Assignment]) -> Result<()> {
    for assignment in assignments {
        if !assignment.field.table().same_identity(table) {
            return Err(Error::invalid_query_shape(format!(
                "assignment field {} does not belong to target table {}",
                assignment.field.name(),
                table.name()
            )));
        }
    }
    Ok(())
}

fn validate_insert_rows(table: Table, rows: &[Vec<Assignment>]) -> Result<()> {
    let first_row = rows
        .first()
        .ok_or_else(|| Error::invalid_query_shape("INSERT requires at least one value"))?;
    if first_row.is_empty() {
        return Err(Error::invalid_query_shape(
            "INSERT requires at least one value",
        ));
    }
    validate_assignments_target(table, first_row)?;

    let first_fields: Vec<FieldRef> = first_row
        .iter()
        .map(|assignment| assignment.field)
        .collect();

    for row in rows.iter().skip(1) {
        if row.is_empty() {
            return Err(Error::invalid_query_shape(
                "INSERT requires at least one value per row",
            ));
        }
        validate_assignments_target(table, row)?;

        let fields_match = row
            .iter()
            .map(|assignment| assignment.field)
            .eq(first_fields.iter().copied());
        if !fields_match {
            return Err(Error::invalid_query_shape(
                "INSERT rows must assign the same fields in the same order",
            ));
        }
    }

    Ok(())
}

fn collect_distinct_order_projection_cast_fields(
    selection: &[SelectItem],
    distinct: Option<&SelectDistinct>,
    projection_cast_mode: ProjectionCastMode,
) -> Vec<FieldRef> {
    if !matches!(projection_cast_mode, ProjectionCastMode::ClientDecoded)
        || !matches!(distinct, Some(SelectDistinct::Distinct))
    {
        return Vec::new();
    }

    selection
        .iter()
        .filter_map(|item| match &item.expr {
            ExprNode::Field(field) if field.select_cast_type().is_some() => Some(*field),
            _ => None,
        })
        .collect()
}

fn render_select_items(
    items: Vec<SelectItem>,
    renderer: &mut Renderer,
    qualification: FieldQualification,
    projection_cast_mode: ProjectionCastMode,
) -> Result<()> {
    for (index, item) in items.into_iter().enumerate() {
        if index > 0 {
            renderer.sql.push_str(", ");
        }
        match (projection_cast_mode, item.expr) {
            (ProjectionCastMode::ClientDecoded, ExprNode::Field(field))
                if field.select_cast_type().is_some() =>
            {
                render_field_select_cast(field, renderer, qualification)?;
            }
            (_, expr) => render_expr(expr, renderer, qualification)?,
        }
        if let Some(alias) = item.alias {
            renderer.sql.push_str(" as ");
            renderer.sql.push_str(&quote_ident(alias)?);
        }
    }
    Ok(())
}

fn render_expr_list(
    exprs: Vec<ExprNode>,
    renderer: &mut Renderer,
    qualification: FieldQualification,
) -> Result<()> {
    for (index, expr) in exprs.into_iter().enumerate() {
        if index > 0 {
            renderer.sql.push_str(", ");
        }
        render_expr(expr, renderer, qualification)?;
    }
    Ok(())
}

fn render_join(join: Join, renderer: &mut Renderer) -> Result<()> {
    renderer.sql.push(' ');
    match join.kind {
        JoinKind::Inner => renderer.sql.push_str("join "),
        JoinKind::Left => renderer.sql.push_str("left join "),
        JoinKind::Right => renderer.sql.push_str("right join "),
        JoinKind::Full => renderer.sql.push_str("full join "),
        JoinKind::Cross => renderer.sql.push_str("cross join "),
    }
    if join.lateral {
        renderer.sql.push_str("lateral ");
    }
    render_table_source(join.source, renderer)?;
    if let Some(on) = join.on {
        renderer.sql.push_str(" on ");
        render_expr(on.into_node(), renderer, FieldQualification::Qualified)?;
    }
    Ok(())
}

fn render_order_by_with_qualification(
    order_by: Vec<OrderExpr>,
    renderer: &mut Renderer,
    qualification: FieldQualification,
    order_projection_cast_fields: &[FieldRef],
) -> Result<()> {
    for (index, order) in order_by.into_iter().enumerate() {
        if index > 0 {
            renderer.sql.push_str(", ");
        }
        render_order_expr(
            order.expr,
            renderer,
            qualification,
            order_projection_cast_fields,
        )?;
        renderer.sql.push(' ');
        renderer.sql.push_str(match order.direction {
            OrderDirection::Asc => "asc",
            OrderDirection::Desc => "desc",
        });
    }
    Ok(())
}

fn render_order_expr(
    expr: ExprNode,
    renderer: &mut Renderer,
    qualification: FieldQualification,
    order_projection_cast_fields: &[FieldRef],
) -> Result<()> {
    match expr {
        ExprNode::Field(field) => {
            if let Some(select_cast_type) = order_projection_cast_fields
                .iter()
                .find(|selected| **selected == field)
                .and_then(|selected| selected.select_cast_type())
            {
                render_field_cast(field, select_cast_type, renderer, qualification)
            } else {
                render_expr(ExprNode::Field(field), renderer, qualification)
            }
        }
        expr => render_expr(expr, renderer, qualification),
    }
}

fn render_field_select_cast(
    field: FieldRef,
    renderer: &mut Renderer,
    qualification: FieldQualification,
) -> Result<()> {
    let Some(select_cast_type) = field.select_cast_type() else {
        return render_expr(ExprNode::Field(field), renderer, qualification);
    };

    render_field_cast(field, select_cast_type, renderer, qualification)
}

fn render_field_cast(
    field: FieldRef,
    select_cast_type: &str,
    renderer: &mut Renderer,
    qualification: FieldQualification,
) -> Result<()> {
    renderer.sql.push_str("cast(");
    render_expr(ExprNode::Field(field), renderer, qualification)?;
    renderer.sql.push_str(" as ");
    renderer.sql.push_str(select_cast_type);
    renderer.sql.push(')');
    Ok(())
}

fn render_table(table: Table, renderer: &mut Renderer) -> Result<()> {
    if let Some(schema) = table.schema() {
        renderer.sql.push_str(&quote_ident(schema)?);
        renderer.sql.push('.');
    }
    renderer.sql.push_str(&quote_ident(table.name())?);

    if let Some(alias) = table.alias() {
        renderer.sql.push_str(" as ");
        renderer.sql.push_str(&quote_ident(alias)?);
    }

    Ok(())
}

fn render_table_source(source: TableSourceRef, renderer: &mut Renderer) -> Result<()> {
    match source.kind {
        TableSourceKind::Table(table) => render_table(table, renderer),
        TableSourceKind::Subquery(subquery) => {
            renderer.sql.push('(');
            render_select(*subquery.query, renderer, ProjectionCastMode::Data)?;
            renderer.sql.push_str(") as ");
            renderer.sql.push_str(&quote_ident(subquery.alias)?);
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum FieldQualification {
    Qualified,
    Unqualified,
}

#[derive(Debug, Clone, Copy)]
enum ProjectionCastMode {
    ClientDecoded,
    Data,
}

#[derive(Debug, Clone, Copy)]
struct ExprRenderContext {
    qualification: FieldQualification,
    excluded_target: Option<Table>,
}

impl ExprRenderContext {
    const fn new(qualification: FieldQualification) -> Self {
        Self {
            qualification,
            excluded_target: None,
        }
    }

    const fn for_excluded_target(table: Table) -> Self {
        Self {
            qualification: FieldQualification::Qualified,
            excluded_target: Some(table),
        }
    }
}

fn render_field_ref(
    field: FieldRef,
    renderer: &mut Renderer,
    context: ExprRenderContext,
) -> Result<()> {
    if matches!(context.qualification, FieldQualification::Qualified) {
        let table = context
            .excluded_target
            .filter(|target| field.table().same_identity(*target))
            .unwrap_or_else(|| field.table());
        let qualifier = table.alias().unwrap_or_else(|| table.name());
        renderer.sql.push_str(&quote_ident(qualifier)?);
        renderer.sql.push('.');
    }
    renderer.sql.push_str(&quote_ident(field.name())?);
    Ok(())
}

fn render_expr(
    expr: ExprNode,
    renderer: &mut Renderer,
    qualification: FieldQualification,
) -> Result<()> {
    render_expr_with_context(expr, renderer, ExprRenderContext::new(qualification))
}

fn render_expr_with_excluded_target(
    expr: ExprNode,
    renderer: &mut Renderer,
    table: Table,
) -> Result<()> {
    render_expr_with_context(
        expr,
        renderer,
        ExprRenderContext::for_excluded_target(table),
    )
}

fn render_expr_with_context(
    expr: ExprNode,
    renderer: &mut Renderer,
    context: ExprRenderContext,
) -> Result<()> {
    match expr {
        ExprNode::Field(field) => render_field_ref(field, renderer, context),
        ExprNode::ExcludedField(field) => {
            let Some(table) = context.excluded_target else {
                return Err(Error::invalid_query_shape(
                    "excluded fields are only valid in ON CONFLICT DO UPDATE assignments",
                ));
            };
            if !field.table().same_identity(table) {
                return Err(Error::invalid_query_shape(format!(
                    "excluded field {} does not belong to target table {}",
                    field.name(),
                    table.name()
                )));
            }
            renderer.sql.push_str("excluded.");
            renderer.sql.push_str(&quote_ident(field.name())?);
            Ok(())
        }
        ExprNode::Bind(bind) => {
            renderer.push_bind(bind.into_value());
            Ok(())
        }
        ExprNode::Binary { op, left, right } => {
            renderer.sql.push('(');
            render_expr_with_context(*left, renderer, context)?;
            renderer.sql.push(' ');
            renderer.sql.push_str(binary_op_sql(op));
            renderer.sql.push(' ');
            render_expr_with_context(*right, renderer, context)?;
            renderer.sql.push(')');
            Ok(())
        }
        ExprNode::Unary { op, expr } => {
            renderer.sql.push('(');
            renderer.sql.push_str(match op {
                UnaryOp::Not => "not ",
            });
            render_expr_with_context(*expr, renderer, context)?;
            renderer.sql.push(')');
            Ok(())
        }
        ExprNode::IsNull { expr, negated } => {
            renderer.sql.push('(');
            render_expr_with_context(*expr, renderer, context)?;
            if negated {
                renderer.sql.push_str(" is not null");
            } else {
                renderer.sql.push_str(" is null");
            }
            renderer.sql.push(')');
            Ok(())
        }
        ExprNode::In {
            expr,
            operand,
            negated,
        } => {
            renderer.sql.push('(');
            render_expr_with_context(*expr, renderer, context)?;
            match operand.kind {
                InOperandKind::List(list) => {
                    if list.is_empty() {
                        return Err(Error::invalid_query_shape(
                            "IN predicate requires at least one list item",
                        ));
                    }

                    if negated {
                        renderer.sql.push_str(" not in (");
                    } else {
                        renderer.sql.push_str(" in (");
                    }
                    for (index, item) in list.into_iter().enumerate() {
                        if index > 0 {
                            renderer.sql.push_str(", ");
                        }
                        render_expr_with_context(item, renderer, context)?;
                    }
                    renderer.sql.push_str("))");
                }
                InOperandKind::Subquery(query) => {
                    if query.selection.len() != 1 {
                        return Err(Error::invalid_query_shape(
                            "IN subquery requires exactly one selected item",
                        ));
                    }

                    if negated {
                        renderer.sql.push_str(" not in (");
                    } else {
                        renderer.sql.push_str(" in (");
                    }
                    render_select(*query, renderer, ProjectionCastMode::Data)?;
                    renderer.sql.push_str("))");
                }
            }
            Ok(())
        }
        ExprNode::Between {
            expr,
            low,
            high,
            negated,
        } => {
            renderer.sql.push('(');
            render_expr_with_context(*expr, renderer, context)?;
            if negated {
                renderer.sql.push_str(" not between ");
            } else {
                renderer.sql.push_str(" between ");
            }
            render_expr_with_context(*low, renderer, context)?;
            renderer.sql.push_str(" and ");
            render_expr_with_context(*high, renderer, context)?;
            renderer.sql.push(')');
            Ok(())
        }
        ExprNode::Arithmetic { op, left, right } => {
            renderer.sql.push('(');
            render_expr_with_context(*left, renderer, context)?;
            renderer.sql.push(' ');
            renderer.sql.push_str(arithmetic_op_sql(op));
            renderer.sql.push(' ');
            render_expr_with_context(*right, renderer, context)?;
            renderer.sql.push(')');
            Ok(())
        }
        ExprNode::ArrayComparison {
            quantifier,
            left,
            array,
        } => {
            renderer.sql.push('(');
            render_expr_with_context(*left, renderer, context)?;
            renderer.sql.push_str(" = ");
            renderer.sql.push_str(array_quantifier_sql(quantifier));
            renderer.sql.push('(');
            render_expr_with_context(*array, renderer, context)?;
            renderer.sql.push_str("))");
            Ok(())
        }
        ExprNode::StringConcat { left, right } => {
            renderer.sql.push('(');
            render_expr_with_context(*left, renderer, context)?;
            renderer.sql.push_str(" || ");
            render_expr_with_context(*right, renderer, context)?;
            renderer.sql.push(')');
            Ok(())
        }
        ExprNode::Function { name, args } => {
            renderer.sql.push_str(name);
            renderer.sql.push('(');
            for (index, arg) in args.into_iter().enumerate() {
                if index > 0 {
                    renderer.sql.push_str(", ");
                }
                render_expr_with_context(arg, renderer, context)?;
            }
            renderer.sql.push(')');
            Ok(())
        }
        ExprNode::Case {
            branches,
            else_expr,
        } => {
            if branches.is_empty() {
                return Err(Error::invalid_query_shape(
                    "CASE expression requires at least one WHEN branch",
                ));
            }

            renderer.sql.push_str("(case");
            for (condition, value) in branches {
                renderer.sql.push_str(" when ");
                render_expr_with_context(condition, renderer, context)?;
                renderer.sql.push_str(" then ");
                render_expr_with_context(value, renderer, context)?;
            }
            if let Some(else_expr) = else_expr {
                renderer.sql.push_str(" else ");
                render_expr_with_context(*else_expr, renderer, context)?;
            }
            renderer.sql.push_str(" end)");
            Ok(())
        }
        ExprNode::Window { func, spec } => {
            render_expr_with_context(*func, renderer, context)?;
            render_window_spec(*spec, renderer, context)?;
            Ok(())
        }
        ExprNode::Exists { query, negated } => {
            if negated {
                renderer.sql.push_str("not exists (");
            } else {
                renderer.sql.push_str("exists (");
            }
            render_select(*query, renderer, ProjectionCastMode::Data)?;
            renderer.sql.push(')');
            Ok(())
        }
        ExprNode::Cast { expr, sql_type } => {
            renderer.sql.push_str("cast(");
            render_expr_with_context(*expr, renderer, context)?;
            renderer.sql.push_str(" as ");
            renderer.sql.push_str(sql_type);
            renderer.sql.push(')');
            Ok(())
        }
        ExprNode::DateTrunc { field, expr } => {
            renderer.sql.push_str("date_trunc(");
            render_sql_string_literal(field, renderer);
            renderer.sql.push_str(", ");
            render_expr_with_context(*expr, renderer, context)?;
            renderer.sql.push(')');
            Ok(())
        }
        ExprNode::Extract { field, expr } => {
            renderer.sql.push_str("extract(");
            renderer.sql.push_str(field);
            renderer.sql.push_str(" from ");
            render_expr_with_context(*expr, renderer, context)?;
            renderer.sql.push(')');
            Ok(())
        }
        ExprNode::Bool(value) => {
            renderer.sql.push_str(if value { "true" } else { "false" });
            Ok(())
        }
        ExprNode::Star => {
            renderer.sql.push('*');
            Ok(())
        }
    }
}

fn render_window_spec(
    spec: WindowSpec,
    renderer: &mut Renderer,
    context: ExprRenderContext,
) -> Result<()> {
    renderer.sql.push_str(" over (");
    let mut needs_space = false;

    if !spec.partition_by.is_empty() {
        renderer.sql.push_str("partition by ");
        for (index, expr) in spec.partition_by.into_iter().enumerate() {
            if index > 0 {
                renderer.sql.push_str(", ");
            }
            render_expr_with_context(expr, renderer, context)?;
        }
        needs_space = true;
    }

    if !spec.order_by.is_empty() {
        if needs_space {
            renderer.sql.push(' ');
        }
        renderer.sql.push_str("order by ");
        for (index, order) in spec.order_by.into_iter().enumerate() {
            if index > 0 {
                renderer.sql.push_str(", ");
            }
            render_expr_with_context(order.expr, renderer, context)?;
            renderer.sql.push(' ');
            renderer.sql.push_str(match order.direction {
                OrderDirection::Asc => "asc",
                OrderDirection::Desc => "desc",
            });
        }
        needs_space = true;
    }

    if let Some(frame) = spec.frame {
        if needs_space {
            renderer.sql.push(' ');
        }
        render_window_frame(frame, renderer);
    }

    renderer.sql.push(')');
    Ok(())
}

fn render_window_frame(frame: WindowFrame, renderer: &mut Renderer) {
    renderer.sql.push_str(match frame.unit {
        WindowFrameUnit::Rows => "rows",
        WindowFrameUnit::Range => "range",
        WindowFrameUnit::Groups => "groups",
    });
    renderer.sql.push_str(" between ");
    render_window_bound(frame.start, renderer);
    renderer.sql.push_str(" and ");
    render_window_bound(frame.end, renderer);
}

fn render_window_bound(bound: WindowFrameBound, renderer: &mut Renderer) {
    match bound {
        WindowFrameBound::UnboundedPreceding => renderer.sql.push_str("unbounded preceding"),
        WindowFrameBound::Preceding(n) => {
            renderer.sql.push_str(&n.to_string());
            renderer.sql.push_str(" preceding");
        }
        WindowFrameBound::CurrentRow => renderer.sql.push_str("current row"),
        WindowFrameBound::Following(n) => {
            renderer.sql.push_str(&n.to_string());
            renderer.sql.push_str(" following");
        }
        WindowFrameBound::UnboundedFollowing => renderer.sql.push_str("unbounded following"),
    }
}

fn render_sql_string_literal(value: &str, renderer: &mut Renderer) {
    renderer.sql.push('\'');
    for c in value.chars() {
        if c == '\'' {
            renderer.sql.push_str("''");
        } else {
            renderer.sql.push(c);
        }
    }
    renderer.sql.push('\'');
}

fn binary_op_sql(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Eq => "=",
        BinaryOp::Ne => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::Lte => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Gte => ">=",
        BinaryOp::And => "and",
        BinaryOp::Or => "or",
        BinaryOp::Like => "like",
        BinaryOp::ILike => "ilike",
        BinaryOp::JsonGet => "->",
        BinaryOp::JsonGetText => "->>",
        BinaryOp::JsonPath => "#>",
        BinaryOp::JsonPathText => "#>>",
        BinaryOp::Contains => "@>",
        BinaryOp::HasKey => "?",
        BinaryOp::Overlaps => "&&",
        BinaryOp::ArrayConcat => "||",
    }
}

fn arithmetic_op_sql(op: ArithmeticOp) -> &'static str {
    match op {
        ArithmeticOp::Add => "+",
        ArithmeticOp::Sub => "-",
        ArithmeticOp::Mul => "*",
        ArithmeticOp::Div => "/",
    }
}

fn array_quantifier_sql(quantifier: ArrayQuantifier) -> &'static str {
    match quantifier {
        ArrayQuantifier::Any => "any",
        ArrayQuantifier::All => "all",
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::*;

    #[allow(non_upper_case_globals)]
    mod users {
        use crate::prelude::*;

        pub const table: Table = Table::new("public", "users");
        pub const id: Field<i64, NotNull> = Field::new(table, "id");
        pub const email: Field<String, NotNull> = Field::new(table, "email");
        pub const display_name: Field<String, Nullable> = Field::new(table, "display_name");
        pub const active: Field<bool, NotNull> = Field::new(table, "active");
        pub const signup_rank: Field<i32, NotNull> = Field::new(table, "signup_rank");
        pub const oid: Field<u32, NotNull> = Field::new(table, "oid");
        pub const ratio: Field<f32, NotNull> = Field::new(table, "ratio");
        pub const created_at: Field<i64, NotNull> = Field::new(table, "created_at");
        pub const profile: Field<serde_json::Value, Nullable> = Field::new(table, "profile");
        pub const tags: Field<Vec<String>, NotNull> = Field::new(table, "tags");
        pub const oid_history: Field<Vec<u32>, NotNull> = Field::new(table, "oid_history");
        pub const status: Field<String, NotNull> =
            Field::new_with_pg_type_and_select_cast(table, "status", "user_status", "text");
        pub const optional_status: Field<String, Nullable> =
            Field::new_with_pg_type_and_select_cast(
                table,
                "optional_status",
                "user_status",
                "text",
            );
    }

    #[allow(non_upper_case_globals)]
    mod posts {
        use crate::prelude::*;

        pub const table: Table = Table::new("blog", "posts");
        pub const id: Field<i64, NotNull> = Field::new(table, "id");
        pub const user_id: Field<i64, NotNull> = Field::new(table, "user_id");
        pub const title: Field<String, NotNull> = Field::new(table, "title");
    }

    #[allow(non_upper_case_globals)]
    mod events {
        use crate::prelude::*;

        pub const table: Table = Table::new("public", "events");
        pub const occurred_at: Field<chrono::NaiveDateTime, NotNull> =
            Field::new(table, "occurred_at");
    }

    #[allow(non_upper_case_globals)]
    mod wide {
        use crate::prelude::*;

        pub const table: Table = Table::new("public", "wide");
        pub const c01: Field<i64, NotNull> = Field::new(table, "c01");
        pub const c02: Field<i64, NotNull> = Field::new(table, "c02");
        pub const c03: Field<i64, NotNull> = Field::new(table, "c03");
        pub const c04: Field<i64, NotNull> = Field::new(table, "c04");
        pub const c05: Field<i64, NotNull> = Field::new(table, "c05");
        pub const c06: Field<i64, NotNull> = Field::new(table, "c06");
        pub const c07: Field<i64, NotNull> = Field::new(table, "c07");
        pub const c08: Field<i64, NotNull> = Field::new(table, "c08");
        pub const c09: Field<i64, NotNull> = Field::new(table, "c09");
        pub const c10: Field<i64, NotNull> = Field::new(table, "c10");
        pub const c11: Field<i64, NotNull> = Field::new(table, "c11");
        pub const c12: Field<i64, NotNull> = Field::new(table, "c12");
        pub const c13: Field<i64, NotNull> = Field::new(table, "c13");
        pub const c14: Field<i64, NotNull> = Field::new(table, "c14");
        pub const c15: Field<i64, NotNull> = Field::new(table, "c15");
        pub const c16: Field<i64, NotNull> = Field::new(table, "c16");
    }

    #[test]
    fn renders_basic_select() {
        let query = Context::new()
            .select((users::id, users::email))
            .from(users::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            r#"select "users"."id", "users"."email" from "public"."users""#
        );
        assert_eq!(query.binds().len(), 0);
    }

    #[test]
    fn renders_projection_cast_for_enum_string_fields() {
        let query = Context::new()
            .select((
                users::status,
                users::status.as_("state"),
                nullable(users::optional_status),
            ))
            .from(users::table)
            .where_(users::status.eq(bind("active")))
            .order_by(users::status.asc())
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select cast("users"."status" as text), "#,
                r#"cast("users"."status" as text) as "state", "#,
                r#"cast("users"."optional_status" as text) from "public"."users" "#,
                r#"where ("users"."status" = $1) order by "users"."status" asc"#
            )
        );
        assert_eq!(query.binds().len(), 1);
    }

    #[test]
    fn renders_projection_cast_for_enum_string_returning_fields() {
        let query = Context::new()
            .insert_into(users::table)
            .values(users::status.set(bind("active")))
            .returning(users::status)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"insert into "public"."users" ("status") values ($1) "#,
                r#"returning cast("status" as text)"#
            )
        );
        assert_eq!(query.binds().len(), 1);
    }

    #[test]
    fn renders_sixteen_column_select() {
        let query = Context::new()
            .select((
                wide::c01,
                wide::c02,
                wide::c03,
                wide::c04,
                wide::c05,
                wide::c06,
                wide::c07,
                wide::c08,
                wide::c09,
                wide::c10,
                wide::c11,
                wide::c12,
                wide::c13,
                wide::c14,
                wide::c15,
                wide::c16,
            ))
            .from(wide::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "wide"."c01", "wide"."c02", "wide"."c03", "wide"."c04", "#,
                r#""wide"."c05", "wide"."c06", "wide"."c07", "wide"."c08", "#,
                r#""wide"."c09", "wide"."c10", "wide"."c11", "wide"."c12", "#,
                r#""wide"."c13", "wide"."c14", "wide"."c15", "wide"."c16" "#,
                r#"from "public"."wide""#
            )
        );
        assert_eq!(query.binds().len(), 0);
    }

    #[test]
    fn renders_distinct_select_snapshot() {
        let query = Context::new()
            .select(users::email)
            .distinct()
            .from(users::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            r#"select distinct "users"."email" from "public"."users""#
        );
    }

    #[test]
    fn renders_distinct_enum_string_order_by_with_projection_cast() {
        let query = Context::new()
            .select(users::status)
            .distinct()
            .from(users::table)
            .order_by(users::status.asc())
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select distinct cast("users"."status" as text) "#,
                r#"from "public"."users" order by cast("users"."status" as text) asc"#
            )
        );
    }

    #[test]
    fn renders_distinct_on_select_snapshot() {
        let query = Context::new()
            .select((users::email, users::created_at))
            .distinct_on((users::email,))
            .from(users::table)
            .order_by((users::email.asc(), users::created_at.desc()))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select distinct on ("users"."email") "users"."email", "#,
                r#""users"."created_at" from "public"."users" "#,
                r#"order by "users"."email" asc, "users"."created_at" desc"#
            )
        );
    }

    #[test]
    fn renders_group_by_and_having_select_snapshot() {
        let query = Context::new()
            .select((posts::user_id, count_star()))
            .from(posts::table)
            .group_by(posts::user_id)
            .having(count_star().gt(bind(1_i64)))
            .order_by(posts::user_id.asc())
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "posts"."user_id", count(*) from "blog"."posts" "#,
                r#"group by "posts"."user_id" having (count(*) > $1) "#,
                r#"order by "posts"."user_id" asc"#
            )
        );
        assert_eq!(query.binds().len(), 1);
    }

    #[test]
    fn renders_where_and_order_limit_offset() {
        let query = Context::new()
            .select((users::id, users::email))
            .from(users::table)
            .where_(
                users::active
                    .eq(bind(true))
                    .and(users::email.ilike(bind("%@example.com"))),
            )
            .order_by(users::created_at.desc())
            .limit(20)
            .offset(40)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."id", "users"."email" from "public"."users" "#,
                r#"where (("users"."active" = $1) and ("users"."email" ilike $2)) "#,
                r#"order by "users"."created_at" desc limit $3 offset $4"#
            )
        );
        assert_eq!(query.binds().len(), 4);
    }

    #[test]
    fn renders_jsonb_array_length_predicate() {
        let query = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(
                users::profile
                    .is_not_null()
                    .and(jsonb_array_length(users::profile).gt(bind(1_i32))),
            )
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."id" from "public"."users" "#,
                r#"where (("users"."profile" is not null) and "#,
                r#"(jsonb_array_length("users"."profile") > $1))"#
            )
        );
        assert_eq!(query.binds().len(), 1);
    }

    #[test]
    fn renders_jsonb_operators() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<serde_json::Value, Nullable>(users::profile.json_get("role"));
        assert_expr_type::<String, Nullable>(users::profile.json_get_text("role"));
        assert_expr_type::<serde_json::Value, Nullable>(
            users::profile.json_path(vec!["settings", "theme"]),
        );
        assert_expr_type::<String, Nullable>(
            users::profile.json_path_text(&["settings", "locale"][..]),
        );

        let query = Context::new()
            .select((
                users::profile.json_get("role"),
                users::profile.json_get_text("role"),
                users::profile.json_path(vec!["settings", "theme"]),
                users::profile.json_path_text(&["settings", "locale"][..]),
            ))
            .from(users::table)
            .where_(
                users::profile
                    .contains(serde_json::json!({ "role": "admin" }))
                    .and(users::profile.has_key("role")),
            )
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select ("users"."profile" -> $1), "#,
                r#"("users"."profile" ->> $2), "#,
                r#"("users"."profile" #> $3), "#,
                r#"("users"."profile" #>> $4) from "public"."users" "#,
                r#"where (("users"."profile" @> $5) and ("users"."profile" ? $6))"#
            )
        );
        assert_eq!(query.binds().len(), 6);
    }

    #[test]
    fn renders_array_operators_and_quantified_comparisons() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<Vec<String>, NotNull>(users::tags.concat(vec!["new"]));
        assert_expr_type::<Vec<String>, Nullable>(nullable(users::tags).concat(vec!["new"]));

        let query = Context::new()
            .select(users::tags.concat(vec!["vip"]))
            .from(users::table)
            .where_(
                users::tags
                    .contains(vec!["admin", "staff"])
                    .and(users::tags.overlaps(vec!["vip", "new"]))
                    .and(users::email.eq_any(vec!["ada@example.com", "ben@example.com"]))
                    .and(users::signup_rank.eq_all(vec![1_i32, 2_i32])),
            )
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select ("users"."tags" || $1) from "public"."users" "#,
                r#"where (((("users"."tags" @> $2) and "#,
                r#"("users"."tags" && $3)) and "#,
                r#"("users"."email" = any($4))) and "#,
                r#"("users"."signup_rank" = all($5)))"#
            )
        );
        assert_eq!(query.binds().len(), 5);
    }

    #[test]
    fn renders_oid_array_helpers_and_aggregate_type() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<Vec<u32>, Nullable>(array_agg(users::oid));

        let query = Context::new()
            .select(array_agg(users::oid))
            .from(users::table)
            .where_(
                users::oid
                    .eq_any(vec![1_u32, 2_u32])
                    .and(users::oid_history.contains(vec![1_u32]))
                    .and(users::oid_history.overlaps(vec![2_u32])),
            )
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select array_agg("users"."oid") from "public"."users" "#,
                r#"where ((("users"."oid" = any($1)) and "#,
                r#"("users"."oid_history" @> $2)) and "#,
                r#"("users"."oid_history" && $3))"#
            )
        );
        assert_eq!(query.binds().len(), 3);
    }

    #[test]
    fn renders_join() {
        let query = Context::new()
            .select((users::id, posts::title))
            .from(users::table)
            .join(posts::table.on(posts::user_id.eq(users::id)))
            .where_(posts::id.gt(bind(10_i64)))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."id", "posts"."title" from "public"."users" "#,
                r#"join "blog"."posts" on ("posts"."user_id" = "users"."id") "#,
                r#"where ("posts"."id" > $1)"#
            )
        );
    }

    #[test]
    fn renders_select_with_cte_and_outer_binds() {
        let active_users = Table::unqualified("active_users");
        let (active_user_id, active_user_email) = active_users.fields_of((users::id, users::email));

        let cte = Context::new()
            .select((users::id, users::email))
            .from(users::table)
            .where_(users::active.eq(bind(true)));

        let query = Context::new()
            .with("active_users", cte)
            .select((active_user_id, active_user_email))
            .from(active_users)
            .where_(active_user_id.gt(bind(10_i64)))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"with "active_users" as (select "users"."id", "users"."email" "#,
                r#"from "public"."users" where ("users"."active" = $1)) "#,
                r#"select "active_users"."id", "active_users"."email" "#,
                r#"from "active_users" where ("active_users"."id" > $2)"#
            )
        );
        assert_eq!(query.binds().len(), 2);
    }

    #[test]
    fn renders_chained_ctes_with_left_to_right_binds() {
        let ranked = Table::unqualified("ranked");
        let ranked_id = ranked.field_of(users::id);
        let recent = Table::unqualified("recent");
        let recent_id = recent.field_of(ranked_id);

        let ranked_query = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::signup_rank.gt(bind(10_i32)));
        let recent_query = Context::new()
            .select(ranked_id)
            .from(ranked)
            .where_(ranked_id.lt(bind(100_i64)));

        let query = Context::new()
            .with("ranked", ranked_query)
            .with("recent", recent_query)
            .select(recent_id)
            .from(recent)
            .where_(recent_id.ne(bind(7_i64)))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"with "ranked" as (select "users"."id" from "public"."users" "#,
                r#"where ("users"."signup_rank" > $1)), "#,
                r#""recent" as (select "ranked"."id" from "ranked" "#,
                r#"where ("ranked"."id" < $2)) "#,
                r#"select "recent"."id" from "recent" where ("recent"."id" <> $3)"#
            )
        );
        assert_eq!(query.binds().len(), 3);
    }

    #[test]
    fn renders_aliased_subquery_as_from_source() {
        let subquery = Context::new()
            .select((users::id, users::email))
            .from(users::table)
            .where_(users::active.eq(bind(true)))
            .alias("u");
        let (subquery_id, subquery_email) = subquery.fields_of((users::id, users::email));

        let query = Context::new()
            .select((subquery_id, subquery_email))
            .from(subquery)
            .where_(subquery_id.gt(bind(5_i64)))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "u"."id", "u"."email" from "#,
                r#"(select "users"."id", "users"."email" from "public"."users" "#,
                r#"where ("users"."active" = $1)) as "u" "#,
                r#"where ("u"."id" > $2)"#
            )
        );
        assert_eq!(query.binds().len(), 2);
    }

    #[test]
    fn aliased_subquery_field_of_accepts_casted_enum_string_field() {
        let subquery = Context::new()
            .select(users::status)
            .from(users::table)
            .alias("u");
        let projected_status = subquery.field_of(users::status);

        let query = Context::new()
            .select(projected_status)
            .from(subquery)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select cast("u"."status" as text) from "#,
                r#"(select "users"."status" from "public"."users") as "u""#
            )
        );
    }

    #[test]
    fn renders_aliased_subquery_as_join_source() {
        let post_counts = Context::new()
            .select((posts::user_id, count_star()))
            .from(posts::table)
            .group_by(posts::user_id)
            .having(count_star().gt(bind(1_i64)))
            .alias("pc");
        let post_counts_user_id = post_counts.field_of(posts::user_id);

        let query = Context::new()
            .select(users::id)
            .from(users::table)
            .join(post_counts.on(post_counts_user_id.eq(users::id)))
            .where_(users::active.eq(bind(true)))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."id" from "public"."users" join "#,
                r#"(select "posts"."user_id", count(*) from "blog"."posts" "#,
                r#"group by "posts"."user_id" having (count(*) > $1)) as "pc" "#,
                r#"on ("pc"."user_id" = "users"."id") "#,
                r#"where ("users"."active" = $2)"#
            )
        );
        assert_eq!(query.binds().len(), 2);
    }

    #[test]
    #[should_panic(
        expected = "field_of source field `email` is not selected by subquery alias `u`"
    )]
    fn aliased_subquery_field_of_rejects_unselected_source_field() {
        let subquery = Context::new()
            .select(users::id)
            .from(users::table)
            .alias("u");

        let _ = subquery.field_of(users::email);
    }

    #[test]
    #[should_panic(expected = "field_of source field `id` is not selected by subquery alias `u`")]
    fn aliased_subquery_field_of_rejects_renamed_source_field() {
        let subquery = Context::new()
            .select(users::id.as_("uid"))
            .from(users::table)
            .alias("u");

        let _ = subquery.field_of(users::id);
    }

    #[test]
    fn aliased_subquery_field_can_access_renamed_source_field_explicitly() {
        let subquery = Context::new()
            .select(users::id.as_("uid"))
            .from(users::table)
            .alias("u");
        let uid: Field<i64, NotNull> = subquery.field("uid");

        let query = Context::new().select(uid).from(subquery).render().unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "u"."uid" from "#,
                r#"(select "users"."id" as "uid" from "public"."users") as "u""#
            )
        );
    }

    #[test]
    #[should_panic(expected = "field_of source field `id` is not selected by subquery alias `u`")]
    fn aliased_subquery_field_of_rejects_same_named_source_field_from_another_table() {
        let accounts = Table::new("public", "accounts");
        let account_id: Field<String, Nullable> = accounts.field("id");
        let subquery = Context::new()
            .select(users::id)
            .from(users::table)
            .alias("u");

        let _ = subquery.field_of(account_id);
    }

    #[test]
    fn aliased_subquery_field_of_preserves_source_nullability_after_left_join() {
        fn assert_field_type<T, N>(_: Field<T, N>) {}

        let subquery = Context::new()
            .select(nullable(posts::title))
            .from(users::table)
            .left_join(posts::table.on(posts::user_id.eq(users::id)))
            .alias("p");
        let projected_title = subquery.field_of(posts::title);

        assert_field_type::<String, NotNull>(projected_title);
    }

    #[test]
    fn renders_left_join_with_explicit_nullable_selection() {
        let query = Context::new()
            .select((users::id, nullable(posts::title)))
            .from(users::table)
            .left_join(posts::table.on(posts::user_id.eq(users::id)))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."id", "posts"."title" from "public"."users" "#,
                r#"left join "blog"."posts" on ("posts"."user_id" = "users"."id")"#
            )
        );
    }

    #[test]
    fn renders_insert_returning() {
        let query = Context::new()
            .insert_into(users::table)
            .values((
                users::email.set(bind("a@example.com")),
                users::active.set(bind(true)),
            ))
            .returning(users::id)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"insert into "public"."users" ("email", "active") values ($1, $2) "#,
                r#"returning "id""#
            )
        );
        assert_eq!(query.binds().len(), 2);
    }

    #[test]
    fn renders_multi_row_insert() {
        let query = Context::new()
            .insert_into(users::table)
            .values_many([
                (
                    users::email.set(bind("a@example.com")),
                    users::active.set(bind(true)),
                ),
                (
                    users::email.set(bind("b@example.com")),
                    users::active.set(bind(false)),
                ),
            ])
            .returning(users::id)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"insert into "public"."users" ("email", "active") "#,
                r#"values ($1, $2), ($3, $4) returning "id""#
            )
        );
        assert_eq!(query.binds().len(), 4);
    }

    #[test]
    fn multi_row_insert_requires_matching_fields() {
        let result = Context::new()
            .insert_into(users::table)
            .values_many([
                (
                    users::email.set(bind("a@example.com")),
                    users::active.set(bind(true)),
                ),
                (
                    users::active.set(bind(false)),
                    users::email.set(bind("b@example.com")),
                ),
            ])
            .render();

        assert!(
            matches!(result, Err(Error::InvalidQueryShape(message)) if message.contains("same fields"))
        );
    }

    #[test]
    fn empty_multi_row_insert_is_invalid_query_shape() {
        let result = Context::new()
            .insert_into(users::table)
            .values_many(Vec::<Assignment>::new())
            .render();

        assert!(
            matches!(result, Err(Error::InvalidQueryShape(message)) if message.contains("INSERT requires"))
        );
    }

    #[test]
    fn renders_insert_on_conflict_do_nothing() {
        let query = Context::new()
            .insert_into(users::table)
            .values((
                users::email.set(bind("a@example.com")),
                users::active.set(bind(true)),
            ))
            .on_conflict((users::email,))
            .do_nothing()
            .returning(users::id)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"insert into "public"."users" ("email", "active") values ($1, $2) "#,
                r#"on conflict ("email") do nothing returning "id""#
            )
        );
        assert_eq!(query.binds().len(), 2);
    }

    #[test]
    fn renders_sixteen_column_conflict_target() {
        let query = Context::new()
            .insert_into(wide::table)
            .values(wide::c01.set(bind(1_i64)))
            .on_conflict((
                wide::c01,
                wide::c02,
                wide::c03,
                wide::c04,
                wide::c05,
                wide::c06,
                wide::c07,
                wide::c08,
                wide::c09,
                wide::c10,
                wide::c11,
                wide::c12,
                wide::c13,
                wide::c14,
                wide::c15,
                wide::c16,
            ))
            .do_nothing()
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"insert into "public"."wide" ("c01") values ($1) "#,
                r#"on conflict ("c01", "c02", "c03", "c04", "c05", "c06", "#,
                r#""c07", "c08", "c09", "c10", "c11", "c12", "c13", "c14", "#,
                r#""c15", "c16") do nothing"#
            )
        );
        assert_eq!(query.binds().len(), 1);
    }

    #[test]
    fn renders_insert_on_conflict_do_update_with_excluded() {
        let query = Context::new()
            .insert_into(users::table)
            .values((
                users::email.set(bind("a@example.com")),
                users::display_name.set(bind(Some("Ada"))),
                users::active.set(bind(true)),
            ))
            .on_conflict((users::email,))
            .do_update(|excluded| {
                (
                    users::display_name.set(excluded.field(users::display_name)),
                    users::active.set(excluded.field(users::active)),
                )
            })
            .returning((users::id, users::display_name))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"insert into "public"."users" ("email", "display_name", "active") "#,
                r#"values ($1, $2, $3) on conflict ("email") do update set "#,
                r#""display_name" = excluded."display_name", "#,
                r#""active" = excluded."active" returning "id", "display_name""#
            )
        );
        assert_eq!(query.binds().len(), 3);
    }

    #[test]
    fn renders_aliased_insert_on_conflict_do_update_target_field_refs() {
        let query = Context::new()
            .insert_into(users::table.as_("u"))
            .values((
                users::email.set(bind("a@example.com")),
                users::signup_rank.set(bind(1_i32)),
            ))
            .on_conflict((users::email,))
            .do_update(|excluded| {
                (
                    users::signup_rank.set(users::signup_rank.expr() + bind(1_i32)),
                    users::display_name.set(excluded.field(users::display_name)),
                )
            })
            .returning((users::id, users::signup_rank))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"insert into "public"."users" as "u" ("email", "signup_rank") "#,
                r#"values ($1, $2) on conflict ("email") do update set "#,
                r#""signup_rank" = ("u"."signup_rank" + $3), "#,
                r#""display_name" = excluded."display_name" returning "id", "signup_rank""#
            )
        );
        assert_eq!(query.binds().len(), 3);
    }

    #[test]
    fn insert_on_conflict_do_update_rejects_excluded_field_from_other_table() {
        let result = Context::new()
            .insert_into(users::table)
            .values((
                users::id.set(bind(1_i64)),
                users::email.set(bind("a@example.com")),
            ))
            .on_conflict((users::email,))
            .do_update(|excluded| users::id.set(excluded.field(posts::id)))
            .render();

        assert!(matches!(
            result,
            Err(Error::InvalidQueryShape(message))
                if message.contains("excluded field id")
                    && message.contains("target table users")
        ));
    }

    #[test]
    fn excluded_field_outside_on_conflict_do_update_is_invalid_query_shape() {
        let result = Context::new()
            .select(Excluded.field(users::id))
            .from(users::table)
            .render();

        assert!(matches!(
            result,
            Err(Error::InvalidQueryShape(message))
                if message.contains("excluded fields")
                    && message.contains("ON CONFLICT DO UPDATE")
        ));
    }

    #[test]
    fn renders_sixteen_field_update() {
        let query = Context::new()
            .update(wide::table)
            .set((
                wide::c01.set(bind(1_i64)),
                wide::c02.set(bind(2_i64)),
                wide::c03.set(bind(3_i64)),
                wide::c04.set(bind(4_i64)),
                wide::c05.set(bind(5_i64)),
                wide::c06.set(bind(6_i64)),
                wide::c07.set(bind(7_i64)),
                wide::c08.set(bind(8_i64)),
                wide::c09.set(bind(9_i64)),
                wide::c10.set(bind(10_i64)),
                wide::c11.set(bind(11_i64)),
                wide::c12.set(bind(12_i64)),
                wide::c13.set(bind(13_i64)),
                wide::c14.set(bind(14_i64)),
                wide::c15.set(bind(15_i64)),
                wide::c16.set(bind(16_i64)),
            ))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"update "public"."wide" set "c01" = $1, "c02" = $2, "#,
                r#""c03" = $3, "c04" = $4, "c05" = $5, "c06" = $6, "#,
                r#""c07" = $7, "c08" = $8, "c09" = $9, "c10" = $10, "#,
                r#""c11" = $11, "c12" = $12, "c13" = $13, "c14" = $14, "#,
                r#""c15" = $15, "c16" = $16"#
            )
        );
        assert_eq!(query.binds().len(), 16);
    }

    #[test]
    fn renders_update_returning() {
        let query = Context::new()
            .update(users::table)
            .set(users::email.set(bind("new@example.com")))
            .where_(users::id.eq(bind(7_i64)))
            .returning((users::id, users::email))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"update "public"."users" set "email" = $1 "#,
                r#"where ("users"."id" = $2) returning "id", "email""#
            )
        );
    }

    #[test]
    fn renders_delete_returning() {
        let query = Context::new()
            .delete_from(users::table)
            .where_(users::id.eq(bind(7_i64)))
            .returning(users::id)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            r#"delete from "public"."users" where ("users"."id" = $1) returning "id""#
        );
    }

    #[test]
    fn bind_numbering_is_left_to_right() {
        let query = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::id.gt(bind(1_i64)).and(users::id.lt(bind(10_i64))))
            .limit(5)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."id" from "public"."users" "#,
                r#"where (("users"."id" > $1) and ("users"."id" < $2)) limit $3"#
            )
        );
        assert_eq!(query.binds().len(), 3);
    }

    #[test]
    fn renders_in_and_not_in_predicates() {
        let query =
            Context::new()
                .select(users::id)
                .from(users::table)
                .where_(users::id.in_([bind(1_i64), bind(2_i64)]).and(
                    users::email.not_in([bind("blocked@example.com"), bind("spam@example.com")]),
                ))
                .render()
                .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."id" from "public"."users" "#,
                r#"where (("users"."id" in ($1, $2)) and "#,
                r#"("users"."email" not in ($3, $4)))"#
            )
        );
        assert_eq!(query.binds().len(), 4);
    }

    #[test]
    fn renders_in_and_not_in_subqueries() {
        let matching_posts = Context::new()
            .select(posts::user_id)
            .from(posts::table)
            .where_(posts::id.gt(bind(10_i64)));
        let blocked_posts = Context::new()
            .select(posts::user_id)
            .from(posts::table)
            .where_(posts::title.ilike(bind("%spam%")));

        let query = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(
                users::id
                    .in_(matching_posts)
                    .and(users::id.not_in(blocked_posts))
                    .and(users::active.eq(bind(true))),
            )
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."id" from "public"."users" where "#,
                r#"((("users"."id" in (select "posts"."user_id" from "blog"."posts" "#,
                r#"where ("posts"."id" > $1))) and "#,
                r#"("users"."id" not in (select "posts"."user_id" from "blog"."posts" "#,
                r#"where ("posts"."title" ilike $2)))) and "#,
                r#"("users"."active" = $3))"#
            )
        );
        assert_eq!(query.binds().len(), 3);
    }

    #[test]
    fn renders_between_and_not_between_predicates() {
        let query = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(
                users::created_at
                    .between(bind(10_i64), bind(20_i64))
                    .and(users::signup_rank.not_between(bind(1_i32), bind(5_i32))),
            )
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."id" from "public"."users" "#,
                r#"where (("users"."created_at" between $1 and $2) and "#,
                r#"("users"."signup_rank" not between $3 and $4))"#
            )
        );
        assert_eq!(query.binds().len(), 4);
    }

    #[test]
    fn renders_arithmetic_expressions() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<i32, NotNull>(users::signup_rank.expr() + bind(1_i32));
        assert_expr_type::<i32, Nullable>(nullable(users::signup_rank) + bind(1_i32));

        let query = Context::new()
            .select((
                users::signup_rank.expr() + bind(1_i32),
                users::signup_rank.expr() - bind(2_i32),
                users::signup_rank.expr() * bind(3_i32),
                users::signup_rank.expr() / bind(4_i32),
            ))
            .from(users::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select ("users"."signup_rank" + $1), "#,
                r#"("users"."signup_rank" - $2), "#,
                r#"("users"."signup_rank" * $3), "#,
                r#"("users"."signup_rank" / $4) from "public"."users""#
            )
        );
        assert_eq!(query.binds().len(), 4);
    }

    #[test]
    fn renders_concat_coalesce_and_nullif() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<String, NotNull>(concat(users::email, bind("!")));
        assert_expr_type::<String, Nullable>(concat(users::display_name, users::email));
        assert_expr_type::<String, NotNull>(coalesce((users::display_name, users::email)));
        assert_expr_type::<String, Nullable>(nullif(users::email, bind("")));
        assert_expr_type::<i64, NotNull>(coalesce((
            wide::c01,
            wide::c02,
            wide::c03,
            wide::c04,
            wide::c05,
            wide::c06,
            wide::c07,
            wide::c08,
            wide::c09,
            wide::c10,
            wide::c11,
            wide::c12,
            wide::c13,
            wide::c14,
            wide::c15,
            wide::c16,
        )));

        let query = Context::new()
            .select((
                concat(users::email, bind("!")),
                coalesce((users::display_name, users::email)),
                nullif(users::email, bind("")),
            ))
            .from(users::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select ("users"."email" || $1), "#,
                r#"coalesce("users"."display_name", "users"."email"), "#,
                r#"nullif("users"."email", $2) from "public"."users""#
            )
        );
        assert_eq!(query.binds().len(), 2);
    }

    #[test]
    fn renders_case_when_with_else() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<String, NotNull>(
            case_when()
                .when(users::active.eq(bind(true)), bind("active"))
                .else_(bind("inactive")),
        );

        let query = Context::new()
            .select(
                case_when()
                    .when(users::active.eq(bind(true)), bind("active"))
                    .when(users::signup_rank.gt(bind(10_i32)), bind("ranked"))
                    .else_(bind("inactive")),
            )
            .from(users::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select (case when ("users"."active" = $1) then $2 "#,
                r#"when ("users"."signup_rank" > $3) then $4 else $5 end) "#,
                r#"from "public"."users""#
            )
        );
        assert_eq!(query.binds().len(), 5);
    }

    #[test]
    fn renders_case_when_without_else_as_nullable() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<String, Nullable>(
            case_when()
                .when(users::active.eq(bind(true)), bind("active"))
                .end(),
        );

        let query = Context::new()
            .select(
                case_when()
                    .when(users::active.eq(bind(true)), bind("active"))
                    .end(),
            )
            .from(users::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select (case when ("users"."active" = $1) then $2 end) "#,
                r#"from "public"."users""#
            )
        );
        assert_eq!(query.binds().len(), 2);
    }

    #[test]
    fn empty_in_list_is_invalid_query_shape() {
        let result = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::id.in_(Vec::<Expr<i64, NotNull>>::new()))
            .render();

        assert!(
            matches!(result, Err(Error::InvalidQueryShape(message)) if message.contains("IN predicate"))
        );
    }

    #[test]
    fn sum_uses_postgres_output_types() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<i64, Nullable>(sum(users::signup_rank));
        assert_expr_type::<rust_decimal::Decimal, Nullable>(sum(users::created_at));

        let query = Context::new()
            .select(sum(users::signup_rank))
            .from(users::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            r#"select sum("users"."signup_rank") from "public"."users""#
        );
    }

    #[test]
    fn renders_row_number_with_partition_and_order() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<i64, NotNull>(
            row_number().over(partition_by(posts::user_id).order_by(posts::id.desc())),
        );

        let query = Context::new()
            .select((
                posts::id,
                row_number()
                    .over(partition_by(posts::user_id).order_by(posts::id.desc()))
                    .as_("rn"),
            ))
            .from(posts::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "posts"."id", row_number() over ("#,
                r#"partition by "posts"."user_id" order by "posts"."id" desc) as "rn" "#,
                r#"from "blog"."posts""#
            )
        );
    }

    #[test]
    fn renders_running_total_with_rows_frame() {
        let query = Context::new()
            .select((
                users::id,
                sum(users::signup_rank)
                    .over(
                        partition_by(users::active)
                            .order_by(users::created_at.asc())
                            .rows_between(unbounded_preceding(), current_row()),
                    )
                    .as_("running_total"),
            ))
            .from(users::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."id", sum("users"."signup_rank") over ("#,
                r#"partition by "users"."active" order by "users"."created_at" asc "#,
                r#"rows between unbounded preceding and current row) as "running_total" "#,
                r#"from "public"."users""#
            )
        );
    }

    #[test]
    fn renders_lag_lead_first_value_last_value_ntile() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<i32, Nullable>(
            lag(users::signup_rank).over(WindowSpec::new().order_by(users::id.asc())),
        );
        assert_expr_type::<i32, Nullable>(
            lead(users::signup_rank).over(WindowSpec::new().order_by(users::id.asc())),
        );
        assert_expr_type::<i32, Nullable>(
            first_value(users::signup_rank).over(WindowSpec::new().order_by(users::id.asc())),
        );
        assert_expr_type::<i32, Nullable>(
            last_value(users::signup_rank).over(WindowSpec::new().order_by(users::id.asc())),
        );
        assert_expr_type::<i32, NotNull>(
            ntile(4).over(WindowSpec::new().order_by(users::id.asc())),
        );

        let query = Context::new()
            .select((
                lag(users::signup_rank)
                    .over(WindowSpec::new().order_by(users::id.asc()))
                    .as_("prev"),
                lead(users::signup_rank)
                    .over(WindowSpec::new().order_by(users::id.asc()))
                    .as_("next"),
                ntile(4)
                    .over(WindowSpec::new().order_by(users::id.asc()))
                    .as_("quartile"),
            ))
            .from(users::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select lag("users"."signup_rank") over ("#,
                r#"order by "users"."id" asc) as "prev", "#,
                r#"lead("users"."signup_rank") over ("#,
                r#"order by "users"."id" asc) as "next", "#,
                r#"ntile($1) over ("#,
                r#"order by "users"."id" asc) as "quartile" from "public"."users""#
            )
        );
        assert_eq!(query.binds().len(), 1);
    }

    #[test]
    fn renders_exists_and_not_exists_predicates() {
        let inner = Context::new()
            .select(posts::id)
            .from(posts::table)
            .where_(posts::user_id.eq(users::id));

        let query = Context::new()
            .select(users::email)
            .from(users::table)
            .where_(exists(inner))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."email" from "public"."users" "#,
                r#"where exists (select "posts"."id" from "blog"."posts" "#,
                r#"where ("posts"."user_id" = "users"."id"))"#
            )
        );

        let inner = Context::new()
            .select(posts::id)
            .from(posts::table)
            .where_(posts::user_id.eq(users::id));

        let query = Context::new()
            .select(users::email)
            .from(users::table)
            .where_(not_exists(inner))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."email" from "public"."users" "#,
                r#"where not exists (select "posts"."id" from "blog"."posts" "#,
                r#"where ("posts"."user_id" = "users"."id"))"#
            )
        );
    }

    #[test]
    fn renders_cast_extract_and_date_trunc_helpers() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<String, NotNull>(cast::<i32, String, _>(users::signup_rank));
        assert_expr_type::<rust_decimal::Decimal, NotNull>(extract("year", events::occurred_at));
        assert_expr_type::<chrono::NaiveDateTime, NotNull>(date_trunc("day", events::occurred_at));

        let query = Context::new()
            .select((
                cast::<i32, String, _>(users::signup_rank).as_("rank_str"),
                extract("year", events::occurred_at).as_("year"),
                date_trunc("day", events::occurred_at).as_("day"),
            ))
            .from(users::table)
            .cross_join(events::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select cast("users"."signup_rank" as text) as "rank_str", "#,
                r#"extract(year from "events"."occurred_at") as "year", "#,
                r#"date_trunc('day', "events"."occurred_at") as "day" "#,
                r#"from "public"."users" cross join "public"."events""#
            )
        );
        assert_eq!(query.binds().len(), 0);
    }

    #[test]
    fn renders_reused_date_trunc_group_by_without_duplicate_binds() {
        let bucket = date_trunc("day", events::occurred_at);

        let query = Context::new()
            .select((bucket.clone().as_("day"), count_star()))
            .from(events::table)
            .group_by(bucket)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select date_trunc('day', "events"."occurred_at") as "day", count(*) "#,
                r#"from "public"."events" "#,
                r#"group by date_trunc('day', "events"."occurred_at")"#
            )
        );
        assert_eq!(query.binds().len(), 0);
    }

    #[test]
    fn renders_greatest_least_length_lower_upper_trim_now() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<i32, NotNull>(greatest((users::signup_rank, users::signup_rank)));
        assert_expr_type::<i32, NotNull>(least((users::signup_rank, users::signup_rank)));
        assert_expr_type::<i32, NotNull>(length(users::email));
        assert_expr_type::<String, NotNull>(lower(users::email));
        assert_expr_type::<String, NotNull>(upper(users::email));
        assert_expr_type::<String, NotNull>(trim(users::email));

        let query = Context::new()
            .select((
                greatest((users::signup_rank, users::signup_rank)).as_("g"),
                least((users::signup_rank, users::signup_rank)).as_("l"),
                length(users::email).as_("len"),
                lower(users::email).as_("lo"),
                upper(users::email).as_("up"),
                trim(users::email).as_("tr"),
                now().as_("ts"),
            ))
            .from(users::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select greatest("users"."signup_rank", "users"."signup_rank") as "g", "#,
                r#"least("users"."signup_rank", "users"."signup_rank") as "l", "#,
                r#"length("users"."email") as "len", "#,
                r#"lower("users"."email") as "lo", "#,
                r#"upper("users"."email") as "up", "#,
                r#"trim("users"."email") as "tr", "#,
                r#"now() as "ts" from "public"."users""#
            )
        );
    }

    #[test]
    fn renders_right_full_cross_joins_and_lateral() {
        let query = Context::new()
            .select((users::id, posts::title))
            .from(users::table)
            .right_join(posts::table.on(posts::user_id.eq(users::id)))
            .full_join(posts::table.on(posts::user_id.eq(users::id)))
            .cross_join(posts::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."id", "posts"."title" from "public"."users" "#,
                r#"right join "blog"."posts" on ("posts"."user_id" = "users"."id") "#,
                r#"full join "blog"."posts" on ("posts"."user_id" = "users"."id") "#,
                r#"cross join "blog"."posts""#
            )
        );

        let lateral_subquery = Context::new()
            .select(posts::title)
            .from(posts::table)
            .where_(posts::user_id.eq(users::id))
            .order_by(posts::id.desc())
            .limit(1)
            .alias("latest");
        let latest_title = lateral_subquery.field_of(posts::title);

        let query = Context::new()
            .select((users::id, latest_title))
            .from(users::table)
            .left_join_lateral(lateral_subquery.on(bind(true).eq(bind(true))))
            .render()
            .unwrap();

        assert!(query.sql().contains("left join lateral"));

        let cross_lateral = Context::new()
            .select(posts::title)
            .from(posts::table)
            .where_(posts::user_id.eq(users::id))
            .alias("latest");
        let cross_title = cross_lateral.field_of(posts::title);

        let query = Context::new()
            .select((users::id, cross_title))
            .from(users::table)
            .cross_join_lateral(cross_lateral)
            .render()
            .unwrap();

        assert!(query.sql().contains("cross join lateral"));
    }

    #[test]
    fn renders_union_union_all_except_intersect() {
        let q1 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::active.eq(bind(true)));
        let q2 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::signup_rank.gt(bind(10_i32)));

        let union = q1.union(q2).render().unwrap();
        assert_eq!(
            union.sql(),
            concat!(
                r#"(select "users"."id" from "public"."users" "#,
                r#"where ("users"."active" = $1)) union "#,
                r#"(select "users"."id" from "public"."users" "#,
                r#"where ("users"."signup_rank" > $2))"#
            )
        );

        let q1 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::active.eq(bind(true)));
        let q2 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::signup_rank.gt(bind(10_i32)));
        let union_all = q1
            .union_all(q2)
            .order_by(users::id.asc())
            .limit(20)
            .render()
            .unwrap();
        assert!(union_all.sql().contains("union all"));
        assert!(union_all.sql().ends_with("limit $3"));

        let q1 = Context::new().select(users::id).from(users::table);
        let q2 = Context::new().select(users::id).from(users::table);
        let except = q1.except(q2).render().unwrap();
        assert!(except.sql().contains("except"));

        let q1 = Context::new().select(users::id).from(users::table);
        let q2 = Context::new().select(users::id).from(users::table);
        let intersect = q1.intersect_all(q2).render().unwrap();
        assert!(intersect.sql().contains("intersect all"));
    }

    #[test]
    fn set_operations_preserve_left_operand_tail_clauses() {
        let q1 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::active.eq(bind(true)))
            .order_by(users::id.desc())
            .limit(1)
            .offset(2);
        let q2 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::signup_rank.gt(bind(10_i32)));

        let query = q1.union(q2).render().unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"(select "users"."id" from "public"."users" "#,
                r#"where ("users"."active" = $1) order by "users"."id" desc "#,
                r#"limit $2 offset $3) union "#,
                r#"(select "users"."id" from "public"."users" "#,
                r#"where ("users"."signup_rank" > $4))"#
            )
        );
        assert_eq!(query.binds().len(), 4);
    }

    #[test]
    fn set_operation_outer_tail_still_applies_to_combined_set() {
        let q1 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::active.eq(bind(true)));
        let q2 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::signup_rank.gt(bind(10_i32)));

        let query = q1
            .union(q2)
            .order_by(users::id.asc())
            .limit(20)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"(select "users"."id" from "public"."users" "#,
                r#"where ("users"."active" = $1)) union "#,
                r#"(select "users"."id" from "public"."users" "#,
                r#"where ("users"."signup_rank" > $2)) "#,
                r#"order by "id" asc limit $3"#
            )
        );
        assert_eq!(query.binds().len(), 3);
    }

    #[test]
    fn set_operation_left_distinct_enum_order_by_uses_projection_cast() {
        let q1 = Context::new()
            .select(users::status)
            .distinct()
            .from(users::table)
            .order_by(users::status.asc());
        let q2 = Context::new().select(users::status).from(users::table);

        let query = q1
            .union(q2)
            .order_by(users::status.desc())
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"(select distinct cast("users"."status" as text) "#,
                r#"from "public"."users" "#,
                r#"order by cast("users"."status" as text) asc) union "#,
                r#"(select cast("users"."status" as text) from "public"."users") "#,
                r#"order by "status" desc"#
            )
        );
    }

    #[test]
    fn renders_mixed_set_operations_left_associative() {
        let q1 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::id.eq(bind(1_i64)));
        let q2 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::id.eq(bind(2_i64)));
        let q3 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::id.eq(bind(3_i64)));

        let query = q1.union(q2).intersect(q3).render().unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"((select "users"."id" from "public"."users" "#,
                r#"where ("users"."id" = $1)) union "#,
                r#"(select "users"."id" from "public"."users" "#,
                r#"where ("users"."id" = $2))) intersect "#,
                r#"(select "users"."id" from "public"."users" "#,
                r#"where ("users"."id" = $3))"#
            )
        );
        assert_eq!(query.binds().len(), 3);
    }

    #[test]
    fn set_operations_preserve_tail_before_next_set_operation() {
        let q1 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::id.eq(bind(1_i64)));
        let q2 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::id.eq(bind(2_i64)));
        let q3 = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::id.eq(bind(3_i64)));

        let query = q1
            .union(q2)
            .order_by(users::id.asc())
            .limit(2)
            .intersect(q3)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"((select "users"."id" from "public"."users" "#,
                r#"where ("users"."id" = $1)) union "#,
                r#"(select "users"."id" from "public"."users" "#,
                r#"where ("users"."id" = $2)) order by "id" asc limit $3) intersect "#,
                r#"(select "users"."id" from "public"."users" "#,
                r#"where ("users"."id" = $4))"#
            )
        );
        assert_eq!(query.binds().len(), 4);
    }

    #[test]
    fn renders_for_update_and_skip_locked() {
        let query = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::active.eq(bind(true)))
            .order_by(users::id.asc())
            .limit(1)
            .for_update()
            .skip_locked()
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."id" from "public"."users" "#,
                r#"where ("users"."active" = $1) "#,
                r#"order by "users"."id" asc limit $2 for update skip locked"#
            )
        );

        let query = Context::new()
            .select(users::id)
            .from(users::table)
            .for_share()
            .no_wait()
            .render()
            .unwrap();

        assert!(query.sql().contains("for share"));
        assert!(query.sql().contains("nowait"));

        let query = Context::new()
            .select(users::id)
            .from(users::table)
            .for_no_key_update()
            .of(users::table)
            .render()
            .unwrap();

        assert!(query.sql().contains("for no key update"));
        assert!(query.sql().contains(r#"of "users""#));

        let query = Context::new()
            .select(users::id)
            .from(users::table.as_("u"))
            .for_update()
            .of(users::table.as_("u"))
            .render()
            .unwrap();

        assert!(query.sql().contains(r#"from "public"."users" as "u""#));
        assert!(query.sql().contains(r#"for update of "u""#));

        let query = Context::new()
            .select(users::id)
            .from(users::table)
            .for_key_share()
            .render()
            .unwrap();
        assert!(query.sql().contains("for key share"));
    }

    #[test]
    fn renders_insert_select_form() {
        let source = Context::new()
            .select((users::id, users::email))
            .from(users::table)
            .where_(users::active.eq(bind(true)));

        let query = Context::new()
            .insert_into(users::table)
            .columns((users::id, users::email))
            .from_select(source)
            .returning(users::id)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"insert into "public"."users" ("id", "email") "#,
                r#"select "users"."id", "users"."email" from "public"."users" "#,
                r#"where ("users"."active" = $1) returning "id""#
            )
        );
        assert_eq!(query.binds().len(), 1);
    }

    #[test]
    fn insert_select_preserves_enum_field_type_in_source_projection() {
        let source = Context::new()
            .select(users::status)
            .from(users::table)
            .where_(users::active.eq(bind(true)));

        let query = Context::new()
            .insert_into(users::table)
            .columns((users::status,))
            .from_select(source)
            .returning(users::status)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"insert into "public"."users" ("status") "#,
                r#"select "users"."status" from "public"."users" "#,
                r#"where ("users"."active" = $1) returning cast("status" as text)"#
            )
        );
        assert_eq!(query.binds().len(), 1);
    }

    #[test]
    fn insert_select_requires_matching_column_count() {
        let source = Context::new().select(users::id).from(users::table);
        let result = Context::new()
            .insert_into(users::table)
            .columns((users::id, users::email))
            .from_select(source)
            .render();

        assert!(matches!(
            result,
            Err(Error::InvalidQueryShape(message))
                if message.contains("column count mismatch")
                    && message.contains("2 target columns")
                    && message.contains("1 selected expressions")
        ));
    }

    #[test]
    fn insert_select_without_columns_is_invalid_query_shape() {
        let source = Context::new().select(users::id).from(users::table);
        let result = Context::new()
            .insert_into(users::table)
            .from_select(source)
            .render();

        assert!(matches!(
            result,
            Err(Error::InvalidQueryShape(message))
                if message.contains("requires .columns")
        ));
    }

    #[test]
    fn insert_select_combined_with_values_is_invalid_query_shape() {
        let source = Context::new().select(users::id).from(users::table);
        let result = Context::new()
            .insert_into(users::table)
            .values(users::email.set(bind("a@b.c")))
            .columns((users::id,))
            .from_select(source)
            .render();

        assert!(matches!(
            result,
            Err(Error::InvalidQueryShape(message))
                if message.contains("cannot combine")
        ));
    }

    #[test]
    fn renders_update_with_from_source() {
        let query = Context::new()
            .update(users::table)
            .set(users::email.set(bind("touched@example.com")))
            .from(posts::table)
            .where_(posts::user_id.eq(users::id))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"update "public"."users" set "email" = $1 "#,
                r#"from "blog"."posts" where ("posts"."user_id" = "users"."id")"#
            )
        );
        assert_eq!(query.binds().len(), 1);
    }

    #[test]
    fn renders_update_from_returning_with_qualified_fields() {
        let query = Context::new()
            .update(users::table)
            .set(users::email.set(bind("touched@example.com")))
            .from(posts::table)
            .where_(posts::user_id.eq(users::id))
            .returning(users::id)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"update "public"."users" set "email" = $1 "#,
                r#"from "blog"."posts" where ("posts"."user_id" = "users"."id") "#,
                r#"returning "users"."id""#
            )
        );
        assert_eq!(query.binds().len(), 1);
    }

    #[test]
    fn renders_delete_with_using_source() {
        let query = Context::new()
            .delete_from(users::table)
            .using(posts::table)
            .where_(posts::user_id.eq(users::id).and(posts::id.gt(bind(10_i64))))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"delete from "public"."users" using "blog"."posts" "#,
                r#"where (("posts"."user_id" = "users"."id") and ("posts"."id" > $1))"#
            )
        );
        assert_eq!(query.binds().len(), 1);
    }

    #[test]
    fn renders_delete_using_returning_with_qualified_fields() {
        let query = Context::new()
            .delete_from(users::table)
            .using(posts::table)
            .where_(posts::user_id.eq(users::id).and(posts::id.gt(bind(10_i64))))
            .returning(users::id)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"delete from "public"."users" using "blog"."posts" "#,
                r#"where (("posts"."user_id" = "users"."id") and ("posts"."id" > $1)) "#,
                r#"returning "users"."id""#
            )
        );
        assert_eq!(query.binds().len(), 1);
    }

    #[test]
    fn renders_condition_all_any_true_false() {
        let empty_all: Vec<Condition> = Vec::new();
        let query = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(Condition::all(empty_all))
            .render()
            .unwrap();
        assert_eq!(
            query.sql(),
            r#"select "users"."id" from "public"."users" where true"#
        );

        let empty_any: Vec<Condition> = Vec::new();
        let query = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(Condition::any(empty_any))
            .render()
            .unwrap();
        assert_eq!(
            query.sql(),
            r#"select "users"."id" from "public"."users" where false"#
        );

        let filters = vec![
            users::active.eq(bind(true)),
            users::signup_rank.gt(bind(10_i32)),
        ];
        let query = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(Condition::all(filters))
            .render()
            .unwrap();
        assert!(query
            .sql()
            .contains(r#"(("users"."active" = $1) and ("users"."signup_rank" > $2))"#));
    }

    #[test]
    fn renders_abs_round_ceil_floor() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<i32, NotNull>(abs(users::signup_rank));
        assert_expr_type::<rust_decimal::Decimal, NotNull>(round(users::signup_rank));
        assert_expr_type::<f64, NotNull>(round(users::ratio));
        assert_expr_type::<f64, NotNull>(ceil(users::ratio));
        assert_expr_type::<f64, NotNull>(floor(users::ratio));

        let query = Context::new()
            .select((
                abs(users::signup_rank).as_("a"),
                round(users::ratio).as_("r"),
                ceil(users::ratio).as_("c"),
                floor(users::ratio).as_("f"),
            ))
            .from(users::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select abs("users"."signup_rank") as "a", "#,
                r#"round("users"."ratio") as "r", "#,
                r#"ceil("users"."ratio") as "c", "#,
                r#"floor("users"."ratio") as "f" from "public"."users""#
            )
        );
    }

    #[test]
    fn filter_alias_matches_where_for_select_update_delete() {
        let s_filtered = Context::new()
            .select(users::id)
            .from(users::table)
            .filter(users::active.eq(bind(true)))
            .render()
            .unwrap();
        let s_where = Context::new()
            .select(users::id)
            .from(users::table)
            .where_(users::active.eq(bind(true)))
            .render()
            .unwrap();
        assert_eq!(s_filtered.sql(), s_where.sql());

        let u_filtered = Context::new()
            .update(users::table)
            .set(users::email.set(bind("a")))
            .filter(users::id.eq(bind(1_i64)))
            .render()
            .unwrap();
        let u_where = Context::new()
            .update(users::table)
            .set(users::email.set(bind("a")))
            .where_(users::id.eq(bind(1_i64)))
            .render()
            .unwrap();
        assert_eq!(u_filtered.sql(), u_where.sql());

        let d_filtered = Context::new()
            .delete_from(users::table)
            .filter(users::id.eq(bind(1_i64)))
            .render()
            .unwrap();
        let d_where = Context::new()
            .delete_from(users::table)
            .where_(users::id.eq(bind(1_i64)))
            .render()
            .unwrap();
        assert_eq!(d_filtered.sql(), d_where.sql());
    }

    #[test]
    fn vec_of_assignments_drives_dynamic_update() {
        let mut patch: Vec<Assignment> = Vec::new();
        let new_email: Option<String> = Some("new@example.com".to_owned());
        let new_active: Option<bool> = None;
        if let Some(v) = new_email {
            patch.push(users::email.set(bind(v)));
        }
        if let Some(v) = new_active {
            patch.push(users::active.set(bind(v)));
        }

        let query = Context::new()
            .update(users::table)
            .set(patch)
            .where_(users::id.eq(bind(1_i64)))
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"update "public"."users" set "email" = $1 "#,
                r#"where ("users"."id" = $2)"#
            )
        );
    }

    #[test]
    fn renders_field_alias_in_select_list() {
        let query = Context::new()
            .select((users::id.as_("uid"), users::email))
            .from(users::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select "users"."id" as "uid", "users"."email" "#,
                r#"from "public"."users""#
            )
        );
    }

    #[test]
    fn aggregate_functions_render_snapshots_and_output_types() {
        fn assert_expr_type<T, N>(_: Expr<T, N>) {}

        assert_expr_type::<i32, Nullable>(min(users::signup_rank));
        assert_expr_type::<i32, Nullable>(max(users::signup_rank));
        assert_expr_type::<rust_decimal::Decimal, Nullable>(avg(users::signup_rank));
        assert_expr_type::<rust_decimal::Decimal, Nullable>(avg(users::created_at));
        assert_expr_type::<f64, Nullable>(avg(users::ratio));
        assert_expr_type::<Vec<String>, Nullable>(array_agg(users::email));
        assert_expr_type::<Vec<Option<String>>, Nullable>(array_agg(users::display_name));
        assert_expr_type::<String, Nullable>(string_agg(users::email, bind(", ")));
        assert_expr_type::<bool, Nullable>(bool_and(users::active));
        assert_expr_type::<bool, Nullable>(bool_or(users::active));

        let query = Context::new()
            .select((
                min(users::signup_rank),
                max(users::signup_rank),
                avg(users::signup_rank),
                array_agg(users::email),
                string_agg(users::email, bind(", ")),
                bool_and(users::active),
                bool_or(users::active),
            ))
            .from(users::table)
            .render()
            .unwrap();

        assert_eq!(
            query.sql(),
            concat!(
                r#"select min("users"."signup_rank"), max("users"."signup_rank"), "#,
                r#"avg("users"."signup_rank"), array_agg("users"."email"), "#,
                r#"string_agg("users"."email", $1), bool_and("users"."active"), "#,
                r#"bool_or("users"."active") from "public"."users""#
            )
        );
        assert_eq!(query.binds().len(), 1);
    }
}
