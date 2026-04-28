use postgres_types::ToSql;

use crate::query::{InsertConflict, SelectDistinct};
use crate::{
    quote_ident, ArithmeticOp, Assignment, BinaryOp, BindValue, DeleteQuery, Error, ExprNode,
    FieldRef, InsertQuery, Join, JoinKind, OrderDirection, OrderExpr, Result, SelectItem,
    SelectQuery, Table, UnaryOp, UpdateQuery,
};

/// A rendered SQL statement plus owned bind values.
#[derive(Debug)]
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

/// Something that can render itself into PostgreSQL SQL and binds.
pub trait RenderQuery {
    fn render(self) -> Result<RenderedQuery>;
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
        self.push_bind(Box::new(value));
    }
}

impl<R> RenderQuery for SelectQuery<R> {
    fn render(self) -> Result<RenderedQuery> {
        let mut renderer = Renderer::default();
        render_select(self, &mut renderer)?;
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

fn render_select<R>(query: SelectQuery<R>, renderer: &mut Renderer) -> Result<()> {
    if query.selection.is_empty() {
        return Err(Error::invalid_query_shape(
            "SELECT requires at least one item",
        ));
    }
    let from = query
        .from
        .ok_or_else(|| Error::invalid_query_shape("SELECT requires a FROM table"))?;

    renderer.sql.push_str("select ");
    if let Some(distinct) = query.distinct {
        render_distinct(distinct, renderer)?;
    }
    render_select_items(query.selection, renderer, FieldQualification::Qualified)?;
    renderer.sql.push_str(" from ");
    render_table(from, renderer)?;

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

    if !query.order_by.is_empty() {
        renderer.sql.push_str(" order by ");
        render_order_by(query.order_by, renderer)?;
    }

    if let Some(limit) = query.limit {
        if limit < 0 {
            return Err(Error::invalid_query_shape("LIMIT cannot be negative"));
        }
        renderer.sql.push_str(" limit ");
        renderer.push_i64_bind(limit);
    }

    if let Some(offset) = query.offset {
        if offset < 0 {
            return Err(Error::invalid_query_shape("OFFSET cannot be negative"));
        }
        renderer.sql.push_str(" offset ");
        renderer.push_i64_bind(offset);
    }

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
        render_select_items(query.returning, renderer, FieldQualification::Unqualified)?;
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
        render_select_items(query.returning, renderer, FieldQualification::Unqualified)?;
    }

    Ok(())
}

fn render_delete<R>(query: DeleteQuery<R>, renderer: &mut Renderer) -> Result<()> {
    renderer.sql.push_str("delete from ");
    render_table(query.table, renderer)?;

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
        render_select_items(query.returning, renderer, FieldQualification::Unqualified)?;
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

fn render_select_items(
    items: Vec<SelectItem>,
    renderer: &mut Renderer,
    qualification: FieldQualification,
) -> Result<()> {
    for (index, item) in items.into_iter().enumerate() {
        if index > 0 {
            renderer.sql.push_str(", ");
        }
        render_expr(item.expr, renderer, qualification)?;
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
    }
    render_table(join.table, renderer)?;
    renderer.sql.push_str(" on ");
    render_expr(join.on.into_node(), renderer, FieldQualification::Qualified)
}

fn render_order_by(order_by: Vec<OrderExpr>, renderer: &mut Renderer) -> Result<()> {
    for (index, order) in order_by.into_iter().enumerate() {
        if index > 0 {
            renderer.sql.push_str(", ");
        }
        render_expr(order.expr, renderer, FieldQualification::Qualified)?;
        renderer.sql.push(' ');
        renderer.sql.push_str(match order.direction {
            OrderDirection::Asc => "asc",
            OrderDirection::Desc => "desc",
        });
    }
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

#[derive(Debug, Clone, Copy)]
enum FieldQualification {
    Qualified,
    Unqualified,
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
            list,
            negated,
        } => {
            if list.is_empty() {
                return Err(Error::invalid_query_shape(
                    "IN predicate requires at least one list item",
                ));
            }

            renderer.sql.push('(');
            render_expr_with_context(*expr, renderer, context)?;
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
        ExprNode::Star => {
            renderer.sql.push('*');
            Ok(())
        }
    }
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
        pub const ratio: Field<f32, NotNull> = Field::new(table, "ratio");
        pub const created_at: Field<i64, NotNull> = Field::new(table, "created_at");
        pub const profile: Field<serde_json::Value, Nullable> = Field::new(table, "profile");
    }

    #[allow(non_upper_case_globals)]
    mod posts {
        use crate::prelude::*;

        pub const table: Table = Table::new("blog", "posts");
        pub const id: Field<i64, NotNull> = Field::new(table, "id");
        pub const user_id: Field<i64, NotNull> = Field::new(table, "user_id");
        pub const title: Field<String, NotNull> = Field::new(table, "title");
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
