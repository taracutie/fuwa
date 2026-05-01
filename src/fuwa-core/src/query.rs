use std::marker::PhantomData;

use crate::{
    AliasedSubquery, Condition, Expr, ExprNode, Field, FieldRef, NotSingleColumn, OrderDirection,
    OrderExpr, RenderQuery, RenderedQuery, Result, SelectItem, Selectable, Table, TableSource,
    TableSourceRef,
};

/// `SELECT ... FOR UPDATE | FOR SHARE | ...` lock strength.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockStrength {
    Update,
    NoKeyUpdate,
    Share,
    KeyShare,
}

/// `SKIP LOCKED` / `NOWAIT` modifier on a row lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockWait {
    Wait,
    NoWait,
    SkipLocked,
}

/// `FOR UPDATE` clause body.
#[derive(Debug, Clone)]
pub struct ForLock {
    pub(crate) strength: LockStrength,
    pub(crate) of: Vec<Table>,
    pub(crate) wait: LockWait,
}

/// SQL set operation kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Union,
    Except,
    Intersect,
}

/// One side of a chained set operation (`UNION`, `EXCEPT`, `INTERSECT`).
#[derive(Debug, Clone)]
pub struct SetOpItem {
    pub(crate) op: SetOp,
    pub(crate) all: bool,
    pub(crate) left_tail: SelectTail,
    pub(crate) query: SelectQuery<(), NotSingleColumn>,
}

/// Tail clauses that appear after a SELECT body.
#[derive(Debug, Clone, Default)]
pub(crate) struct SelectTail {
    pub(crate) order_by: Vec<OrderExpr>,
    pub(crate) limit: Option<i64>,
    pub(crate) offset: Option<i64>,
    pub(crate) for_lock: Option<ForLock>,
}

/// Stateless entry point for building queries.
#[derive(Debug, Clone, Copy, Default)]
pub struct Context;

impl Context {
    pub const fn new() -> Self {
        Self
    }

    pub fn select<S>(&self, selection: S) -> SelectQuery<S::Record, S::SingleSql>
    where
        S: Selectable,
    {
        SelectQuery {
            ctes: Vec::new(),
            selection: selection.into_select_items(),
            distinct: None,
            from: None,
            joins: Vec::new(),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            for_lock: None,
            set_ops: Vec::new(),
            marker: PhantomData,
        }
    }

    pub fn with<R, S>(&self, name: &'static str, query: SelectQuery<R, S>) -> WithBuilder {
        WithBuilder {
            ctes: vec![Cte {
                name,
                query: query.erase_record(),
            }],
        }
    }

    pub fn insert_into(&self, table: Table) -> InsertQuery<()> {
        InsertQuery {
            table,
            rows: Vec::new(),
            insert_columns: Vec::new(),
            select_source: None,
            on_conflict: None,
            returning: Vec::new(),
            marker: PhantomData,
        }
    }

    pub fn update(&self, table: Table) -> UpdateQuery<()> {
        UpdateQuery {
            table,
            assignments: Vec::new(),
            from: None,
            where_clause: None,
            returning: Vec::new(),
            marker: PhantomData,
        }
    }

    pub fn delete_from(&self, table: Table) -> DeleteQuery<()> {
        DeleteQuery {
            table,
            using: None,
            where_clause: None,
            returning: Vec::new(),
            marker: PhantomData,
        }
    }
}

/// Build a `SELECT` query without an executor handle.
///
/// Mirrors JOOQ-style `DSL.select(...)`: query construction is decoupled from
/// execution. Pass the resulting query to `dsl.fetch_all(...)` /
/// `dsl.execute(...)` when you are ready to run it.
pub fn select<S>(selection: S) -> SelectQuery<S::Record, S::SingleSql>
where
    S: Selectable,
{
    Context::new().select(selection)
}

/// Build a `WITH` chain without an executor handle.
pub fn with<R, S>(name: &'static str, query: SelectQuery<R, S>) -> WithBuilder {
    Context::new().with(name, query)
}

/// Build an `INSERT` query without an executor handle.
pub fn insert_into(table: Table) -> InsertQuery<()> {
    Context::new().insert_into(table)
}

/// Build an `UPDATE` query without an executor handle.
pub fn update(table: Table) -> UpdateQuery<()> {
    Context::new().update(table)
}

/// Build a `DELETE` query without an executor handle.
pub fn delete_from(table: Table) -> DeleteQuery<()> {
    Context::new().delete_from(table)
}

/// A `SELECT` query.
#[derive(Debug)]
pub struct SelectQuery<R = (), S = NotSingleColumn> {
    pub(crate) ctes: Vec<Cte>,
    pub(crate) selection: Vec<SelectItem>,
    pub(crate) distinct: Option<SelectDistinct>,
    pub(crate) from: Option<TableSourceRef>,
    pub(crate) joins: Vec<Join>,
    pub(crate) where_clause: Option<Condition>,
    pub(crate) group_by: Vec<ExprNode>,
    pub(crate) having: Option<Condition>,
    pub(crate) order_by: Vec<OrderExpr>,
    pub(crate) limit: Option<i64>,
    pub(crate) offset: Option<i64>,
    pub(crate) for_lock: Option<ForLock>,
    pub(crate) set_ops: Vec<SetOpItem>,
    marker: PhantomData<fn() -> (R, S)>,
}

impl<R, S> Clone for SelectQuery<R, S> {
    fn clone(&self) -> Self {
        Self {
            ctes: self.ctes.clone(),
            selection: self.selection.clone(),
            distinct: self.distinct.clone(),
            from: self.from.clone(),
            joins: self.joins.clone(),
            where_clause: self.where_clause.clone(),
            group_by: self.group_by.clone(),
            having: self.having.clone(),
            order_by: self.order_by.clone(),
            limit: self.limit,
            offset: self.offset,
            for_lock: self.for_lock.clone(),
            set_ops: self.set_ops.clone(),
            marker: PhantomData,
        }
    }
}

/// A non-recursive common table expression attached to a `SELECT`.
#[derive(Debug, Clone)]
pub(crate) struct Cte {
    pub(crate) name: &'static str,
    pub(crate) query: SelectQuery<(), NotSingleColumn>,
}

/// Builder for a `WITH ... SELECT ...` query.
#[derive(Debug, Clone)]
pub struct WithBuilder {
    pub(crate) ctes: Vec<Cte>,
}

impl WithBuilder {
    pub fn with<R, S>(mut self, name: &'static str, query: SelectQuery<R, S>) -> Self {
        self.ctes.push(Cte {
            name,
            query: query.erase_record(),
        });
        self
    }

    pub fn select<S>(self, selection: S) -> SelectQuery<S::Record, S::SingleSql>
    where
        S: Selectable,
    {
        SelectQuery {
            ctes: self.ctes,
            selection: selection.into_select_items(),
            distinct: None,
            from: None,
            joins: Vec::new(),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            for_lock: None,
            set_ops: Vec::new(),
            marker: PhantomData,
        }
    }
}

/// `SELECT` distinct mode.
#[derive(Debug, Clone)]
pub(crate) enum SelectDistinct {
    Distinct,
    DistinctOn(Vec<ExprNode>),
}

impl<R, S> SelectQuery<R, S> {
    pub fn distinct(mut self) -> Self {
        self.distinct = Some(SelectDistinct::Distinct);
        self
    }

    pub fn distinct_on<E>(mut self, columns: E) -> Self
    where
        E: ExprList,
    {
        self.distinct = Some(SelectDistinct::DistinctOn(columns.into_exprs()));
        self
    }

    pub fn from<Source>(mut self, source: Source) -> Self
    where
        Source: TableSource,
    {
        self.from = Some(source.into_table_source());
        self
    }

    pub fn join(mut self, target: JoinTarget) -> Self {
        self.joins.push(Join {
            kind: JoinKind::Inner,
            lateral: false,
            source: target.source,
            on: Some(target.on),
        });
        self
    }

    pub fn left_join(mut self, target: JoinTarget) -> Self {
        self.joins.push(Join {
            kind: JoinKind::Left,
            lateral: false,
            source: target.source,
            on: Some(target.on),
        });
        self
    }

    pub fn right_join(mut self, target: JoinTarget) -> Self {
        self.joins.push(Join {
            kind: JoinKind::Right,
            lateral: false,
            source: target.source,
            on: Some(target.on),
        });
        self
    }

    pub fn full_join(mut self, target: JoinTarget) -> Self {
        self.joins.push(Join {
            kind: JoinKind::Full,
            lateral: false,
            source: target.source,
            on: Some(target.on),
        });
        self
    }

    pub fn cross_join<T>(mut self, source: T) -> Self
    where
        T: TableSource,
    {
        self.joins.push(Join {
            kind: JoinKind::Cross,
            lateral: false,
            source: source.into_table_source(),
            on: None,
        });
        self
    }

    pub fn join_lateral(mut self, target: JoinTarget) -> Self {
        self.joins.push(Join {
            kind: JoinKind::Inner,
            lateral: true,
            source: target.source,
            on: Some(target.on),
        });
        self
    }

    pub fn left_join_lateral(mut self, target: JoinTarget) -> Self {
        self.joins.push(Join {
            kind: JoinKind::Left,
            lateral: true,
            source: target.source,
            on: Some(target.on),
        });
        self
    }

    pub fn cross_join_lateral<T>(mut self, source: T) -> Self
    where
        T: TableSource,
    {
        self.joins.push(Join {
            kind: JoinKind::Cross,
            lateral: true,
            source: source.into_table_source(),
            on: None,
        });
        self
    }

    pub fn where_<C: crate::IntoCondition>(mut self, condition: C) -> Self {
        let condition = condition.into_condition();
        self.where_clause = Some(match self.where_clause {
            Some(existing) => existing.and(condition),
            None => condition,
        });
        self
    }

    /// Alias for [`where_`](Self::where_) for diesel-style ergonomics.
    pub fn filter<C: crate::IntoCondition>(self, condition: C) -> Self {
        self.where_(condition)
    }

    pub fn group_by<E>(mut self, group_by: E) -> Self
    where
        E: ExprList,
    {
        self.group_by.extend(group_by.into_exprs());
        self
    }

    pub fn having<C: crate::IntoCondition>(mut self, condition: C) -> Self {
        let condition = condition.into_condition();
        self.having = Some(match self.having {
            Some(existing) => existing.and(condition),
            None => condition,
        });
        self
    }

    pub fn order_by<O>(mut self, order_by: O) -> Self
    where
        O: OrderByList,
    {
        self.order_by.extend(order_by.into_order_by());
        self
    }

    pub fn limit(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: i64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn alias(self, alias: &'static str) -> AliasedSubquery {
        AliasedSubquery::new(self, alias)
    }

    pub fn render(self) -> Result<RenderedQuery> {
        RenderQuery::render(self)
    }

    /// Render this query without consuming it.
    pub fn render_ref(&self) -> Result<RenderedQuery> {
        RenderQuery::render(Clone::clone(self))
    }

    /// Acquire a row lock with `FOR UPDATE`.
    pub fn for_update(mut self) -> Self {
        self.for_lock = Some(ForLock {
            strength: LockStrength::Update,
            of: Vec::new(),
            wait: LockWait::Wait,
        });
        self
    }

    /// Acquire a row lock with `FOR NO KEY UPDATE`.
    pub fn for_no_key_update(mut self) -> Self {
        self.for_lock = Some(ForLock {
            strength: LockStrength::NoKeyUpdate,
            of: Vec::new(),
            wait: LockWait::Wait,
        });
        self
    }

    /// Acquire a row lock with `FOR SHARE`.
    pub fn for_share(mut self) -> Self {
        self.for_lock = Some(ForLock {
            strength: LockStrength::Share,
            of: Vec::new(),
            wait: LockWait::Wait,
        });
        self
    }

    /// Acquire a row lock with `FOR KEY SHARE`.
    pub fn for_key_share(mut self) -> Self {
        self.for_lock = Some(ForLock {
            strength: LockStrength::KeyShare,
            of: Vec::new(),
            wait: LockWait::Wait,
        });
        self
    }

    /// Restrict the row lock to specific tables (`OF table1, table2`).
    pub fn of<L>(mut self, tables: L) -> Self
    where
        L: LockTargetList,
    {
        let lock = self.for_lock.as_mut().expect(
            "of(...) requires a prior for_update/for_share/for_no_key_update/for_key_share call",
        );
        lock.of = tables.into_tables();
        self
    }

    /// Add `SKIP LOCKED` to the row lock.
    pub fn skip_locked(mut self) -> Self {
        let lock = self.for_lock.as_mut().expect(
            "skip_locked() requires a prior for_update/for_share/for_no_key_update/for_key_share call",
        );
        lock.wait = LockWait::SkipLocked;
        self
    }

    /// Add `NOWAIT` to the row lock.
    pub fn no_wait(mut self) -> Self {
        let lock = self.for_lock.as_mut().expect(
            "no_wait() requires a prior for_update/for_share/for_no_key_update/for_key_share call",
        );
        lock.wait = LockWait::NoWait;
        self
    }

    /// Append `UNION` with another `SELECT`.
    pub fn union(mut self, other: SelectQuery<R, S>) -> Self {
        self.push_set_op(SetOp::Union, false, other);
        self
    }

    /// Append `UNION ALL` with another `SELECT`.
    pub fn union_all(mut self, other: SelectQuery<R, S>) -> Self {
        self.push_set_op(SetOp::Union, true, other);
        self
    }

    /// Append `EXCEPT` with another `SELECT`.
    pub fn except(mut self, other: SelectQuery<R, S>) -> Self {
        self.push_set_op(SetOp::Except, false, other);
        self
    }

    /// Append `EXCEPT ALL` with another `SELECT`.
    pub fn except_all(mut self, other: SelectQuery<R, S>) -> Self {
        self.push_set_op(SetOp::Except, true, other);
        self
    }

    /// Append `INTERSECT` with another `SELECT`.
    pub fn intersect(mut self, other: SelectQuery<R, S>) -> Self {
        self.push_set_op(SetOp::Intersect, false, other);
        self
    }

    /// Append `INTERSECT ALL` with another `SELECT`.
    pub fn intersect_all(mut self, other: SelectQuery<R, S>) -> Self {
        self.push_set_op(SetOp::Intersect, true, other);
        self
    }

    fn push_set_op(&mut self, op: SetOp, all: bool, other: SelectQuery<R, S>) {
        let left_tail = self.take_tail();
        self.set_ops.push(SetOpItem {
            op,
            all,
            left_tail,
            query: other.erase_record(),
        });
    }

    fn take_tail(&mut self) -> SelectTail {
        SelectTail {
            order_by: std::mem::take(&mut self.order_by),
            limit: self.limit.take(),
            offset: self.offset.take(),
            for_lock: self.for_lock.take(),
        }
    }

    /// Append a raw `SelectItem` to the projection.
    ///
    /// The Record type is *not* updated; this is intended for dynamic queries
    /// where the caller will decode rows themselves (e.g. with `fetch_all_as`).
    pub fn push_select_item(mut self, item: SelectItem) -> Self {
        self.selection.push(item);
        self
    }

    /// Append a raw `OrderExpr` to the ORDER BY clause.
    pub fn push_order_by(mut self, order: OrderExpr) -> Self {
        self.order_by.push(order);
        self
    }

    /// Append a raw `Join` to the FROM clause.
    pub fn push_join(mut self, join: Join) -> Self {
        self.joins.push(join);
        self
    }

    /// Append an `ExprNode` to GROUP BY.
    pub fn push_group_by(mut self, expr: ExprNode) -> Self {
        self.group_by.push(expr);
        self
    }

    /// AND another condition into WHERE.
    ///
    /// Equivalent to `.where_(condition)` but written as a verb so it reads
    /// well in dynamic accumulation loops.
    pub fn and_where<C: crate::IntoCondition>(self, condition: C) -> Self {
        self.where_(condition)
    }

    /// AND another condition into HAVING.
    pub fn and_having<C: crate::IntoCondition>(self, condition: C) -> Self {
        self.having(condition)
    }

    pub(crate) fn erase_record(self) -> SelectQuery<(), NotSingleColumn> {
        SelectQuery {
            ctes: self.ctes,
            selection: self.selection,
            distinct: self.distinct,
            from: self.from,
            joins: self.joins,
            where_clause: self.where_clause,
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            limit: self.limit,
            offset: self.offset,
            for_lock: self.for_lock,
            set_ops: self.set_ops,
            marker: PhantomData,
        }
    }
}

/// A list of tables passed to `SelectQuery::of(...)` for `FOR UPDATE OF`.
pub trait LockTargetList {
    fn into_tables(self) -> Vec<Table>;
}

impl LockTargetList for Table {
    fn into_tables(self) -> Vec<Table> {
        vec![self]
    }
}

macro_rules! impl_tuple_lock_target_list {
    ($($ty:ident $var:ident),+ $(,)?) => {
        impl<$($ty),+> LockTargetList for ($($ty,)+)
        where
            $($ty: LockTargetList),+
        {
            fn into_tables(self) -> Vec<Table> {
                let ($($var,)+) = self;
                let mut tables = Vec::new();
                $(
                    tables.extend($var.into_tables());
                )+
                tables
            }
        }
    };
}

impl_tuple_lock_target_list!(A a);
impl_tuple_lock_target_list!(A a, B b);
impl_tuple_lock_target_list!(A a, B b, C c);
impl_tuple_lock_target_list!(A a, B b, C c, D d);
impl_tuple_lock_target_list!(A a, B b, C c, D d, E e);
impl_tuple_lock_target_list!(A a, B b, C c, D d, E e, F f);
impl_tuple_lock_target_list!(A a, B b, C c, D d, E e, F f, G g);
impl_tuple_lock_target_list!(A a, B b, C c, D d, E e, F f, G g, H h);

/// A list of SQL expressions.
pub trait ExprList {
    fn into_exprs(self) -> Vec<ExprNode>;
}

impl<T, N> ExprList for Field<T, N> {
    fn into_exprs(self) -> Vec<ExprNode> {
        vec![self.expr().into_node()]
    }
}

impl<T, N> ExprList for Expr<T, N> {
    fn into_exprs(self) -> Vec<ExprNode> {
        vec![self.into_node()]
    }
}

macro_rules! impl_tuple_expr_list {
    ($($ty:ident $var:ident),+ $(,)?) => {
        impl<$($ty),+> ExprList for ($($ty,)+)
        where
            $($ty: ExprList),+
        {
            fn into_exprs(self) -> Vec<ExprNode> {
                let ($($var,)+) = self;
                let mut exprs = Vec::new();
                $(
                    exprs.extend($var.into_exprs());
                )+
                exprs
            }
        }
    };
}

impl_tuple_expr_list!(A a);
impl_tuple_expr_list!(A a, B b);
impl_tuple_expr_list!(A a, B b, C c);
impl_tuple_expr_list!(A a, B b, C c, D d);
impl_tuple_expr_list!(A a, B b, C c, D d, E e);
impl_tuple_expr_list!(A a, B b, C c, D d, E e, F f);
impl_tuple_expr_list!(A a, B b, C c, D d, E e, F f, G g);
impl_tuple_expr_list!(A a, B b, C c, D d, E e, F f, G g, H h);
impl_tuple_expr_list!(A a, B b, C c, D d, E e, F f, G g, H h, I i);
impl_tuple_expr_list!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
impl_tuple_expr_list!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);
impl_tuple_expr_list!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l);
impl_tuple_expr_list!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m);
impl_tuple_expr_list!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n);
impl_tuple_expr_list!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o);
impl_tuple_expr_list!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o, P p);

/// SQL join kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// A complete join clause.
#[derive(Debug, Clone)]
pub struct Join {
    pub(crate) kind: JoinKind,
    pub(crate) lateral: bool,
    pub(crate) source: TableSourceRef,
    pub(crate) on: Option<Condition>,
}

/// A table plus `ON` condition before the join kind is chosen.
#[derive(Debug, Clone)]
pub struct JoinTarget {
    pub(crate) source: TableSourceRef,
    pub(crate) on: Condition,
}

/// Field assignment used by `INSERT` and `UPDATE`.
#[derive(Debug, Clone)]
pub struct Assignment {
    pub(crate) field: FieldRef,
    pub(crate) value: ExprNode,
}

impl<T, N> Field<T, N> {
    pub fn set<R>(self, value: R) -> Assignment
    where
        R: crate::IntoExpr<T>,
    {
        Assignment {
            field: FieldRef::new(self.table(), self.name()),
            value: value.into_expr().into_node(),
        }
    }
}

/// A list of field assignments.
pub trait Assignments {
    fn into_assignments(self) -> Vec<Assignment>;
}

impl Assignments for Assignment {
    fn into_assignments(self) -> Vec<Assignment> {
        vec![self]
    }
}

impl Assignments for Vec<Assignment> {
    fn into_assignments(self) -> Vec<Assignment> {
        self
    }
}

macro_rules! impl_tuple_assignments {
    ($($ty:ident $var:ident),+ $(,)?) => {
        impl<$($ty),+> Assignments for ($($ty,)+)
        where
            $($ty: Assignments),+
        {
            fn into_assignments(self) -> Vec<Assignment> {
                let ($($var,)+) = self;
                let mut assignments = Vec::new();
                $(
                    assignments.extend($var.into_assignments());
                )+
                assignments
            }
        }
    };
}

impl_tuple_assignments!(A a);
impl_tuple_assignments!(A a, B b);
impl_tuple_assignments!(A a, B b, C c);
impl_tuple_assignments!(A a, B b, C c, D d);
impl_tuple_assignments!(A a, B b, C c, D d, E e);
impl_tuple_assignments!(A a, B b, C c, D d, E e, F f);
impl_tuple_assignments!(A a, B b, C c, D d, E e, F f, G g);
impl_tuple_assignments!(A a, B b, C c, D d, E e, F f, G g, H h);
impl_tuple_assignments!(A a, B b, C c, D d, E e, F f, G g, H h, I i);
impl_tuple_assignments!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
impl_tuple_assignments!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);
impl_tuple_assignments!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l);
impl_tuple_assignments!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m);
impl_tuple_assignments!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n);
impl_tuple_assignments!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o);
impl_tuple_assignments!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o, P p);

/// A field reference that resolves to PostgreSQL's `excluded` pseudo-table.
#[derive(Debug, Clone, Copy, Default)]
pub struct Excluded;

impl Excluded {
    pub fn field<T, N>(self, field: Field<T, N>) -> Expr<T, N> {
        Expr::from_node(ExprNode::ExcludedField(FieldRef::new(
            field.table(),
            field.name(),
        )))
    }
}

/// A field list accepted by `ON CONFLICT`.
pub trait ConflictTarget {
    fn into_conflict_fields(self) -> Vec<FieldRef>;
}

impl<T, N> ConflictTarget for Field<T, N> {
    fn into_conflict_fields(self) -> Vec<FieldRef> {
        vec![FieldRef::new(self.table(), self.name())]
    }
}

macro_rules! impl_tuple_conflict_target {
    ($($ty:ident $var:ident),+ $(,)?) => {
        impl<$($ty),+> ConflictTarget for ($($ty,)+)
        where
            $($ty: ConflictTarget),+
        {
            fn into_conflict_fields(self) -> Vec<FieldRef> {
                let ($($var,)+) = self;
                let mut fields = Vec::new();
                $(
                    fields.extend($var.into_conflict_fields());
                )+
                fields
            }
        }
    };
}

impl_tuple_conflict_target!(A a);
impl_tuple_conflict_target!(A a, B b);
impl_tuple_conflict_target!(A a, B b, C c);
impl_tuple_conflict_target!(A a, B b, C c, D d);
impl_tuple_conflict_target!(A a, B b, C c, D d, E e);
impl_tuple_conflict_target!(A a, B b, C c, D d, E e, F f);
impl_tuple_conflict_target!(A a, B b, C c, D d, E e, F f, G g);
impl_tuple_conflict_target!(A a, B b, C c, D d, E e, F f, G g, H h);
impl_tuple_conflict_target!(A a, B b, C c, D d, E e, F f, G g, H h, I i);
impl_tuple_conflict_target!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
impl_tuple_conflict_target!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);
impl_tuple_conflict_target!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l);
impl_tuple_conflict_target!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m);
impl_tuple_conflict_target!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n);
impl_tuple_conflict_target!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o);
impl_tuple_conflict_target!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o, P p);

/// `ON CONFLICT` behavior for an `INSERT`.
#[derive(Debug, Clone)]
pub(crate) enum InsertConflict {
    DoNothing {
        target: Vec<FieldRef>,
    },
    DoUpdate {
        target: Vec<FieldRef>,
        assignments: Vec<Assignment>,
    },
}

/// An `INSERT` query.
#[derive(Debug)]
pub struct InsertQuery<R = ()> {
    pub(crate) table: Table,
    pub(crate) rows: Vec<Vec<Assignment>>,
    pub(crate) insert_columns: Vec<FieldRef>,
    pub(crate) select_source: Option<SelectQuery<(), NotSingleColumn>>,
    pub(crate) on_conflict: Option<InsertConflict>,
    pub(crate) returning: Vec<SelectItem>,
    marker: PhantomData<fn() -> R>,
}

impl<R> Clone for InsertQuery<R> {
    fn clone(&self) -> Self {
        Self {
            table: self.table,
            rows: self.rows.clone(),
            insert_columns: self.insert_columns.clone(),
            select_source: self.select_source.clone(),
            on_conflict: self.on_conflict.clone(),
            returning: self.returning.clone(),
            marker: PhantomData,
        }
    }
}

impl<R> InsertQuery<R> {
    pub fn values<A>(mut self, assignments: A) -> Self
    where
        A: Assignments,
    {
        self.rows = vec![assignments.into_assignments()];
        self
    }

    /// Insert a single record using its `Assignments` impl (typically a
    /// derived `#[derive(Insertable)]`).
    pub fn value<A>(self, record: A) -> Self
    where
        A: Assignments,
    {
        self.values(record)
    }

    pub fn values_many<I, A>(mut self, rows: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: Assignments,
    {
        self.rows = rows
            .into_iter()
            .map(Assignments::into_assignments)
            .collect();
        self
    }

    /// Declare the target columns for `INSERT ... SELECT`.
    pub fn columns<C>(mut self, columns: C) -> Self
    where
        C: ConflictTarget,
    {
        self.insert_columns = columns.into_conflict_fields();
        self
    }

    /// Source rows from a `SELECT` query (must be preceded by `.columns(...)`).
    pub fn from_select<R2, S>(mut self, query: SelectQuery<R2, S>) -> Self {
        self.select_source = Some(query.erase_record());
        self
    }

    pub fn on_conflict<T>(self, target: T) -> InsertConflictBuilder<R>
    where
        T: ConflictTarget,
    {
        InsertConflictBuilder {
            query: self,
            target: target.into_conflict_fields(),
        }
    }

    pub fn returning<S>(self, selection: S) -> InsertQuery<S::Record>
    where
        S: Selectable,
    {
        InsertQuery {
            table: self.table,
            rows: self.rows,
            insert_columns: self.insert_columns,
            select_source: self.select_source,
            on_conflict: self.on_conflict,
            returning: selection.into_select_items(),
            marker: PhantomData,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        RenderQuery::render(self)
    }

    /// Render this query without consuming it.
    pub fn render_ref(&self) -> Result<RenderedQuery> {
        RenderQuery::render(Clone::clone(self))
    }
}

/// Builder returned after `INSERT ... ON CONFLICT (...)`.
#[derive(Debug, Clone)]
pub struct InsertConflictBuilder<R = ()> {
    query: InsertQuery<R>,
    target: Vec<FieldRef>,
}

impl<R> InsertConflictBuilder<R> {
    pub fn do_nothing(mut self) -> InsertQuery<R> {
        self.query.on_conflict = Some(InsertConflict::DoNothing {
            target: self.target,
        });
        self.query
    }

    pub fn do_update<F, A>(mut self, f: F) -> InsertQuery<R>
    where
        F: FnOnce(Excluded) -> A,
        A: Assignments,
    {
        self.query.on_conflict = Some(InsertConflict::DoUpdate {
            target: self.target,
            assignments: f(Excluded).into_assignments(),
        });
        self.query
    }
}

/// An `UPDATE` query.
#[derive(Debug)]
pub struct UpdateQuery<R = ()> {
    pub(crate) table: Table,
    pub(crate) assignments: Vec<Assignment>,
    pub(crate) from: Option<TableSourceRef>,
    pub(crate) where_clause: Option<Condition>,
    pub(crate) returning: Vec<SelectItem>,
    marker: PhantomData<fn() -> R>,
}

impl<R> Clone for UpdateQuery<R> {
    fn clone(&self) -> Self {
        Self {
            table: self.table,
            assignments: self.assignments.clone(),
            from: self.from.clone(),
            where_clause: self.where_clause.clone(),
            returning: self.returning.clone(),
            marker: PhantomData,
        }
    }
}

impl<R> UpdateQuery<R> {
    pub fn set<A>(mut self, assignments: A) -> Self
    where
        A: Assignments,
    {
        self.assignments = assignments.into_assignments();
        self
    }

    /// Add a `FROM <source>` clause to allow joining additional tables in `SET ... FROM ...`.
    pub fn from<S>(mut self, source: S) -> Self
    where
        S: TableSource,
    {
        self.from = Some(source.into_table_source());
        self
    }

    pub fn where_<C: crate::IntoCondition>(mut self, condition: C) -> Self {
        let condition = condition.into_condition();
        self.where_clause = Some(match self.where_clause {
            Some(existing) => existing.and(condition),
            None => condition,
        });
        self
    }

    /// Alias for [`where_`](Self::where_) for diesel-style ergonomics.
    pub fn filter<C: crate::IntoCondition>(self, condition: C) -> Self {
        self.where_(condition)
    }

    pub fn returning<S>(self, selection: S) -> UpdateQuery<S::Record>
    where
        S: Selectable,
    {
        UpdateQuery {
            table: self.table,
            assignments: self.assignments,
            from: self.from,
            where_clause: self.where_clause,
            returning: selection.into_select_items(),
            marker: PhantomData,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        RenderQuery::render(self)
    }

    /// Render this query without consuming it.
    pub fn render_ref(&self) -> Result<RenderedQuery> {
        RenderQuery::render(Clone::clone(self))
    }
}

/// A `DELETE` query.
#[derive(Debug)]
pub struct DeleteQuery<R = ()> {
    pub(crate) table: Table,
    pub(crate) using: Option<TableSourceRef>,
    pub(crate) where_clause: Option<Condition>,
    pub(crate) returning: Vec<SelectItem>,
    marker: PhantomData<fn() -> R>,
}

impl<R> Clone for DeleteQuery<R> {
    fn clone(&self) -> Self {
        Self {
            table: self.table,
            using: self.using.clone(),
            where_clause: self.where_clause.clone(),
            returning: self.returning.clone(),
            marker: PhantomData,
        }
    }
}

impl<R> DeleteQuery<R> {
    /// Add a `USING <source>` clause for cross-table deletes.
    pub fn using<S>(mut self, source: S) -> Self
    where
        S: TableSource,
    {
        self.using = Some(source.into_table_source());
        self
    }

    pub fn where_<C: crate::IntoCondition>(mut self, condition: C) -> Self {
        let condition = condition.into_condition();
        self.where_clause = Some(match self.where_clause {
            Some(existing) => existing.and(condition),
            None => condition,
        });
        self
    }

    /// Alias for [`where_`](Self::where_) for diesel-style ergonomics.
    pub fn filter<C: crate::IntoCondition>(self, condition: C) -> Self {
        self.where_(condition)
    }

    pub fn returning<S>(self, selection: S) -> DeleteQuery<S::Record>
    where
        S: Selectable,
    {
        DeleteQuery {
            table: self.table,
            using: self.using,
            where_clause: self.where_clause,
            returning: selection.into_select_items(),
            marker: PhantomData,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        RenderQuery::render(self)
    }

    /// Render this query without consuming it.
    pub fn render_ref(&self) -> Result<RenderedQuery> {
        RenderQuery::render(Clone::clone(self))
    }
}

/// A list of `ORDER BY` expressions.
pub trait OrderByList {
    fn into_order_by(self) -> Vec<OrderExpr>;
}

impl OrderByList for OrderExpr {
    fn into_order_by(self) -> Vec<OrderExpr> {
        vec![self]
    }
}

macro_rules! impl_tuple_order_by {
    ($($ty:ident $var:ident),+ $(,)?) => {
        impl<$($ty),+> OrderByList for ($($ty,)+)
        where
            $($ty: OrderByList),+
        {
            fn into_order_by(self) -> Vec<OrderExpr> {
                let ($($var,)+) = self;
                let mut order_by = Vec::new();
                $(
                    order_by.extend($var.into_order_by());
                )+
                order_by
            }
        }
    };
}

impl_tuple_order_by!(A a);
impl_tuple_order_by!(A a, B b);
impl_tuple_order_by!(A a, B b, C c);
impl_tuple_order_by!(A a, B b, C c, D d);
impl_tuple_order_by!(A a, B b, C c, D d, E e);
impl_tuple_order_by!(A a, B b, C c, D d, E e, F f);
impl_tuple_order_by!(A a, B b, C c, D d, E e, F f, G g);
impl_tuple_order_by!(A a, B b, C c, D d, E e, F f, G g, H h);
impl_tuple_order_by!(A a, B b, C c, D d, E e, F f, G g, H h, I i);
impl_tuple_order_by!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
impl_tuple_order_by!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);
impl_tuple_order_by!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l);
impl_tuple_order_by!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m);
impl_tuple_order_by!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n);
impl_tuple_order_by!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o);
impl_tuple_order_by!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o, P p);

impl From<OrderDirection> for &'static str {
    fn from(direction: OrderDirection) -> Self {
        match direction {
            OrderDirection::Asc => "asc",
            OrderDirection::Desc => "desc",
        }
    }
}
