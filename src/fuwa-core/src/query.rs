use std::marker::PhantomData;

use crate::{
    Condition, Expr, ExprNode, Field, FieldRef, OrderDirection, OrderExpr, RenderQuery,
    RenderedQuery, Result, SelectItem, Selectable, Table,
};

/// Stateless entry point for building queries.
#[derive(Debug, Clone, Copy, Default)]
pub struct Context;

impl Context {
    pub const fn new() -> Self {
        Self
    }

    pub fn select<S>(&self, selection: S) -> SelectQuery<S::Record>
    where
        S: Selectable,
    {
        SelectQuery {
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
            marker: PhantomData,
        }
    }

    pub fn insert_into(&self, table: Table) -> InsertQuery<()> {
        InsertQuery {
            table,
            assignments: Vec::new(),
            returning: Vec::new(),
            marker: PhantomData,
        }
    }

    pub fn update(&self, table: Table) -> UpdateQuery<()> {
        UpdateQuery {
            table,
            assignments: Vec::new(),
            where_clause: None,
            returning: Vec::new(),
            marker: PhantomData,
        }
    }

    pub fn delete_from(&self, table: Table) -> DeleteQuery<()> {
        DeleteQuery {
            table,
            where_clause: None,
            returning: Vec::new(),
            marker: PhantomData,
        }
    }
}

/// A `SELECT` query.
#[derive(Debug)]
pub struct SelectQuery<R = ()> {
    pub(crate) selection: Vec<SelectItem>,
    pub(crate) distinct: Option<SelectDistinct>,
    pub(crate) from: Option<Table>,
    pub(crate) joins: Vec<Join>,
    pub(crate) where_clause: Option<Condition>,
    pub(crate) group_by: Vec<ExprNode>,
    pub(crate) having: Option<Condition>,
    pub(crate) order_by: Vec<OrderExpr>,
    pub(crate) limit: Option<i64>,
    pub(crate) offset: Option<i64>,
    marker: PhantomData<fn() -> R>,
}

/// `SELECT` distinct mode.
#[derive(Debug)]
pub(crate) enum SelectDistinct {
    Distinct,
    DistinctOn(Vec<ExprNode>),
}

impl<R> SelectQuery<R> {
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

    pub fn from(mut self, table: Table) -> Self {
        self.from = Some(table);
        self
    }

    pub fn join(mut self, target: JoinTarget) -> Self {
        self.joins.push(Join {
            kind: JoinKind::Inner,
            table: target.table,
            on: target.on,
        });
        self
    }

    pub fn left_join(mut self, target: JoinTarget) -> Self {
        self.joins.push(Join {
            kind: JoinKind::Left,
            table: target.table,
            on: target.on,
        });
        self
    }

    pub fn where_(mut self, condition: Condition) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => existing.and(condition),
            None => condition,
        });
        self
    }

    pub fn group_by<E>(mut self, group_by: E) -> Self
    where
        E: ExprList,
    {
        self.group_by.extend(group_by.into_exprs());
        self
    }

    pub fn having(mut self, condition: Condition) -> Self {
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

    pub fn render(self) -> Result<RenderedQuery> {
        RenderQuery::render(self)
    }
}

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

/// SQL join kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

/// A complete join clause.
#[derive(Debug)]
pub struct Join {
    pub(crate) kind: JoinKind,
    pub(crate) table: Table,
    pub(crate) on: Condition,
}

/// A table plus `ON` condition before the join kind is chosen.
#[derive(Debug)]
pub struct JoinTarget {
    pub(crate) table: Table,
    pub(crate) on: Condition,
}

/// Field assignment used by `INSERT` and `UPDATE`.
#[derive(Debug)]
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

/// An `INSERT` query.
#[derive(Debug)]
pub struct InsertQuery<R = ()> {
    pub(crate) table: Table,
    pub(crate) assignments: Vec<Assignment>,
    pub(crate) returning: Vec<SelectItem>,
    marker: PhantomData<fn() -> R>,
}

impl<R> InsertQuery<R> {
    pub fn values<A>(mut self, assignments: A) -> Self
    where
        A: Assignments,
    {
        self.assignments = assignments.into_assignments();
        self
    }

    pub fn returning<S>(self, selection: S) -> InsertQuery<S::Record>
    where
        S: Selectable,
    {
        InsertQuery {
            table: self.table,
            assignments: self.assignments,
            returning: selection.into_select_items(),
            marker: PhantomData,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        RenderQuery::render(self)
    }
}

/// An `UPDATE` query.
#[derive(Debug)]
pub struct UpdateQuery<R = ()> {
    pub(crate) table: Table,
    pub(crate) assignments: Vec<Assignment>,
    pub(crate) where_clause: Option<Condition>,
    pub(crate) returning: Vec<SelectItem>,
    marker: PhantomData<fn() -> R>,
}

impl<R> UpdateQuery<R> {
    pub fn set<A>(mut self, assignments: A) -> Self
    where
        A: Assignments,
    {
        self.assignments = assignments.into_assignments();
        self
    }

    pub fn where_(mut self, condition: Condition) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => existing.and(condition),
            None => condition,
        });
        self
    }

    pub fn returning<S>(self, selection: S) -> UpdateQuery<S::Record>
    where
        S: Selectable,
    {
        UpdateQuery {
            table: self.table,
            assignments: self.assignments,
            where_clause: self.where_clause,
            returning: selection.into_select_items(),
            marker: PhantomData,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        RenderQuery::render(self)
    }
}

/// A `DELETE` query.
#[derive(Debug)]
pub struct DeleteQuery<R = ()> {
    pub(crate) table: Table,
    pub(crate) where_clause: Option<Condition>,
    pub(crate) returning: Vec<SelectItem>,
    marker: PhantomData<fn() -> R>,
}

impl<R> DeleteQuery<R> {
    pub fn where_(mut self, condition: Condition) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => existing.and(condition),
            None => condition,
        });
        self
    }

    pub fn returning<S>(self, selection: S) -> DeleteQuery<S::Record>
    where
        S: Selectable,
    {
        DeleteQuery {
            table: self.table,
            where_clause: self.where_clause,
            returning: selection.into_select_items(),
            marker: PhantomData,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        RenderQuery::render(self)
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

impl From<OrderDirection> for &'static str {
    fn from(direction: OrderDirection) -> Self {
        match direction {
            OrderDirection::Asc => "asc",
            OrderDirection::Desc => "desc",
        }
    }
}
