use std::marker::PhantomData;
use std::ops::Not;

use rust_decimal::Decimal;

use crate::{BindParam, Field, NotNull, Nullable, Table};

/// Reference to a field in the AST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FieldRef {
    pub(crate) table: Table,
    pub(crate) name: &'static str,
}

impl FieldRef {
    pub(crate) const fn new(table: Table, name: &'static str) -> Self {
        Self { table, name }
    }

    pub const fn table(self) -> Table {
        self.table
    }

    pub const fn name(self) -> &'static str {
        self.name
    }
}

/// Binary SQL operators supported by the MVP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Like,
    ILike,
}

/// Unary SQL operators supported by the MVP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
}

/// Runtime SQL expression AST.
#[derive(Debug)]
pub enum ExprNode {
    Field(FieldRef),
    Bind(BindParam),
    Binary {
        op: BinaryOp,
        left: Box<ExprNode>,
        right: Box<ExprNode>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<ExprNode>,
    },
    IsNull {
        expr: Box<ExprNode>,
        negated: bool,
    },
    Function {
        name: &'static str,
        args: Vec<ExprNode>,
    },
    Star,
}

/// A typed SQL expression.
#[derive(Debug)]
pub struct Expr<T, N = NotNull> {
    pub(crate) node: ExprNode,
    marker: PhantomData<fn() -> (T, N)>,
}

impl<T, N> Expr<T, N> {
    pub(crate) fn from_node(node: ExprNode) -> Self {
        Self {
            node,
            marker: PhantomData,
        }
    }

    pub(crate) fn into_node(self) -> ExprNode {
        self.node
    }

    pub fn eq<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        self.compare(BinaryOp::Eq, rhs)
    }

    pub fn ne<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        self.compare(BinaryOp::Ne, rhs)
    }

    pub fn lt<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        self.compare(BinaryOp::Lt, rhs)
    }

    pub fn lte<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        self.compare(BinaryOp::Lte, rhs)
    }

    pub fn gt<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        self.compare(BinaryOp::Gt, rhs)
    }

    pub fn gte<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        self.compare(BinaryOp::Gte, rhs)
    }

    pub fn is_null(self) -> Condition {
        Condition {
            node: ExprNode::IsNull {
                expr: Box::new(self.node),
                negated: false,
            },
        }
    }

    pub fn is_not_null(self) -> Condition {
        Condition {
            node: ExprNode::IsNull {
                expr: Box::new(self.node),
                negated: true,
            },
        }
    }

    pub fn asc(self) -> OrderExpr {
        OrderExpr {
            expr: self.node,
            direction: OrderDirection::Asc,
        }
    }

    pub fn desc(self) -> OrderExpr {
        OrderExpr {
            expr: self.node,
            direction: OrderDirection::Desc,
        }
    }

    fn compare<R>(self, op: BinaryOp, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        Condition {
            node: ExprNode::Binary {
                op,
                left: Box::new(self.node),
                right: Box::new(rhs.into_expr().node),
            },
        }
    }
}

impl<N> Expr<String, N> {
    pub fn like<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<String>,
    {
        self.compare(BinaryOp::Like, rhs)
    }

    pub fn ilike<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<String>,
    {
        self.compare(BinaryOp::ILike, rhs)
    }
}

impl<T, N> Field<T, N> {
    pub fn expr(self) -> Expr<T, N> {
        Expr::from_node(ExprNode::Field(FieldRef::new(self.table(), self.name())))
    }

    pub fn eq<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        self.expr().eq(rhs)
    }

    pub fn ne<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        self.expr().ne(rhs)
    }

    pub fn lt<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        self.expr().lt(rhs)
    }

    pub fn lte<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        self.expr().lte(rhs)
    }

    pub fn gt<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        self.expr().gt(rhs)
    }

    pub fn gte<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        self.expr().gte(rhs)
    }

    pub fn is_null(self) -> Condition {
        self.expr().is_null()
    }

    pub fn is_not_null(self) -> Condition {
        self.expr().is_not_null()
    }

    pub fn asc(self) -> OrderExpr {
        self.expr().asc()
    }

    pub fn desc(self) -> OrderExpr {
        self.expr().desc()
    }
}

impl<N> Field<String, N> {
    pub fn like<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<String>,
    {
        self.expr().like(rhs)
    }

    pub fn ilike<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<String>,
    {
        self.expr().ilike(rhs)
    }
}

/// Convert fields or expressions into typed expressions.
pub trait IntoExpr<T> {
    type Nullability;

    fn into_expr(self) -> Expr<T, Self::Nullability>;
}

impl<T, N> IntoExpr<T> for Expr<T, N> {
    type Nullability = N;

    fn into_expr(self) -> Expr<T, Self::Nullability> {
        self
    }
}

impl<T, N> IntoExpr<T> for Field<T, N> {
    type Nullability = N;

    fn into_expr(self) -> Expr<T, Self::Nullability> {
        self.expr()
    }
}

/// A boolean SQL condition.
#[derive(Debug)]
pub struct Condition {
    pub(crate) node: ExprNode,
}

impl Condition {
    pub(crate) fn into_node(self) -> ExprNode {
        self.node
    }

    pub fn and(self, rhs: Condition) -> Condition {
        Condition {
            node: ExprNode::Binary {
                op: BinaryOp::And,
                left: Box::new(self.node),
                right: Box::new(rhs.node),
            },
        }
    }

    pub fn or(self, rhs: Condition) -> Condition {
        Condition {
            node: ExprNode::Binary {
                op: BinaryOp::Or,
                left: Box::new(self.node),
                right: Box::new(rhs.node),
            },
        }
    }
}

impl Not for Condition {
    type Output = Condition;

    fn not(self) -> Self::Output {
        not(self)
    }
}

/// Negate a condition.
pub fn not(condition: Condition) -> Condition {
    Condition {
        node: ExprNode::Unary {
            op: UnaryOp::Not,
            expr: Box::new(condition.node),
        },
    }
}

/// Convert an expression to nullable at the type level without changing SQL text.
pub fn nullable<T, E>(expr: E) -> Expr<T, Nullable>
where
    E: IntoExpr<T>,
{
    Expr::from_node(expr.into_expr().into_node())
}

/// SQL order direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

/// SQL `ORDER BY` expression.
#[derive(Debug)]
pub struct OrderExpr {
    pub(crate) expr: ExprNode,
    pub(crate) direction: OrderDirection,
}

/// Render `count(expression)`.
pub fn count<T, E>(expr: E) -> Expr<i64, NotNull>
where
    E: IntoExpr<T>,
{
    Expr::from_node(ExprNode::Function {
        name: "count",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render `count(*)`.
pub fn count_star() -> Expr<i64, NotNull> {
    Expr::from_node(ExprNode::Function {
        name: "count",
        args: vec![ExprNode::Star],
    })
}

/// PostgreSQL return type for `sum(expression)`.
pub trait SumOutput {
    type Output;
}

impl SumOutput for i16 {
    type Output = i64;
}

impl SumOutput for i32 {
    type Output = i64;
}

impl SumOutput for i64 {
    type Output = Decimal;
}

impl SumOutput for Decimal {
    type Output = Decimal;
}

impl SumOutput for f32 {
    type Output = f32;
}

impl SumOutput for f64 {
    type Output = f64;
}

/// Render `sum(expression)`.
pub fn sum<T, E>(expr: E) -> Expr<T::Output, Nullable>
where
    E: IntoExpr<T>,
    T: SumOutput,
{
    Expr::from_node(ExprNode::Function {
        name: "sum",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render PostgreSQL `jsonb_array_length(expression)`.
pub fn jsonb_array_length<E>(expr: E) -> Expr<i32, E::Nullability>
where
    E: IntoExpr<serde_json::Value>,
{
    Expr::from_node(ExprNode::Function {
        name: "jsonb_array_length",
        args: vec![expr.into_expr().into_node()],
    })
}
