use std::marker::PhantomData;
use std::ops::{Add, Div, Mul, Not, Sub};

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

/// Arithmetic SQL operators supported by typed numeric expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// Unary SQL operators supported by the MVP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
}

/// Marker trait for SQL types that support arithmetic operators.
#[doc(hidden)]
pub trait SqlNumeric {}

macro_rules! impl_sql_numeric {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl SqlNumeric for $ty {}
        )+
    };
}

impl_sql_numeric!(i16, i32, i64, f32, f64, Decimal);

/// Type-level SQL nullability for expressions that are nullable if either side is nullable.
#[doc(hidden)]
pub trait NullableIfEither<Rhs> {
    type Output;
}

impl NullableIfEither<NotNull> for NotNull {
    type Output = NotNull;
}

impl NullableIfEither<Nullable> for NotNull {
    type Output = Nullable;
}

impl NullableIfEither<NotNull> for Nullable {
    type Output = Nullable;
}

impl NullableIfEither<Nullable> for Nullable {
    type Output = Nullable;
}

/// Type-level SQL nullability for `coalesce`, nullable only when every input is nullable.
#[doc(hidden)]
pub trait CoalesceNullability<Rhs> {
    type Output;
}

impl CoalesceNullability<NotNull> for NotNull {
    type Output = NotNull;
}

impl CoalesceNullability<Nullable> for NotNull {
    type Output = NotNull;
}

impl CoalesceNullability<NotNull> for Nullable {
    type Output = NotNull;
}

impl CoalesceNullability<Nullable> for Nullable {
    type Output = Nullable;
}

#[doc(hidden)]
pub trait CoalesceFold<Current> {
    type Output;
}

impl<Current> CoalesceFold<Current> for () {
    type Output = Current;
}

macro_rules! impl_coalesce_fold {
    ($head:ident $(, $tail:ident)*) => {
        impl<Current, $head, $($tail),*> CoalesceFold<Current> for ($head, $($tail,)*)
        where
            Current: CoalesceNullability<$head>,
            ($($tail,)*): CoalesceFold<<Current as CoalesceNullability<$head>>::Output>,
        {
            type Output =
                <($($tail,)*) as CoalesceFold<<Current as CoalesceNullability<$head>>::Output>>::Output;
        }
    };
}

impl_coalesce_fold!(A);
impl_coalesce_fold!(A, B);
impl_coalesce_fold!(A, B, C);
impl_coalesce_fold!(A, B, C, D);
impl_coalesce_fold!(A, B, C, D, E);
impl_coalesce_fold!(A, B, C, D, E, F);
impl_coalesce_fold!(A, B, C, D, E, F, G);

#[doc(hidden)]
pub trait CoalesceNullabilityList {
    type Output;
}

impl<A> CoalesceNullabilityList for (A,) {
    type Output = A;
}

macro_rules! impl_coalesce_nullability_list {
    ($first:ident, $($rest:ident),+ $(,)?) => {
        impl<$first, $($rest),+> CoalesceNullabilityList for ($first, $($rest,)+)
        where
            ($($rest,)+): CoalesceFold<$first>,
        {
            type Output = <($($rest,)+) as CoalesceFold<$first>>::Output;
        }
    };
}

impl_coalesce_nullability_list!(A, B);
impl_coalesce_nullability_list!(A, B, C);
impl_coalesce_nullability_list!(A, B, C, D);
impl_coalesce_nullability_list!(A, B, C, D, E);
impl_coalesce_nullability_list!(A, B, C, D, E, F);
impl_coalesce_nullability_list!(A, B, C, D, E, F, G);
impl_coalesce_nullability_list!(A, B, C, D, E, F, G, H);

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
    In {
        expr: Box<ExprNode>,
        list: Vec<ExprNode>,
        negated: bool,
    },
    Between {
        expr: Box<ExprNode>,
        low: Box<ExprNode>,
        high: Box<ExprNode>,
        negated: bool,
    },
    Arithmetic {
        op: ArithmeticOp,
        left: Box<ExprNode>,
        right: Box<ExprNode>,
    },
    StringConcat {
        left: Box<ExprNode>,
        right: Box<ExprNode>,
    },
    Function {
        name: &'static str,
        args: Vec<ExprNode>,
    },
    Case {
        branches: Vec<(ExprNode, ExprNode)>,
        else_expr: Option<Box<ExprNode>>,
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

    pub fn in_<I, R>(self, list: I) -> Condition
    where
        I: IntoIterator<Item = R>,
        R: IntoExpr<T>,
    {
        self.in_list(list, false)
    }

    pub fn not_in<I, R>(self, list: I) -> Condition
    where
        I: IntoIterator<Item = R>,
        R: IntoExpr<T>,
    {
        self.in_list(list, true)
    }

    pub fn between<L, H>(self, low: L, high: H) -> Condition
    where
        L: IntoExpr<T>,
        H: IntoExpr<T>,
    {
        self.between_bounds(low, high, false)
    }

    pub fn not_between<L, H>(self, low: L, high: H) -> Condition
    where
        L: IntoExpr<T>,
        H: IntoExpr<T>,
    {
        self.between_bounds(low, high, true)
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

    fn in_list<I, R>(self, list: I, negated: bool) -> Condition
    where
        I: IntoIterator<Item = R>,
        R: IntoExpr<T>,
    {
        Condition {
            node: ExprNode::In {
                expr: Box::new(self.node),
                list: list
                    .into_iter()
                    .map(|item| item.into_expr().into_node())
                    .collect(),
                negated,
            },
        }
    }

    fn between_bounds<L, H>(self, low: L, high: H, negated: bool) -> Condition
    where
        L: IntoExpr<T>,
        H: IntoExpr<T>,
    {
        Condition {
            node: ExprNode::Between {
                expr: Box::new(self.node),
                low: Box::new(low.into_expr().into_node()),
                high: Box::new(high.into_expr().into_node()),
                negated,
            },
        }
    }
}

fn arithmetic_expr<T, N, R>(
    left: Expr<T, N>,
    op: ArithmeticOp,
    right: R,
) -> Expr<T, <N as NullableIfEither<R::Nullability>>::Output>
where
    R: IntoExpr<T>,
    N: NullableIfEither<R::Nullability>,
{
    Expr::from_node(ExprNode::Arithmetic {
        op,
        left: Box::new(left.into_node()),
        right: Box::new(right.into_expr().into_node()),
    })
}

impl<T, N, R> Add<R> for Expr<T, N>
where
    T: SqlNumeric,
    R: IntoExpr<T>,
    N: NullableIfEither<R::Nullability>,
{
    type Output = Expr<T, <N as NullableIfEither<R::Nullability>>::Output>;

    fn add(self, rhs: R) -> Self::Output {
        arithmetic_expr(self, ArithmeticOp::Add, rhs)
    }
}

impl<T, N, R> Sub<R> for Expr<T, N>
where
    T: SqlNumeric,
    R: IntoExpr<T>,
    N: NullableIfEither<R::Nullability>,
{
    type Output = Expr<T, <N as NullableIfEither<R::Nullability>>::Output>;

    fn sub(self, rhs: R) -> Self::Output {
        arithmetic_expr(self, ArithmeticOp::Sub, rhs)
    }
}

impl<T, N, R> Mul<R> for Expr<T, N>
where
    T: SqlNumeric,
    R: IntoExpr<T>,
    N: NullableIfEither<R::Nullability>,
{
    type Output = Expr<T, <N as NullableIfEither<R::Nullability>>::Output>;

    fn mul(self, rhs: R) -> Self::Output {
        arithmetic_expr(self, ArithmeticOp::Mul, rhs)
    }
}

impl<T, N, R> Div<R> for Expr<T, N>
where
    T: SqlNumeric,
    R: IntoExpr<T>,
    N: NullableIfEither<R::Nullability>,
{
    type Output = Expr<T, <N as NullableIfEither<R::Nullability>>::Output>;

    fn div(self, rhs: R) -> Self::Output {
        arithmetic_expr(self, ArithmeticOp::Div, rhs)
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

    pub fn in_<I, R>(self, list: I) -> Condition
    where
        I: IntoIterator<Item = R>,
        R: IntoExpr<T>,
    {
        self.expr().in_(list)
    }

    pub fn not_in<I, R>(self, list: I) -> Condition
    where
        I: IntoIterator<Item = R>,
        R: IntoExpr<T>,
    {
        self.expr().not_in(list)
    }

    pub fn between<L, H>(self, low: L, high: H) -> Condition
    where
        L: IntoExpr<T>,
        H: IntoExpr<T>,
    {
        self.expr().between(low, high)
    }

    pub fn not_between<L, H>(self, low: L, high: H) -> Condition
    where
        L: IntoExpr<T>,
        H: IntoExpr<T>,
    {
        self.expr().not_between(low, high)
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

/// A tuple of expressions accepted by `coalesce`.
#[doc(hidden)]
pub trait CoalesceArgs<T> {
    type Nullability;

    fn into_nodes(self) -> Vec<ExprNode>;
}

macro_rules! impl_coalesce_args {
    ($($ty:ident $var:ident),+ $(,)?) => {
        impl<T, $($ty),+> CoalesceArgs<T> for ($($ty,)+)
        where
            $($ty: IntoExpr<T>,)+
            ($($ty::Nullability,)+): CoalesceNullabilityList,
        {
            type Nullability = <($($ty::Nullability,)+) as CoalesceNullabilityList>::Output;

            fn into_nodes(self) -> Vec<ExprNode> {
                let ($($var,)+) = self;
                vec![$($var.into_expr().into_node(),)+]
            }
        }
    };
}

impl_coalesce_args!(A a);
impl_coalesce_args!(A a, B b);
impl_coalesce_args!(A a, B b, C c);
impl_coalesce_args!(A a, B b, C c, D d);
impl_coalesce_args!(A a, B b, C c, D d, E e);
impl_coalesce_args!(A a, B b, C c, D d, E e, F f);
impl_coalesce_args!(A a, B b, C c, D d, E e, F f, G g);
impl_coalesce_args!(A a, B b, C c, D d, E e, F f, G g, H h);

/// Concatenate two string expressions with PostgreSQL `||`.
pub fn concat<A, B>(
    left: A,
    right: B,
) -> Expr<String, <A::Nullability as NullableIfEither<B::Nullability>>::Output>
where
    A: IntoExpr<String>,
    B: IntoExpr<String>,
    A::Nullability: NullableIfEither<B::Nullability>,
{
    Expr::from_node(ExprNode::StringConcat {
        left: Box::new(left.into_expr().into_node()),
        right: Box::new(right.into_expr().into_node()),
    })
}

/// Render PostgreSQL `coalesce(...)`.
pub fn coalesce<T, A>(args: A) -> Expr<T, A::Nullability>
where
    A: CoalesceArgs<T>,
{
    Expr::from_node(ExprNode::Function {
        name: "coalesce",
        args: args.into_nodes(),
    })
}

/// Render PostgreSQL `nullif(a, b)`.
pub fn nullif<T, A, B>(left: A, right: B) -> Expr<T, Nullable>
where
    A: IntoExpr<T>,
    B: IntoExpr<T>,
{
    Expr::from_node(ExprNode::Function {
        name: "nullif",
        args: vec![left.into_expr().into_node(), right.into_expr().into_node()],
    })
}

/// Starting point for a typed SQL `case when ... then ... end` expression.
#[derive(Debug, Clone, Copy, Default)]
pub struct CaseWhenStart;

/// Builder for a typed SQL `case when ... then ... end` expression.
#[derive(Debug)]
pub struct CaseWhen<T, N> {
    branches: Vec<(ExprNode, ExprNode)>,
    marker: PhantomData<fn() -> (T, N)>,
}

/// Start a typed SQL `case when ... then ... end` expression.
pub fn case_when() -> CaseWhenStart {
    CaseWhenStart
}

impl CaseWhenStart {
    pub fn when<T, E>(self, condition: Condition, value: E) -> CaseWhen<T, E::Nullability>
    where
        E: IntoExpr<T>,
    {
        CaseWhen {
            branches: vec![(condition.into_node(), value.into_expr().into_node())],
            marker: PhantomData,
        }
    }
}

impl<T, N> CaseWhen<T, N> {
    pub fn when<E>(
        mut self,
        condition: Condition,
        value: E,
    ) -> CaseWhen<T, <N as NullableIfEither<E::Nullability>>::Output>
    where
        E: IntoExpr<T>,
        N: NullableIfEither<E::Nullability>,
    {
        self.branches
            .push((condition.into_node(), value.into_expr().into_node()));
        CaseWhen {
            branches: self.branches,
            marker: PhantomData,
        }
    }

    pub fn else_<E>(self, value: E) -> Expr<T, <N as NullableIfEither<E::Nullability>>::Output>
    where
        E: IntoExpr<T>,
        N: NullableIfEither<E::Nullability>,
    {
        Expr::from_node(ExprNode::Case {
            branches: self.branches,
            else_expr: Some(Box::new(value.into_expr().into_node())),
        })
    }

    pub fn end(self) -> Expr<T, Nullable> {
        Expr::from_node(ExprNode::Case {
            branches: self.branches,
            else_expr: None,
        })
    }
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
