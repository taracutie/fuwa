use std::marker::PhantomData;
use std::ops::{Add, Div, Mul, Not, Sub};

use rust_decimal::Decimal;
use uuid::Uuid;

use crate::{
    BindParam, ExprList, Field, NotNull, NotSingleColumn, NullabilityOutput, Nullable, OrderByList,
    SelectQuery, Table,
};

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
    JsonGet,
    JsonGetText,
    JsonPath,
    JsonPathText,
    Contains,
    HasKey,
    Overlaps,
    ArrayConcat,
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

/// PostgreSQL array comparison quantifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayQuantifier {
    Any,
    All,
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

/// Marker trait for SQL values backed by PostgreSQL `jsonb`.
#[doc(hidden)]
pub trait SqlJsonb {}

impl SqlJsonb for serde_json::Value {}

/// Marker trait for Rust element types that represent PostgreSQL array elements.
#[doc(hidden)]
pub trait SqlArrayElement {}

macro_rules! impl_sql_array_element {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl SqlArrayElement for $ty {}
        )+
    };
}

impl_sql_array_element!(
    i16,
    i32,
    i64,
    u32,
    f32,
    f64,
    bool,
    String,
    Uuid,
    chrono::NaiveDate,
    chrono::NaiveDateTime,
    chrono::DateTime<chrono::Utc>,
    Decimal,
    serde_json::Value,
    Vec<u8>,
);

/// Marker trait for SQL scalar types accepted by `array_agg`.
///
/// This intentionally excludes generated PostgreSQL array columns such as
/// `Field<Vec<String>>`: PostgreSQL's `array_agg(anyarray)` overload returns a
/// multidimensional array that `postgres-types` does not decode into nested
/// `Vec` values.
pub trait ArrayAggInput {}

macro_rules! impl_array_agg_input {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl ArrayAggInput for $ty {}
        )+
    };
}

impl_array_agg_input!(
    i16,
    i32,
    i64,
    u32,
    f32,
    f64,
    bool,
    String,
    Uuid,
    chrono::NaiveDate,
    chrono::NaiveDateTime,
    chrono::DateTime<chrono::Utc>,
    Decimal,
    serde_json::Value,
    Vec<u8>,
);

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
impl_coalesce_fold!(A, B, C, D, E, F, G, H);
impl_coalesce_fold!(A, B, C, D, E, F, G, H, I);
impl_coalesce_fold!(A, B, C, D, E, F, G, H, I, J);
impl_coalesce_fold!(A, B, C, D, E, F, G, H, I, J, K);
impl_coalesce_fold!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_coalesce_fold!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_coalesce_fold!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_coalesce_fold!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);

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
impl_coalesce_nullability_list!(A, B, C, D, E, F, G, H, I);
impl_coalesce_nullability_list!(A, B, C, D, E, F, G, H, I, J);
impl_coalesce_nullability_list!(A, B, C, D, E, F, G, H, I, J, K);
impl_coalesce_nullability_list!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_coalesce_nullability_list!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_coalesce_nullability_list!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_coalesce_nullability_list!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_coalesce_nullability_list!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

/// Runtime SQL expression AST.
#[derive(Debug, Clone)]
pub enum ExprNode {
    Field(FieldRef),
    ExcludedField(FieldRef),
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
        operand: InOperandNode,
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
    ArrayComparison {
        quantifier: ArrayQuantifier,
        left: Box<ExprNode>,
        array: Box<ExprNode>,
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
    Window {
        func: Box<ExprNode>,
        spec: Box<WindowSpec>,
    },
    Exists {
        query: Box<SelectQuery<(), NotSingleColumn>>,
        negated: bool,
    },
    Cast {
        expr: Box<ExprNode>,
        sql_type: &'static str,
    },
    DateTrunc {
        field: &'static str,
        expr: Box<ExprNode>,
    },
    Extract {
        field: &'static str,
        expr: Box<ExprNode>,
    },
    Bool(bool),
    Star,
}

/// Runtime operand for an `IN` or `NOT IN` predicate.
#[derive(Debug, Clone)]
pub struct InOperandNode {
    pub(crate) kind: InOperandKind,
}

#[derive(Debug, Clone)]
pub(crate) enum InOperandKind {
    List(Vec<ExprNode>),
    Subquery(Box<SelectQuery<(), NotSingleColumn>>),
}

impl InOperandNode {
    fn list(list: Vec<ExprNode>) -> Self {
        Self {
            kind: InOperandKind::List(list),
        }
    }

    fn subquery<R, S>(query: SelectQuery<R, S>) -> Self {
        Self {
            kind: InOperandKind::Subquery(Box::new(query.erase_record())),
        }
    }
}

/// A typed SQL expression.
#[derive(Debug, Clone)]
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

    pub fn in_<I>(self, operand: I) -> Condition
    where
        I: InOperand<T>,
    {
        self.in_operand(operand, false)
    }

    pub fn not_in<I>(self, operand: I) -> Condition
    where
        I: InOperand<T>,
    {
        self.in_operand(operand, true)
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

    fn in_operand<I>(self, operand: I, negated: bool) -> Condition
    where
        I: InOperand<T>,
    {
        Condition {
            node: ExprNode::In {
                expr: Box::new(self.node),
                operand: operand.into_in_operand(),
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

fn binary_expr<T>(left: ExprNode, op: BinaryOp, right: ExprNode) -> Expr<T, Nullable> {
    Expr::from_node(ExprNode::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    })
}

fn binary_condition(left: ExprNode, op: BinaryOp, right: ExprNode) -> Condition {
    Condition {
        node: ExprNode::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        },
    }
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

impl<T, N> Expr<T, N>
where
    T: SqlJsonb,
{
    pub fn json_get<K>(self, key: K) -> Expr<serde_json::Value, Nullable>
    where
        K: IntoExpr<String>,
    {
        binary_expr(
            self.into_node(),
            BinaryOp::JsonGet,
            key.into_expr().into_node(),
        )
    }

    pub fn json_get_text<K>(self, key: K) -> Expr<String, Nullable>
    where
        K: IntoExpr<String>,
    {
        binary_expr(
            self.into_node(),
            BinaryOp::JsonGetText,
            key.into_expr().into_node(),
        )
    }

    pub fn json_path<P>(self, path: P) -> Expr<serde_json::Value, Nullable>
    where
        P: IntoExpr<Vec<String>>,
    {
        binary_expr(
            self.into_node(),
            BinaryOp::JsonPath,
            path.into_expr().into_node(),
        )
    }

    pub fn json_path_text<P>(self, path: P) -> Expr<String, Nullable>
    where
        P: IntoExpr<Vec<String>>,
    {
        binary_expr(
            self.into_node(),
            BinaryOp::JsonPathText,
            path.into_expr().into_node(),
        )
    }

    pub fn contains<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        binary_condition(
            self.into_node(),
            BinaryOp::Contains,
            rhs.into_expr().into_node(),
        )
    }

    pub fn has_key<K>(self, key: K) -> Condition
    where
        K: IntoExpr<String>,
    {
        binary_condition(
            self.into_node(),
            BinaryOp::HasKey,
            key.into_expr().into_node(),
        )
    }
}

impl<T, N> Expr<Vec<T>, N>
where
    T: SqlArrayElement,
{
    pub fn contains<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<Vec<T>>,
    {
        binary_condition(
            self.into_node(),
            BinaryOp::Contains,
            rhs.into_expr().into_node(),
        )
    }

    pub fn overlaps<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<Vec<T>>,
    {
        binary_condition(
            self.into_node(),
            BinaryOp::Overlaps,
            rhs.into_expr().into_node(),
        )
    }

    pub fn concat<R>(self, rhs: R) -> Expr<Vec<T>, <N as NullableIfEither<R::Nullability>>::Output>
    where
        R: IntoExpr<Vec<T>>,
        N: NullableIfEither<R::Nullability>,
    {
        Expr::from_node(ExprNode::Binary {
            op: BinaryOp::ArrayConcat,
            left: Box::new(self.into_node()),
            right: Box::new(rhs.into_expr().into_node()),
        })
    }
}

impl<T, N> Expr<T, N>
where
    T: SqlArrayElement,
{
    pub fn eq_any<A>(self, array: A) -> Condition
    where
        A: IntoExpr<Vec<T>>,
    {
        Condition {
            node: ExprNode::ArrayComparison {
                quantifier: ArrayQuantifier::Any,
                left: Box::new(self.into_node()),
                array: Box::new(array.into_expr().into_node()),
            },
        }
    }

    pub fn eq_all<A>(self, array: A) -> Condition
    where
        A: IntoExpr<Vec<T>>,
    {
        Condition {
            node: ExprNode::ArrayComparison {
                quantifier: ArrayQuantifier::All,
                left: Box::new(self.into_node()),
                array: Box::new(array.into_expr().into_node()),
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

    pub fn in_<I>(self, operand: I) -> Condition
    where
        I: InOperand<T>,
    {
        self.expr().in_(operand)
    }

    pub fn not_in<I>(self, operand: I) -> Condition
    where
        I: InOperand<T>,
    {
        self.expr().not_in(operand)
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

impl<T, N> Field<T, N>
where
    T: SqlJsonb,
{
    pub fn json_get<K>(self, key: K) -> Expr<serde_json::Value, Nullable>
    where
        K: IntoExpr<String>,
    {
        self.expr().json_get(key)
    }

    pub fn json_get_text<K>(self, key: K) -> Expr<String, Nullable>
    where
        K: IntoExpr<String>,
    {
        self.expr().json_get_text(key)
    }

    pub fn json_path<P>(self, path: P) -> Expr<serde_json::Value, Nullable>
    where
        P: IntoExpr<Vec<String>>,
    {
        self.expr().json_path(path)
    }

    pub fn json_path_text<P>(self, path: P) -> Expr<String, Nullable>
    where
        P: IntoExpr<Vec<String>>,
    {
        self.expr().json_path_text(path)
    }

    pub fn contains<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<T>,
    {
        self.expr().contains(rhs)
    }

    pub fn has_key<K>(self, key: K) -> Condition
    where
        K: IntoExpr<String>,
    {
        self.expr().has_key(key)
    }
}

impl<T, N> Field<Vec<T>, N>
where
    T: SqlArrayElement,
{
    pub fn contains<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<Vec<T>>,
    {
        self.expr().contains(rhs)
    }

    pub fn overlaps<R>(self, rhs: R) -> Condition
    where
        R: IntoExpr<Vec<T>>,
    {
        self.expr().overlaps(rhs)
    }

    pub fn concat<R>(self, rhs: R) -> Expr<Vec<T>, <N as NullableIfEither<R::Nullability>>::Output>
    where
        R: IntoExpr<Vec<T>>,
        N: NullableIfEither<R::Nullability>,
    {
        self.expr().concat(rhs)
    }
}

impl<T, N> Field<T, N>
where
    T: SqlArrayElement,
{
    pub fn eq_any<A>(self, array: A) -> Condition
    where
        A: IntoExpr<Vec<T>>,
    {
        self.expr().eq_any(array)
    }

    pub fn eq_all<A>(self, array: A) -> Condition
    where
        A: IntoExpr<Vec<T>>,
    {
        self.expr().eq_all(array)
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

impl<T, V> IntoExpr<T> for V
where
    V: crate::IntoBindValue<Sql = T>,
{
    type Nullability = V::Nullability;

    fn into_expr(self) -> Expr<T, Self::Nullability> {
        crate::bind(self)
    }
}

/// Something accepted on the right-hand side of `IN` or `NOT IN`.
///
/// Subquery operands must select exactly one SQL expression whose SQL type
/// matches the left-hand expression.
///
/// ```compile_fail
/// use fuwa_core::{Context, Field, NotNull, Table};
///
/// let ctx = Context::new();
/// let users = Table::new("public", "users");
/// let posts = Table::new("blog", "posts");
/// let user_email: Field<String, NotNull> = users.field("email");
/// let post_id: Field<i64, NotNull> = posts.field("id");
///
/// let _ = user_email.in_(ctx.select(post_id).from(posts));
/// ```
///
/// ```compile_fail
/// use fuwa_core::{Context, Field, NotNull, Table};
///
/// let ctx = Context::new();
/// let users = Table::new("public", "users");
/// let posts = Table::new("blog", "posts");
/// let user_id: Field<i64, NotNull> = users.field("id");
/// let post_user_id: Field<i64, NotNull> = posts.field("user_id");
/// let post_title: Field<String, NotNull> = posts.field("title");
///
/// let _ = user_id.in_(ctx.select((post_user_id, post_title)).from(posts));
/// ```
pub trait InOperand<T> {
    #[doc(hidden)]
    fn into_in_operand(self) -> InOperandNode;
}

impl<T, R, const N: usize> InOperand<T> for [R; N]
where
    R: IntoExpr<T>,
{
    fn into_in_operand(self) -> InOperandNode {
        InOperandNode::list(
            self.into_iter()
                .map(|item| item.into_expr().into_node())
                .collect(),
        )
    }
}

impl<T, R> InOperand<T> for Vec<R>
where
    R: IntoExpr<T>,
{
    fn into_in_operand(self) -> InOperandNode {
        InOperandNode::list(
            self.into_iter()
                .map(|item| item.into_expr().into_node())
                .collect(),
        )
    }
}

impl<T, R> InOperand<T> for SelectQuery<R, T> {
    fn into_in_operand(self) -> InOperandNode {
        InOperandNode::subquery(self)
    }
}

/// A boolean SQL condition.
#[derive(Debug, Clone)]
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

    /// Combine an iterator of conditions with `AND`. Returns `TRUE` when empty.
    pub fn all<I>(conditions: I) -> Condition
    where
        I: IntoIterator<Item = Condition>,
    {
        let mut iter = conditions.into_iter();
        match iter.next() {
            None => Self::true_(),
            Some(first) => iter.fold(first, |acc, next| acc.and(next)),
        }
    }

    /// Combine an iterator of conditions with `OR`. Returns `FALSE` when empty.
    pub fn any<I>(conditions: I) -> Condition
    where
        I: IntoIterator<Item = Condition>,
    {
        let mut iter = conditions.into_iter();
        match iter.next() {
            None => Self::false_(),
            Some(first) => iter.fold(first, |acc, next| acc.or(next)),
        }
    }

    /// Construct the SQL constant `TRUE`.
    pub fn true_() -> Condition {
        Condition {
            node: ExprNode::Bool(true),
        }
    }

    /// Construct the SQL constant `FALSE`.
    pub fn false_() -> Condition {
        Condition {
            node: ExprNode::Bool(false),
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

/// Anything that can be converted into a [`Condition`].
///
/// Tuples up to 16 elements are supported; each element must implement
/// `IntoCondition`. Tuple inputs are AND-folded so `where_((a, b, c))`
/// is equivalent to `where_(a.and(b).and(c))`.
pub trait IntoCondition {
    fn into_condition(self) -> Condition;
}

impl IntoCondition for Condition {
    fn into_condition(self) -> Condition {
        self
    }
}

macro_rules! impl_tuple_into_condition {
    ($($ty:ident $var:ident),+ $(,)?) => {
        impl<$($ty),+> IntoCondition for ($($ty,)+)
        where
            $($ty: IntoCondition),+
        {
            fn into_condition(self) -> Condition {
                let ($($var,)+) = self;
                Condition::all([$($var.into_condition()),+])
            }
        }
    };
}

impl_tuple_into_condition!(A a);
impl_tuple_into_condition!(A a, B b);
impl_tuple_into_condition!(A a, B b, C c);
impl_tuple_into_condition!(A a, B b, C c, D d);
impl_tuple_into_condition!(A a, B b, C c, D d, E e);
impl_tuple_into_condition!(A a, B b, C c, D d, E e, F f);
impl_tuple_into_condition!(A a, B b, C c, D d, E e, F f, G g);
impl_tuple_into_condition!(A a, B b, C c, D d, E e, F f, G g, H h);
impl_tuple_into_condition!(A a, B b, C c, D d, E e, F f, G g, H h, I i);
impl_tuple_into_condition!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
impl_tuple_into_condition!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);
impl_tuple_into_condition!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l);
impl_tuple_into_condition!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m);
impl_tuple_into_condition!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n);
impl_tuple_into_condition!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o);
impl_tuple_into_condition!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o, P p);

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
impl_coalesce_args!(A a, B b, C c, D d, E e, F f, G g, H h, I i);
impl_coalesce_args!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
impl_coalesce_args!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k);
impl_coalesce_args!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l);
impl_coalesce_args!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m);
impl_coalesce_args!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n);
impl_coalesce_args!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o);
impl_coalesce_args!(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j, K k, L l, M m, N n, O o, P p);

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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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

/// PostgreSQL return type for `avg(expression)`.
pub trait AvgOutput {
    type Output;
}

impl AvgOutput for i16 {
    type Output = Decimal;
}

impl AvgOutput for i32 {
    type Output = Decimal;
}

impl AvgOutput for i64 {
    type Output = Decimal;
}

impl AvgOutput for Decimal {
    type Output = Decimal;
}

impl AvgOutput for f32 {
    type Output = f64;
}

impl AvgOutput for f64 {
    type Output = f64;
}

/// PostgreSQL return type for `round`, `ceil`, and `floor`.
pub trait RoundingOutput {
    type Output;
}

impl RoundingOutput for i16 {
    type Output = Decimal;
}

impl RoundingOutput for i32 {
    type Output = Decimal;
}

impl RoundingOutput for i64 {
    type Output = Decimal;
}

impl RoundingOutput for Decimal {
    type Output = Decimal;
}

impl RoundingOutput for f32 {
    type Output = f64;
}

impl RoundingOutput for f64 {
    type Output = f64;
}

/// PostgreSQL return type for `date_trunc`.
#[doc(hidden)]
pub trait DateTruncOutput {
    type Output;
}

impl DateTruncOutput for chrono::NaiveDateTime {
    type Output = chrono::NaiveDateTime;
}

impl DateTruncOutput for chrono::DateTime<chrono::Utc> {
    type Output = chrono::DateTime<chrono::Utc>;
}

/// PostgreSQL types accepted by `extract(field from expression)`.
#[doc(hidden)]
pub trait SqlTemporal {}

impl SqlTemporal for chrono::NaiveDate {}
impl SqlTemporal for chrono::NaiveDateTime {}
impl SqlTemporal for chrono::DateTime<chrono::Utc> {}

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

/// Render `min(expression)`.
pub fn min<T, E>(expr: E) -> Expr<T, Nullable>
where
    E: IntoExpr<T>,
{
    Expr::from_node(ExprNode::Function {
        name: "min",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render `max(expression)`.
pub fn max<T, E>(expr: E) -> Expr<T, Nullable>
where
    E: IntoExpr<T>,
{
    Expr::from_node(ExprNode::Function {
        name: "max",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render `avg(expression)`.
pub fn avg<T, E>(expr: E) -> Expr<T::Output, Nullable>
where
    E: IntoExpr<T>,
    T: AvgOutput,
{
    Expr::from_node(ExprNode::Function {
        name: "avg",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render `array_agg(expression)`.
///
/// PostgreSQL array inputs are rejected at the Rust type level because
/// `array_agg(anyarray)` returns a multidimensional array, which is not
/// supported by this crate's `Vec<T>` decoding path.
///
/// ```compile_fail
/// use fuwa_core::{array_agg, Field, NotNull, Table};
///
/// let table = Table::new("public", "users");
/// let tags: Field<Vec<String>, NotNull> = table.field("tags");
/// let _ = array_agg(tags);
/// ```
pub fn array_agg<T, E>(
    expr: E,
) -> Expr<Vec<<E::Nullability as NullabilityOutput<T>>::Output>, Nullable>
where
    E: IntoExpr<T>,
    T: ArrayAggInput,
    E::Nullability: NullabilityOutput<T>,
{
    Expr::from_node(ExprNode::Function {
        name: "array_agg",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render `string_agg(expression, delimiter)`.
pub fn string_agg<E, D>(expr: E, delimiter: D) -> Expr<String, Nullable>
where
    E: IntoExpr<String>,
    D: IntoExpr<String>,
{
    Expr::from_node(ExprNode::Function {
        name: "string_agg",
        args: vec![
            expr.into_expr().into_node(),
            delimiter.into_expr().into_node(),
        ],
    })
}

/// Render `bool_and(expression)`.
pub fn bool_and<E>(expr: E) -> Expr<bool, Nullable>
where
    E: IntoExpr<bool>,
{
    Expr::from_node(ExprNode::Function {
        name: "bool_and",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render `bool_or(expression)`.
pub fn bool_or<E>(expr: E) -> Expr<bool, Nullable>
where
    E: IntoExpr<bool>,
{
    Expr::from_node(ExprNode::Function {
        name: "bool_or",
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

/// Render PostgreSQL `greatest(...)`. Nullable only when every argument is nullable.
pub fn greatest<T, A>(args: A) -> Expr<T, A::Nullability>
where
    A: CoalesceArgs<T>,
{
    Expr::from_node(ExprNode::Function {
        name: "greatest",
        args: args.into_nodes(),
    })
}

/// Render PostgreSQL `least(...)`. Nullable only when every argument is nullable.
pub fn least<T, A>(args: A) -> Expr<T, A::Nullability>
where
    A: CoalesceArgs<T>,
{
    Expr::from_node(ExprNode::Function {
        name: "least",
        args: args.into_nodes(),
    })
}

/// Render PostgreSQL `length(text_expr)`.
pub fn length<E>(expr: E) -> Expr<i32, E::Nullability>
where
    E: IntoExpr<String>,
{
    Expr::from_node(ExprNode::Function {
        name: "length",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render PostgreSQL `lower(text_expr)`.
pub fn lower<E>(expr: E) -> Expr<String, E::Nullability>
where
    E: IntoExpr<String>,
{
    Expr::from_node(ExprNode::Function {
        name: "lower",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render PostgreSQL `upper(text_expr)`.
pub fn upper<E>(expr: E) -> Expr<String, E::Nullability>
where
    E: IntoExpr<String>,
{
    Expr::from_node(ExprNode::Function {
        name: "upper",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render PostgreSQL `trim(text_expr)`.
pub fn trim<E>(expr: E) -> Expr<String, E::Nullability>
where
    E: IntoExpr<String>,
{
    Expr::from_node(ExprNode::Function {
        name: "trim",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render PostgreSQL `abs(numeric_expr)`.
pub fn abs<T, E>(expr: E) -> Expr<T, E::Nullability>
where
    E: IntoExpr<T>,
    T: SqlNumeric,
{
    Expr::from_node(ExprNode::Function {
        name: "abs",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render PostgreSQL `round(numeric_expr)`.
pub fn round<T, E>(expr: E) -> Expr<T::Output, E::Nullability>
where
    E: IntoExpr<T>,
    T: RoundingOutput,
{
    Expr::from_node(ExprNode::Function {
        name: "round",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render PostgreSQL `ceil(numeric_expr)`.
pub fn ceil<T, E>(expr: E) -> Expr<T::Output, E::Nullability>
where
    E: IntoExpr<T>,
    T: RoundingOutput,
{
    Expr::from_node(ExprNode::Function {
        name: "ceil",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render PostgreSQL `floor(numeric_expr)`.
pub fn floor<T, E>(expr: E) -> Expr<T::Output, E::Nullability>
where
    E: IntoExpr<T>,
    T: RoundingOutput,
{
    Expr::from_node(ExprNode::Function {
        name: "floor",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render PostgreSQL `now()`.
pub fn now() -> Expr<chrono::DateTime<chrono::Utc>, NotNull> {
    Expr::from_node(ExprNode::Function {
        name: "now",
        args: Vec::new(),
    })
}

/// Render PostgreSQL `date_trunc(field, timestamp_expr)`.
///
/// ```compile_fail
/// use fuwa_core::{date_trunc, Field, NotNull, Table};
///
/// let table = Table::new("public", "events");
/// let day: Field<chrono::NaiveDate, NotNull> = table.field("day");
/// let _ = date_trunc("day", day);
/// ```
pub fn date_trunc<E, T>(field: &'static str, expr: E) -> Expr<T::Output, E::Nullability>
where
    E: IntoExpr<T>,
    T: DateTruncOutput,
{
    Expr::from_node(ExprNode::DateTrunc {
        field,
        expr: Box::new(expr.into_expr().into_node()),
    })
}

/// Render PostgreSQL `extract(field from timestamp_expr)`.
///
/// ```compile_fail
/// use fuwa_core::{extract, Field, NotNull, Table};
///
/// let table = Table::new("public", "users");
/// let id: Field<i64, NotNull> = table.field("id");
/// let _ = extract("year", id);
/// ```
pub fn extract<E, T>(field: &'static str, expr: E) -> Expr<rust_decimal::Decimal, E::Nullability>
where
    E: IntoExpr<T>,
    T: SqlTemporal,
{
    Expr::from_node(ExprNode::Extract {
        field,
        expr: Box::new(expr.into_expr().into_node()),
    })
}

/// Render a typed PostgreSQL `CAST(expr AS type)`.
pub fn cast<T, S, E>(expr: E) -> Expr<S, E::Nullability>
where
    E: IntoExpr<T>,
    S: SqlType,
{
    Expr::from_node(ExprNode::Cast {
        expr: Box::new(expr.into_expr().into_node()),
        sql_type: S::SQL_TYPE,
    })
}

/// Maps Rust scalar types to a PostgreSQL type name suitable for `CAST(... AS ...)`.
pub trait SqlType {
    const SQL_TYPE: &'static str;
}

impl SqlType for i16 {
    const SQL_TYPE: &'static str = "int2";
}
impl SqlType for i32 {
    const SQL_TYPE: &'static str = "int4";
}
impl SqlType for i64 {
    const SQL_TYPE: &'static str = "int8";
}
impl SqlType for f32 {
    const SQL_TYPE: &'static str = "float4";
}
impl SqlType for f64 {
    const SQL_TYPE: &'static str = "float8";
}
impl SqlType for rust_decimal::Decimal {
    const SQL_TYPE: &'static str = "numeric";
}
impl SqlType for bool {
    const SQL_TYPE: &'static str = "bool";
}
impl SqlType for String {
    const SQL_TYPE: &'static str = "text";
}
impl SqlType for serde_json::Value {
    const SQL_TYPE: &'static str = "jsonb";
}
impl SqlType for Uuid {
    const SQL_TYPE: &'static str = "uuid";
}
impl SqlType for chrono::NaiveDate {
    const SQL_TYPE: &'static str = "date";
}
impl SqlType for chrono::NaiveDateTime {
    const SQL_TYPE: &'static str = "timestamp";
}
impl SqlType for chrono::DateTime<chrono::Utc> {
    const SQL_TYPE: &'static str = "timestamptz";
}
impl SqlType for Vec<u8> {
    const SQL_TYPE: &'static str = "bytea";
}

/// Render PostgreSQL `EXISTS (subquery)`.
pub fn exists<R, S, Q>(query: Q) -> Condition
where
    Q: IntoExistsQuery<R, S>,
{
    Condition {
        node: ExprNode::Exists {
            query: Box::new(query.into_select_query()),
            negated: false,
        },
    }
}

/// Render PostgreSQL `NOT EXISTS (subquery)`.
pub fn not_exists<R, S, Q>(query: Q) -> Condition
where
    Q: IntoExistsQuery<R, S>,
{
    Condition {
        node: ExprNode::Exists {
            query: Box::new(query.into_select_query()),
            negated: true,
        },
    }
}

/// Convertible into a `SELECT` query erased of its record type for use with `EXISTS`.
pub trait IntoExistsQuery<R, S> {
    #[doc(hidden)]
    fn into_select_query(self) -> SelectQuery<(), NotSingleColumn>;
}

impl<R, S> IntoExistsQuery<R, S> for SelectQuery<R, S> {
    fn into_select_query(self) -> SelectQuery<(), NotSingleColumn> {
        self.erase_record()
    }
}

/// SQL window function frame unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameUnit {
    Rows,
    Range,
    Groups,
}

/// SQL window function frame bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
}

/// SQL window function frame.
#[derive(Debug, Clone)]
pub struct WindowFrame {
    pub(crate) unit: WindowFrameUnit,
    pub(crate) start: WindowFrameBound,
    pub(crate) end: WindowFrameBound,
}

/// SQL `OVER (...)` window specification.
#[derive(Debug, Clone, Default)]
pub struct WindowSpec {
    pub(crate) partition_by: Vec<ExprNode>,
    pub(crate) order_by: Vec<OrderExpr>,
    pub(crate) frame: Option<WindowFrame>,
}

impl WindowSpec {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn partition_by<E>(mut self, cols: E) -> Self
    where
        E: ExprList,
    {
        self.partition_by.extend(cols.into_exprs());
        self
    }

    pub fn order_by<O>(mut self, order: O) -> Self
    where
        O: OrderByList,
    {
        self.order_by.extend(order.into_order_by());
        self
    }

    pub fn rows_between(mut self, start: WindowFrameBound, end: WindowFrameBound) -> Self {
        self.frame = Some(WindowFrame {
            unit: WindowFrameUnit::Rows,
            start,
            end,
        });
        self
    }

    pub fn range_between(mut self, start: WindowFrameBound, end: WindowFrameBound) -> Self {
        self.frame = Some(WindowFrame {
            unit: WindowFrameUnit::Range,
            start,
            end,
        });
        self
    }

    pub fn groups_between(mut self, start: WindowFrameBound, end: WindowFrameBound) -> Self {
        self.frame = Some(WindowFrame {
            unit: WindowFrameUnit::Groups,
            start,
            end,
        });
        self
    }
}

/// Begin a window specification with `PARTITION BY ...`.
pub fn partition_by<E>(cols: E) -> WindowSpec
where
    E: ExprList,
{
    WindowSpec::new().partition_by(cols)
}

/// Window frame bound `UNBOUNDED PRECEDING`.
pub fn unbounded_preceding() -> WindowFrameBound {
    WindowFrameBound::UnboundedPreceding
}

/// Window frame bound `UNBOUNDED FOLLOWING`.
pub fn unbounded_following() -> WindowFrameBound {
    WindowFrameBound::UnboundedFollowing
}

/// Window frame bound `CURRENT ROW`.
pub fn current_row() -> WindowFrameBound {
    WindowFrameBound::CurrentRow
}

/// Window frame bound `N PRECEDING`.
pub fn preceding(n: i64) -> WindowFrameBound {
    WindowFrameBound::Preceding(n)
}

/// Window frame bound `N FOLLOWING`.
pub fn following(n: i64) -> WindowFrameBound {
    WindowFrameBound::Following(n)
}

/// A pure SQL window function that requires an `OVER (...)` clause to be selectable.
#[derive(Debug, Clone)]
pub struct WindowFunction<T, N = NotNull> {
    node: ExprNode,
    marker: PhantomData<fn() -> (T, N)>,
}

impl<T, N> WindowFunction<T, N> {
    fn from_node(node: ExprNode) -> Self {
        Self {
            node,
            marker: PhantomData,
        }
    }

    pub fn over(self, spec: WindowSpec) -> Expr<T, N> {
        Expr::from_node(ExprNode::Window {
            func: Box::new(self.node),
            spec: Box::new(spec),
        })
    }
}

/// Render `row_number() OVER (...)`.
pub fn row_number() -> WindowFunction<i64, NotNull> {
    WindowFunction::from_node(ExprNode::Function {
        name: "row_number",
        args: Vec::new(),
    })
}

/// Render `rank() OVER (...)`.
pub fn rank() -> WindowFunction<i64, NotNull> {
    WindowFunction::from_node(ExprNode::Function {
        name: "rank",
        args: Vec::new(),
    })
}

/// Render `dense_rank() OVER (...)`.
pub fn dense_rank() -> WindowFunction<i64, NotNull> {
    WindowFunction::from_node(ExprNode::Function {
        name: "dense_rank",
        args: Vec::new(),
    })
}

/// Render `lag(expr) OVER (...)`. Always nullable since edge rows have no predecessor.
pub fn lag<T, E>(expr: E) -> WindowFunction<T, Nullable>
where
    E: IntoExpr<T>,
{
    WindowFunction::from_node(ExprNode::Function {
        name: "lag",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render `lead(expr) OVER (...)`. Always nullable since edge rows have no successor.
pub fn lead<T, E>(expr: E) -> WindowFunction<T, Nullable>
where
    E: IntoExpr<T>,
{
    WindowFunction::from_node(ExprNode::Function {
        name: "lead",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render `first_value(expr) OVER (...)`.
pub fn first_value<T, E>(expr: E) -> WindowFunction<T, Nullable>
where
    E: IntoExpr<T>,
{
    WindowFunction::from_node(ExprNode::Function {
        name: "first_value",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render `last_value(expr) OVER (...)`.
pub fn last_value<T, E>(expr: E) -> WindowFunction<T, Nullable>
where
    E: IntoExpr<T>,
{
    WindowFunction::from_node(ExprNode::Function {
        name: "last_value",
        args: vec![expr.into_expr().into_node()],
    })
}

/// Render `ntile(buckets) OVER (...)`.
pub fn ntile(buckets: i32) -> WindowFunction<i32, NotNull> {
    WindowFunction::from_node(ExprNode::Function {
        name: "ntile",
        args: vec![ExprNode::Bind(BindParam::new(std::sync::Arc::new(buckets)))],
    })
}

impl<T, N> Expr<T, N> {
    /// Wrap this expression in `OVER (...)` to produce a window expression.
    pub fn over(self, spec: WindowSpec) -> Expr<T, N> {
        Expr::from_node(ExprNode::Window {
            func: Box::new(self.into_node()),
            spec: Box::new(spec),
        })
    }

    /// Attach a SQL column alias for use in `SELECT` and `RETURNING` lists.
    pub fn as_(self, alias: &'static str) -> AliasedExpr<T, N> {
        AliasedExpr {
            expr: self.into_node(),
            alias,
            marker: PhantomData,
        }
    }
}

impl<T, N> Field<T, N> {
    /// Attach a SQL column alias for use in `SELECT` and `RETURNING` lists.
    pub fn as_(self, alias: &'static str) -> AliasedExpr<T, N> {
        self.expr().as_(alias)
    }
}

/// A SQL expression with an explicit column alias.
#[derive(Debug, Clone)]
pub struct AliasedExpr<T, N = NotNull> {
    pub(crate) expr: ExprNode,
    pub(crate) alias: &'static str,
    marker: PhantomData<fn() -> (T, N)>,
}
