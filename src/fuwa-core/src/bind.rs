use std::fmt;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use postgres_types::ToSql;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::{Expr, ExprNode, NotNull, Nullable};

/// Owned value stored separately from SQL text and passed to PostgreSQL as a bind.
pub type BindValue = Box<dyn ToSql + Sync + Send>;

/// A single owned bind parameter.
pub struct BindParam {
    value: BindValue,
}

impl BindParam {
    pub fn new(value: BindValue) -> Self {
        Self { value }
    }

    pub(crate) fn into_value(self) -> BindValue {
        self.value
    }
}

impl fmt::Debug for BindParam {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("BindParam").field(&"..").finish()
    }
}

/// Convert a Rust value into an owned PostgreSQL bind value with a SQL marker type.
pub trait IntoBindValue {
    type Sql: 'static;
    type Nullability;
    type Stored: ToSql + Sync + Send + 'static;

    fn into_stored(self) -> Self::Stored;
}

macro_rules! impl_bind_scalar {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl IntoBindValue for $ty {
                type Sql = $ty;
                type Nullability = NotNull;
                type Stored = $ty;

                fn into_stored(self) -> Self::Stored {
                    self
                }
            }
        )+
    };
}

impl_bind_scalar!(
    i16,
    i32,
    i64,
    f32,
    f64,
    bool,
    String,
    Uuid,
    NaiveDate,
    NaiveDateTime,
    DateTime<Utc>,
    Decimal,
    serde_json::Value,
);

impl IntoBindValue for &str {
    type Sql = String;
    type Nullability = NotNull;
    type Stored = String;

    fn into_stored(self) -> Self::Stored {
        self.to_owned()
    }
}

impl IntoBindValue for &String {
    type Sql = String;
    type Nullability = NotNull;
    type Stored = String;

    fn into_stored(self) -> Self::Stored {
        self.clone()
    }
}

impl<T> IntoBindValue for Vec<T>
where
    T: IntoBindValue<Nullability = NotNull>,
    Vec<T::Stored>: ToSql + Sync + Send + 'static,
{
    type Sql = Vec<T::Sql>;
    type Nullability = NotNull;
    type Stored = Vec<T::Stored>;

    fn into_stored(self) -> Self::Stored {
        self.into_iter().map(IntoBindValue::into_stored).collect()
    }
}

impl<T> IntoBindValue for &[T]
where
    T: Clone + IntoBindValue<Nullability = NotNull>,
    Vec<T::Stored>: ToSql + Sync + Send + 'static,
{
    type Sql = Vec<T::Sql>;
    type Nullability = NotNull;
    type Stored = Vec<T::Stored>;

    fn into_stored(self) -> Self::Stored {
        self.iter()
            .cloned()
            .map(IntoBindValue::into_stored)
            .collect()
    }
}

impl<T> IntoBindValue for Option<T>
where
    T: IntoBindValue<Nullability = NotNull>,
    Option<T::Stored>: ToSql + Sync + Send + 'static,
{
    type Sql = T::Sql;
    type Nullability = Nullable;
    type Stored = Option<T::Stored>;

    fn into_stored(self) -> Self::Stored {
        self.map(IntoBindValue::into_stored)
    }
}

/// Create a typed bind expression.
pub fn bind<T>(value: T) -> Expr<T::Sql, T::Nullability>
where
    T: IntoBindValue,
{
    Expr::from_node(ExprNode::Bind(BindParam::new(Box::new(
        value.into_stored(),
    ))))
}
