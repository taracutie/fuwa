//! Async `tokio-postgres` execution and row decoding for `fuwa`.

use std::future::Future;
use std::pin::Pin;

use fuwa_core::{Error, RenderQuery, Result};
use tokio_postgres::types::FromSqlOwned;
pub use tokio_postgres::Row;
use tokio_postgres::{GenericClient, Row as PgRow};

/// Boxed future returned by `fuwa-postgres` extension methods.
pub type PgFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Decode a `tokio-postgres` row into a Rust value.
pub trait FromRow: Sized {
    fn from_row(row: &PgRow) -> Result<Self>;
}

fn ensure_width(row: &PgRow, expected: usize) -> Result<()> {
    let actual = row.len();
    if actual != expected {
        return Err(Error::row_decode(format!(
            "expected {expected} columns, got {actual}"
        )));
    }
    Ok(())
}

fn decode_column<T>(row: &PgRow, index: usize) -> Result<T>
where
    T: FromSqlOwned,
{
    row.try_get(index)
        .map_err(|err| Error::row_decode(format!("failed to decode column {index}: {err}")))
}

macro_rules! impl_scalar_from_row {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl FromRow for $ty {
                fn from_row(row: &PgRow) -> Result<Self> {
                    ensure_width(row, 1)?;
                    decode_column(row, 0)
                }
            }

            impl FromRow for Option<$ty> {
                fn from_row(row: &PgRow) -> Result<Self> {
                    ensure_width(row, 1)?;
                    decode_column(row, 0)
                }
            }
        )+
    };
}

impl_scalar_from_row!(
    i16,
    i32,
    i64,
    f32,
    f64,
    bool,
    String,
    uuid::Uuid,
    chrono::NaiveDate,
    chrono::NaiveDateTime,
    chrono::DateTime<chrono::Utc>,
    rust_decimal::Decimal,
    serde_json::Value,
);

impl<T> FromRow for Vec<T>
where
    Vec<T>: FromSqlOwned,
{
    fn from_row(row: &PgRow) -> Result<Self> {
        ensure_width(row, 1)?;
        decode_column(row, 0)
    }
}

impl<T> FromRow for Option<Vec<T>>
where
    Option<Vec<T>>: FromSqlOwned,
{
    fn from_row(row: &PgRow) -> Result<Self> {
        ensure_width(row, 1)?;
        decode_column(row, 0)
    }
}

macro_rules! impl_tuple_from_row {
    ($len:expr, $($ty:ident $idx:tt),+ $(,)?) => {
        impl<$($ty),+> FromRow for ($($ty,)+)
        where
            $($ty: FromSqlOwned),+
        {
            fn from_row(row: &PgRow) -> Result<Self> {
                ensure_width(row, $len)?;
                Ok((
                    $(
                        decode_column::<$ty>(row, $idx)?,
                    )+
                ))
            }
        }
    };
}

impl_tuple_from_row!(1, A 0);
impl_tuple_from_row!(2, A 0, B 1);
impl_tuple_from_row!(3, A 0, B 1, C 2);
impl_tuple_from_row!(4, A 0, B 1, C 2, D 3);
impl_tuple_from_row!(5, A 0, B 1, C 2, D 3, E 4);
impl_tuple_from_row!(6, A 0, B 1, C 2, D 3, E 4, F 5);
impl_tuple_from_row!(7, A 0, B 1, C 2, D 3, E 4, F 5, G 6);
impl_tuple_from_row!(8, A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);

/// Async execution methods for rendered `fuwa` query builders.
pub trait PgQueryExt: RenderQuery + Sized {
    fn execute<'a>(self, client: &'a (impl GenericClient + Sync)) -> PgFuture<'a, u64>
    where
        Self: Send + 'a,
    {
        Box::pin(async move {
            let rendered = self.render()?;
            let params = rendered.bind_refs();
            client
                .execute(rendered.sql(), params.as_slice())
                .await
                .map_err(|err| Error::execution(err.to_string()))
        })
    }

    fn fetch_all<'a, R>(self, client: &'a (impl GenericClient + Sync)) -> PgFuture<'a, Vec<R>>
    where
        Self: Send + 'a,
        R: FromRow + Send + 'a,
    {
        Box::pin(async move {
            let rendered = self.render()?;
            let params = rendered.bind_refs();
            let rows = client
                .query(rendered.sql(), params.as_slice())
                .await
                .map_err(|err| Error::execution(err.to_string()))?;

            rows.iter().map(R::from_row).collect()
        })
    }

    fn fetch_one<'a, R>(self, client: &'a (impl GenericClient + Sync)) -> PgFuture<'a, R>
    where
        Self: Send + 'a,
        R: FromRow + Send + 'a,
    {
        Box::pin(async move {
            let rendered = self.render()?;
            let params = rendered.bind_refs();
            let row = client
                .query_one(rendered.sql(), params.as_slice())
                .await
                .map_err(|err| Error::execution(err.to_string()))?;

            R::from_row(&row)
        })
    }

    fn fetch_optional<'a, R>(
        self,
        client: &'a (impl GenericClient + Sync),
    ) -> PgFuture<'a, Option<R>>
    where
        Self: Send + 'a,
        R: FromRow + Send + 'a,
    {
        Box::pin(async move {
            let rendered = self.render()?;
            let params = rendered.bind_refs();
            let row = client
                .query_opt(rendered.sql(), params.as_slice())
                .await
                .map_err(|err| Error::execution(err.to_string()))?;

            row.as_ref().map(R::from_row).transpose()
        })
    }
}

impl<Q> PgQueryExt for Q where Q: RenderQuery + Sized {}
