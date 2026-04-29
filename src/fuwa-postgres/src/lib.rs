//! Async `tokio-postgres` execution and row decoding for `fuwa`.

use std::future::Future;
use std::pin::Pin;

use futures_core::Stream;
use futures_util::{stream, StreamExt};
use fuwa_core::RenderQuery;
pub use tokio_postgres::types;
use tokio_postgres::types::FromSqlOwned;
pub use tokio_postgres::Row;
use tokio_postgres::{GenericClient, Portal, Row as PgRow, RowStream, Transaction};

pub use fuwa_core::{Error, Result};
/// Boxed future returned by `fuwa-postgres` extension methods.
pub type PgFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Boxed stream returned by `fuwa-postgres` streaming extension methods.
pub type PgStream<'a, T> = Pin<Box<dyn Stream<Item = Result<T>> + Send + 'a>>;

/// Boxed future returned by transaction callbacks.
pub type TransactionFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

const DEFAULT_PORTAL_FETCH_ROWS: i32 = 1024;

/// Run a closure inside a PostgreSQL transaction.
pub async fn transaction<C, F, T>(client: &mut C, f: F) -> Result<T>
where
    C: GenericClient,
    F: for<'a> FnOnce(&'a Transaction<'a>) -> TransactionFuture<'a, T>,
{
    let tx = client
        .transaction()
        .await
        .map_err(|err| Error::execution(err.to_string()))?;

    match f(&tx).await {
        Ok(value) => {
            tx.commit()
                .await
                .map_err(|err| Error::execution(err.to_string()))?;
            Ok(value)
        }
        Err(err) => {
            if let Err(rollback_err) = tx.rollback().await {
                return Err(Error::execution(format!(
                    "transaction rollback failed: {rollback_err}"
                )));
            }
            Err(err)
        }
    }
}

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

impl<T> FromRow for Option<T>
where
    Option<T>: FromSqlOwned,
{
    fn from_row(row: &PgRow) -> Result<Self> {
        ensure_width(row, 1)?;
        decode_column(row, 0)
    }
}

impl<T> FromRow for Vec<T>
where
    Vec<T>: FromSqlOwned,
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

struct PortalStreamState<'tx, 'client> {
    transaction: &'tx Transaction<'client>,
    portal: Portal,
    current: Option<Pin<Box<RowStream>>>,
    current_yielded: bool,
    done: bool,
}

fn portal_stream<'tx, 'client, R>(
    transaction: &'tx Transaction<'client>,
    portal: Portal,
) -> PgStream<'tx, R>
where
    'client: 'tx,
    R: FromRow + Send + 'tx,
{
    let state = PortalStreamState {
        transaction,
        portal,
        current: None,
        current_yielded: false,
        done: false,
    };

    Box::pin(stream::try_unfold(state, |mut state| async move {
        loop {
            if state.done {
                return Ok(None);
            }

            if state.current.is_none() {
                let row_stream = state
                    .transaction
                    .query_portal_raw(&state.portal, DEFAULT_PORTAL_FETCH_ROWS)
                    .await
                    .map_err(|err| Error::execution(err.to_string()))?;
                state.current = Some(Box::pin(row_stream));
                state.current_yielded = false;
            }

            let row_stream = state.current.as_mut().expect("portal stream is present");
            match row_stream.as_mut().next().await {
                Some(row) => {
                    state.current_yielded = true;
                    let row = row.map_err(|err| Error::execution(err.to_string()))?;
                    return R::from_row(&row).map(|row| Some((row, state)));
                }
                None => {
                    state.done = row_stream.as_ref().get_ref().rows_affected().is_some();
                    let current_yielded = state.current_yielded;
                    state.current = None;
                    state.current_yielded = false;

                    if !state.done && !current_yielded {
                        return Err(Error::execution(
                            "portal returned no rows without completing",
                        ));
                    }
                }
            }
        }
    }))
}

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

    /// Fetch rows through a PostgreSQL portal.
    ///
    /// The caller must pass an open transaction and keep it alive until the returned stream
    /// is exhausted or dropped. PostgreSQL portals live only for their transaction, so this
    /// method does not accept a plain client.
    fn fetch_stream<'tx, 'client, R>(
        self,
        transaction: &'tx Transaction<'client>,
    ) -> PgFuture<'tx, PgStream<'tx, R>>
    where
        'client: 'tx,
        Self: Send + 'tx,
        R: FromRow + Send + 'tx,
    {
        Box::pin(async move {
            let rendered = self.render()?;
            let params = rendered.bind_refs();
            let portal = transaction
                .bind(rendered.sql(), params.as_slice())
                .await
                .map_err(|err| Error::execution(err.to_string()))?;

            Ok(portal_stream(transaction, portal))
        })
    }

    /// Fetch rows through a PostgreSQL portal and emit decoded chunks of up to `n` rows.
    ///
    /// This is built on top of [`PgQueryExt::fetch_stream`]. Every chunk contains `n`
    /// rows except the final chunk, which may be shorter.
    fn fetch_chunked<'tx, 'client, R>(
        self,
        n: usize,
        transaction: &'tx Transaction<'client>,
    ) -> PgFuture<'tx, PgStream<'tx, Vec<R>>>
    where
        'client: 'tx,
        Self: Send + 'tx,
        R: FromRow + Send + 'tx,
    {
        Box::pin(async move {
            if n == 0 {
                return Err(Error::invalid_query_shape(
                    "fetch_chunked requires a chunk size greater than 0",
                ));
            }

            let rows = self.fetch_stream::<R>(transaction).await?;
            let chunks = stream::try_unfold(
                (rows, Vec::with_capacity(n)),
                move |(mut rows, mut chunk)| async move {
                    loop {
                        match rows.next().await {
                            Some(Ok(row)) => {
                                chunk.push(row);

                                if chunk.len() == n {
                                    return Ok(Some((chunk, (rows, Vec::with_capacity(n)))));
                                }
                            }
                            Some(Err(err)) => return Err(err),
                            None => {
                                if chunk.is_empty() {
                                    return Ok(None);
                                }

                                return Ok(Some((chunk, (rows, Vec::with_capacity(n)))));
                            }
                        }
                    }
                },
            );
            let chunks: PgStream<'tx, Vec<R>> = Box::pin(chunks);

            Ok(chunks)
        })
    }
}

impl<Q> PgQueryExt for Q where Q: RenderQuery + Sized {}
