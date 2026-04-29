//! Async `tokio-postgres` execution and row decoding for `fuwa`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures_core::Stream;
use futures_util::stream;
pub use futures_util::StreamExt;
use fuwa_core::{
    raw, AliasedSubquery, Assignments, ConflictTarget, Context, DeleteQuery, Excluded, ExprList,
    InOperand, InOperandNode, InsertConflictBuilder, InsertQuery, JoinTarget, OrderByList,
    RawQuery, RenderQuery, RenderedQuery, SelectQuery, Selectable, Table, TableSource, UpdateQuery,
    WithBuilder,
};
pub use tokio_postgres::types;
use tokio_postgres::types::FromSqlOwned;
pub use tokio_postgres::Row;
use tokio_postgres::{Client, GenericClient, NoTls, Portal, Row as PgRow, RowStream, Transaction};

pub use deadpool_postgres::Pool;
pub use fuwa_core::{Error, Result};
/// Boxed future returned by `fuwa-postgres` extension methods.
pub type PgFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Boxed stream returned by `fuwa-postgres` streaming extension methods.
pub type PgStream<'a, T> = Pin<Box<dyn Stream<Item = Result<T>> + Send + 'a>>;

/// Boxed future returned by transaction callbacks.
pub type TransactionFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

const DEFAULT_PORTAL_FETCH_ROWS: i32 = 1024;
const STREAMING_REQUIRES_TRANSACTION: &str =
    "fetch_stream and fetch_chunked require an existing transaction; wrap the call in dsl.transaction(|dsl| ...)";

/// Pool construction options for fuwa's default PostgreSQL pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolOptions {
    pub max_size: usize,
}

impl Default for PoolOptions {
    fn default() -> Self {
        Self { max_size: 16 }
    }
}

/// Build fuwa's default PostgreSQL pool from a database URL.
pub fn create_pool(database_url: impl AsRef<str>) -> Result<Pool> {
    create_pool_with_options(database_url, PoolOptions::default())
}

/// Build fuwa's default PostgreSQL pool from a database URL and options.
pub fn create_pool_with_options(
    database_url: impl AsRef<str>,
    options: PoolOptions,
) -> Result<Pool> {
    if options.max_size == 0 {
        return Err(Error::execution("pool max_size must be greater than zero"));
    }

    let config = database_url
        .as_ref()
        .parse::<tokio_postgres::Config>()
        .map_err(|err| Error::execution(err.to_string()))?;
    let manager = deadpool_postgres::Manager::from_config(
        config,
        NoTls,
        deadpool_postgres::ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        },
    );

    Pool::builder(manager)
        .max_size(options.max_size)
        .build()
        .map_err(|err| Error::execution(err.to_string()))
}

/// Entry point for creating an executor-bound DSL context.
#[derive(Debug, Clone, Copy, Default)]
pub struct Dsl;

impl Dsl {
    pub fn connect(database_url: impl AsRef<str>) -> Result<DslContext<Pool>> {
        Self::connect_with_options(database_url, PoolOptions::default())
    }

    pub fn connect_with_options(
        database_url: impl AsRef<str>,
        options: PoolOptions,
    ) -> Result<DslContext<Pool>> {
        create_pool_with_options(database_url, options).map(Self::using)
    }

    pub fn using<E>(executor: E) -> DslContext<E>
    where
        E: Executor,
    {
        DslContext::new(executor)
    }
}

/// A source that can provide a PostgreSQL client when a query executes.
pub trait Executor: Sync {
    type Conn<'a>: AcquiredConnection + Send + Sync
    where
        Self: 'a;

    fn acquire(&self) -> PgFuture<'_, Self::Conn<'_>>;
}

#[doc(hidden)]
pub trait AcquiredConnection {
    type Client: GenericClient + Sync + ?Sized;

    fn as_client(&self) -> &Self::Client;

    fn as_transaction(&self) -> Option<&Transaction<'_>> {
        None
    }
}

#[doc(hidden)]
pub trait TransactionConnection: AcquiredConnection<Client = Client> {
    fn as_client_mut(&mut self) -> &mut Client;
}

impl<'client> Executor for &'client Client {
    type Conn<'a>
        = &'a Client
    where
        Self: 'a;

    fn acquire(&self) -> PgFuture<'_, Self::Conn<'_>> {
        Box::pin(async move { Ok(*self) })
    }
}

impl<'client> AcquiredConnection for &'client Client {
    type Client = Client;

    fn as_client(&self) -> &Self::Client {
        self
    }
}

impl<'client> Executor for &'client mut Client {
    type Conn<'a>
        = &'a Client
    where
        Self: 'a;

    fn acquire(&self) -> PgFuture<'_, Self::Conn<'_>> {
        Box::pin(async move { Ok(&**self) })
    }
}

impl<'transaction, 'client> Executor for &'transaction Transaction<'client>
where
    'client: 'transaction,
{
    type Conn<'a>
        = &'a Transaction<'client>
    where
        Self: 'a;

    fn acquire(&self) -> PgFuture<'_, Self::Conn<'_>> {
        Box::pin(async move { Ok(*self) })
    }
}

impl<'transaction, 'client> Executor for &'transaction mut Transaction<'client>
where
    'client: 'transaction,
{
    type Conn<'a>
        = &'a Transaction<'client>
    where
        Self: 'a;

    fn acquire(&self) -> PgFuture<'_, Self::Conn<'_>> {
        Box::pin(async move { Ok(&**self) })
    }
}

impl<'transaction, 'client> AcquiredConnection for &'transaction Transaction<'client>
where
    'client: 'transaction,
{
    type Client = Transaction<'client>;

    fn as_client(&self) -> &Self::Client {
        self
    }

    fn as_transaction(&self) -> Option<&Transaction<'_>> {
        Some(*self)
    }
}

impl<E> Executor for Arc<E>
where
    E: Executor + Send,
{
    type Conn<'a>
        = E::Conn<'a>
    where
        Self: 'a;

    fn acquire(&self) -> PgFuture<'_, Self::Conn<'_>> {
        self.as_ref().acquire()
    }
}

impl Executor for Pool {
    type Conn<'a>
        = deadpool_postgres::Client
    where
        Self: 'a;

    fn acquire(&self) -> PgFuture<'_, Self::Conn<'_>> {
        Box::pin(async move {
            self.get()
                .await
                .map_err(|err| Error::execution(err.to_string()))
        })
    }
}

impl AcquiredConnection for deadpool_postgres::Client {
    type Client = Client;

    fn as_client(&self) -> &Self::Client {
        &***self
    }
}

impl TransactionConnection for deadpool_postgres::Client {
    fn as_client_mut(&mut self) -> &mut Client {
        &mut ***self
    }
}

impl<M> Executor for bb8::Pool<M>
where
    M: bb8::ManageConnection<Connection = Client> + Sync,
    M::Error: std::fmt::Debug + Send + Sync + 'static,
    for<'a> bb8::PooledConnection<'a, M>: Send + Sync,
{
    type Conn<'a>
        = bb8::PooledConnection<'a, M>
    where
        Self: 'a;

    fn acquire(&self) -> PgFuture<'_, Self::Conn<'_>> {
        Box::pin(async move {
            self.get()
                .await
                .map_err(|err| Error::execution(format!("{err:?}")))
        })
    }
}

impl<'pool, M> AcquiredConnection for bb8::PooledConnection<'pool, M>
where
    M: bb8::ManageConnection<Connection = Client>,
{
    type Client = Client;

    fn as_client(&self) -> &Self::Client {
        self
    }
}

impl<'pool, M> TransactionConnection for bb8::PooledConnection<'pool, M>
where
    M: bb8::ManageConnection<Connection = Client>,
{
    fn as_client_mut(&mut self) -> &mut Client {
        self
    }
}

/// A jOOQ-style, executor-bound DSL context.
///
/// The executor may be a single connection, a transaction, or a pool that
/// acquires on demand.
pub struct DslContext<E: Executor> {
    executor: E,
    context: Context,
}

impl<E> Clone for DslContext<E>
where
    E: Executor + Clone,
{
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
            context: self.context,
        }
    }
}

impl<E> DslContext<E>
where
    E: Executor,
{
    fn new(executor: E) -> Self {
        Self {
            executor,
            context: Context::new(),
        }
    }

    pub fn select<S>(&self, selection: S) -> AttachedSelectQuery<'_, E, S::Record, S::SingleSql>
    where
        S: Selectable,
    {
        AttachedSelectQuery {
            query: self.context.select(selection),
            executor: &self.executor,
        }
    }

    pub fn with<R, S, Q>(&self, name: &'static str, query: Q) -> AttachedWithBuilder<'_, E>
    where
        Q: IntoDetachedSelectQuery<R, S>,
    {
        AttachedWithBuilder {
            builder: self.context.with(name, query.into_select_query()),
            executor: &self.executor,
        }
    }

    pub fn insert_into(&self, table: Table) -> AttachedInsertQuery<'_, E> {
        AttachedInsertQuery {
            query: self.context.insert_into(table),
            executor: &self.executor,
        }
    }

    pub fn update(&self, table: Table) -> AttachedUpdateQuery<'_, E> {
        AttachedUpdateQuery {
            query: self.context.update(table),
            executor: &self.executor,
        }
    }

    pub fn delete_from(&self, table: Table) -> AttachedDeleteQuery<'_, E> {
        AttachedDeleteQuery {
            query: self.context.delete_from(table),
            executor: &self.executor,
        }
    }

    pub fn raw(&self, sql: impl Into<String>) -> AttachedRawQuery<'_, E> {
        AttachedRawQuery {
            query: raw(sql),
            executor: &self.executor,
        }
    }
}

async fn transaction_with_client<F, T>(client: &mut Client, f: F) -> Result<T>
where
    F: for<'tx> FnOnce(DslContext<&'tx Transaction<'tx>>) -> TransactionFuture<'tx, T>,
{
    let transaction = client
        .transaction()
        .await
        .map_err(|err| Error::execution(err.to_string()))?;

    match f(DslContext::new(&transaction)).await {
        Ok(value) => {
            transaction
                .commit()
                .await
                .map_err(|err| Error::execution(err.to_string()))?;
            Ok(value)
        }
        Err(err) => {
            if let Err(rollback_err) = transaction.rollback().await {
                return Err(Error::execution(format!(
                    "transaction rollback failed: {rollback_err}"
                )));
            }
            Err(err)
        }
    }
}

async fn transaction_with_transaction<F, T>(transaction: &mut Transaction<'_>, f: F) -> Result<T>
where
    F: for<'tx> FnOnce(DslContext<&'tx Transaction<'tx>>) -> TransactionFuture<'tx, T>,
{
    let transaction = transaction
        .transaction()
        .await
        .map_err(|err| Error::execution(err.to_string()))?;

    match f(DslContext::new(&transaction)).await {
        Ok(value) => {
            transaction
                .commit()
                .await
                .map_err(|err| Error::execution(err.to_string()))?;
            Ok(value)
        }
        Err(err) => {
            if let Err(rollback_err) = transaction.rollback().await {
                return Err(Error::execution(format!(
                    "transaction rollback failed: {rollback_err}"
                )));
            }
            Err(err)
        }
    }
}

impl<E> DslContext<E>
where
    E: Executor,
    for<'conn> E::Conn<'conn>: TransactionConnection,
{
    /// Run a closure inside a PostgreSQL transaction.
    pub async fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'tx> FnOnce(DslContext<&'tx Transaction<'tx>>) -> TransactionFuture<'tx, T>,
    {
        let mut conn = self.executor.acquire().await?;
        transaction_with_client(conn.as_client_mut(), f).await
    }
}

impl DslContext<&Client> {
    /// Reuse this connection for several queries in one callback.
    pub async fn with_connection<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'conn> FnOnce(DslContext<&'conn Client>) -> TransactionFuture<'conn, T>,
    {
        f(DslContext::new(self.executor)).await
    }
}

impl DslContext<&mut Client> {
    /// Run a closure inside a PostgreSQL transaction.
    pub async fn transaction<F, T>(&mut self, f: F) -> Result<T>
    where
        F: for<'tx> FnOnce(DslContext<&'tx Transaction<'tx>>) -> TransactionFuture<'tx, T>,
    {
        transaction_with_client(self.executor, f).await
    }

    /// Reuse this connection for several queries in one callback.
    pub async fn with_connection<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'conn> FnOnce(DslContext<&'conn Client>) -> TransactionFuture<'conn, T>,
    {
        f(DslContext::new(&*self.executor)).await
    }
}

impl<'transaction, 'client> DslContext<&'transaction mut Transaction<'client>>
where
    'client: 'transaction,
{
    /// Run a closure inside a nested PostgreSQL transaction.
    pub async fn transaction<F, T>(&mut self, f: F) -> Result<T>
    where
        F: for<'tx> FnOnce(DslContext<&'tx Transaction<'tx>>) -> TransactionFuture<'tx, T>,
    {
        transaction_with_transaction(self.executor, f).await
    }
}

impl<'transaction, 'client> DslContext<&'transaction Transaction<'client>>
where
    'client: 'transaction,
{
    /// Reuse this transaction for several queries in one callback.
    pub async fn with_connection<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'conn> FnOnce(
            DslContext<&'conn Transaction<'client>>,
        ) -> TransactionFuture<'conn, T>,
    {
        f(DslContext::new(self.executor)).await
    }
}

impl DslContext<Pool> {
    /// Acquire one physical connection and run multiple queries against it.
    pub async fn with_connection<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'conn> FnOnce(DslContext<&'conn Client>) -> TransactionFuture<'conn, T>,
    {
        let conn = self.executor.acquire().await?;
        f(DslContext::new(conn.as_client())).await
    }
}

impl<M> DslContext<bb8::Pool<M>>
where
    M: bb8::ManageConnection<Connection = Client> + Sync,
    M::Error: std::fmt::Debug + Send + Sync + 'static,
    for<'a> bb8::PooledConnection<'a, M>: Send + Sync,
{
    /// Acquire one physical connection and run multiple queries against it.
    pub async fn with_connection<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'conn> FnOnce(DslContext<&'conn Client>) -> TransactionFuture<'conn, T>,
    {
        let conn = self.executor.acquire().await?;
        f(DslContext::new(conn.as_client())).await
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
impl_tuple_from_row!(9, A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
impl_tuple_from_row!(10, A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
impl_tuple_from_row!(11, A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
impl_tuple_from_row!(12, A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);
impl_tuple_from_row!(13, A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11, M 12);
impl_tuple_from_row!(14, A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11, M 12, N 13);
impl_tuple_from_row!(15, A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11, M 12, N 13, O 14);
impl_tuple_from_row!(16, A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11, M 12, N 13, O 14, P 15);

struct PortalStreamState<C> {
    connection: C,
    portal: Portal,
    current: Option<Pin<Box<RowStream>>>,
    current_yielded: bool,
    done: bool,
}

fn portal_stream<'a, C, R>(connection: C, portal: Portal) -> PgStream<'a, R>
where
    C: AcquiredConnection + Send + 'a,
    R: FromRow + Send + 'a,
{
    let state = PortalStreamState {
        connection,
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
                let transaction = state
                    .connection
                    .as_transaction()
                    .ok_or_else(|| Error::execution(STREAMING_REQUIRES_TRANSACTION))?;
                let row_stream = transaction
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

fn execute_query<'a, E, Q>(executor: &'a E, query: Q) -> PgFuture<'a, u64>
where
    E: Executor,
    Q: RenderQuery + Send + 'a,
{
    Box::pin(async move {
        let rendered = query.render()?;
        let params = rendered.bind_refs();
        let conn = executor.acquire().await?;
        conn.as_client()
            .execute(rendered.sql(), params.as_slice())
            .await
            .map_err(|err| Error::execution(err.to_string()))
    })
}

fn fetch_all_query<'a, E, Q, R>(executor: &'a E, query: Q) -> PgFuture<'a, Vec<R>>
where
    E: Executor,
    Q: RenderQuery + Send + 'a,
    R: FromRow + Send + 'a,
{
    Box::pin(async move {
        let rendered = query.render()?;
        let params = rendered.bind_refs();
        let rows = {
            let conn = executor.acquire().await?;
            conn.as_client()
                .query(rendered.sql(), params.as_slice())
                .await
                .map_err(|err| Error::execution(err.to_string()))?
        };

        rows.iter().map(R::from_row).collect()
    })
}

fn fetch_one_query<'a, E, Q, R>(executor: &'a E, query: Q) -> PgFuture<'a, R>
where
    E: Executor,
    Q: RenderQuery + Send + 'a,
    R: FromRow + Send + 'a,
{
    Box::pin(async move {
        let rendered = query.render()?;
        let params = rendered.bind_refs();
        let row = {
            let conn = executor.acquire().await?;
            conn.as_client()
                .query_one(rendered.sql(), params.as_slice())
                .await
                .map_err(|err| Error::execution(err.to_string()))?
        };

        R::from_row(&row)
    })
}

fn fetch_optional_query<'a, E, Q, R>(executor: &'a E, query: Q) -> PgFuture<'a, Option<R>>
where
    E: Executor,
    Q: RenderQuery + Send + 'a,
    R: FromRow + Send + 'a,
{
    Box::pin(async move {
        let rendered = query.render()?;
        let params = rendered.bind_refs();
        let row = {
            let conn = executor.acquire().await?;
            conn.as_client()
                .query_opt(rendered.sql(), params.as_slice())
                .await
                .map_err(|err| Error::execution(err.to_string()))?
        };

        row.as_ref().map(R::from_row).transpose()
    })
}

fn fetch_stream_query<'a, E, Q, R>(executor: &'a E, query: Q) -> PgFuture<'a, PgStream<'a, R>>
where
    E: Executor,
    Q: RenderQuery + Send + 'a,
    R: FromRow + Send + 'a,
{
    Box::pin(async move {
        let rendered = query.render()?;
        let params = rendered.bind_refs();
        let conn = executor.acquire().await?;
        let transaction = conn
            .as_transaction()
            .ok_or_else(|| Error::execution(STREAMING_REQUIRES_TRANSACTION))?;
        let portal = transaction
            .bind(rendered.sql(), params.as_slice())
            .await
            .map_err(|err| Error::execution(err.to_string()))?;

        Ok(portal_stream(conn, portal))
    })
}

fn fetch_chunked_query<'a, E, Q, R>(
    executor: &'a E,
    query: Q,
    n: usize,
) -> PgFuture<'a, PgStream<'a, Vec<R>>>
where
    E: Executor,
    Q: RenderQuery + Send + 'a,
    R: FromRow + Send + 'a,
{
    Box::pin(async move {
        if n == 0 {
            return Err(Error::invalid_query_shape(
                "fetch_chunked requires a chunk size greater than 0",
            ));
        }

        let rows = fetch_stream_query::<_, _, R>(executor, query).await?;
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
        let chunks: PgStream<'a, Vec<R>> = Box::pin(chunks);

        Ok(chunks)
    })
}

#[doc(hidden)]
pub trait IntoDetachedSelectQuery<R, S> {
    fn into_select_query(self) -> SelectQuery<R, S>;
}

impl<R, S> IntoDetachedSelectQuery<R, S> for SelectQuery<R, S> {
    fn into_select_query(self) -> SelectQuery<R, S> {
        self
    }
}

pub struct AttachedSelectQuery<'a, E: Executor, R = (), S = fuwa_core::NotSingleColumn> {
    query: SelectQuery<R, S>,
    executor: &'a E,
}

impl<'a, E: Executor, R, S> IntoDetachedSelectQuery<R, S> for AttachedSelectQuery<'a, E, R, S> {
    fn into_select_query(self) -> SelectQuery<R, S> {
        self.query
    }
}

impl<'a, E: Executor, T, R> InOperand<T> for AttachedSelectQuery<'a, E, R, T> {
    fn into_in_operand(self) -> InOperandNode {
        self.query.into_in_operand()
    }
}

impl<'a, E, R, S> AttachedSelectQuery<'a, E, R, S>
where
    E: Executor,
{
    pub fn distinct(self) -> Self {
        Self {
            query: self.query.distinct(),
            executor: self.executor,
        }
    }

    pub fn distinct_on<Columns>(self, columns: Columns) -> Self
    where
        Columns: ExprList,
    {
        Self {
            query: self.query.distinct_on(columns),
            executor: self.executor,
        }
    }

    pub fn from<Source>(self, source: Source) -> Self
    where
        Source: TableSource,
    {
        Self {
            query: self.query.from(source),
            executor: self.executor,
        }
    }

    pub fn join(self, target: JoinTarget) -> Self {
        Self {
            query: self.query.join(target),
            executor: self.executor,
        }
    }

    pub fn left_join(self, target: JoinTarget) -> Self {
        Self {
            query: self.query.left_join(target),
            executor: self.executor,
        }
    }

    pub fn where_(self, condition: fuwa_core::Condition) -> Self {
        Self {
            query: self.query.where_(condition),
            executor: self.executor,
        }
    }

    pub fn group_by<GroupBy>(self, group_by: GroupBy) -> Self
    where
        GroupBy: ExprList,
    {
        Self {
            query: self.query.group_by(group_by),
            executor: self.executor,
        }
    }

    pub fn having(self, condition: fuwa_core::Condition) -> Self {
        Self {
            query: self.query.having(condition),
            executor: self.executor,
        }
    }

    pub fn order_by<O>(self, order_by: O) -> Self
    where
        O: OrderByList,
    {
        Self {
            query: self.query.order_by(order_by),
            executor: self.executor,
        }
    }

    pub fn limit(self, limit: i64) -> Self {
        Self {
            query: self.query.limit(limit),
            executor: self.executor,
        }
    }

    pub fn offset(self, offset: i64) -> Self {
        Self {
            query: self.query.offset(offset),
            executor: self.executor,
        }
    }

    pub fn alias(self, alias: &'static str) -> AliasedSubquery {
        self.query.alias(alias)
    }

    pub fn render(self) -> Result<RenderedQuery> {
        self.query.render()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        SelectQuery<R, S>: Send + 'a,
    {
        execute_query(self.executor, self.query)
    }

    pub fn fetch_all<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        SelectQuery<R, S>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_one<Row>(self) -> PgFuture<'a, Row>
    where
        SelectQuery<R, S>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_optional<Row>(self) -> PgFuture<'a, Option<Row>>
    where
        SelectQuery<R, S>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_optional_query(self.executor, self.query)
    }

    pub fn fetch_stream<Row>(self) -> PgFuture<'a, PgStream<'a, Row>>
    where
        SelectQuery<R, S>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_stream_query(self.executor, self.query)
    }

    pub fn fetch_chunked<Row>(self, n: usize) -> PgFuture<'a, PgStream<'a, Vec<Row>>>
    where
        SelectQuery<R, S>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_chunked_query(self.executor, self.query, n)
    }
}

pub struct AttachedWithBuilder<'a, E: Executor> {
    builder: WithBuilder,
    executor: &'a E,
}

impl<'a, E> AttachedWithBuilder<'a, E>
where
    E: Executor,
{
    pub fn with<R, S, Q>(self, name: &'static str, query: Q) -> Self
    where
        Q: IntoDetachedSelectQuery<R, S>,
    {
        Self {
            builder: self.builder.with(name, query.into_select_query()),
            executor: self.executor,
        }
    }

    pub fn select<S>(self, selection: S) -> AttachedSelectQuery<'a, E, S::Record, S::SingleSql>
    where
        S: Selectable,
    {
        AttachedSelectQuery {
            query: self.builder.select(selection),
            executor: self.executor,
        }
    }
}

pub struct AttachedInsertQuery<'a, E: Executor, R = ()> {
    query: InsertQuery<R>,
    executor: &'a E,
}

impl<'a, E, R> AttachedInsertQuery<'a, E, R>
where
    E: Executor,
{
    pub fn values<A>(self, assignments: A) -> Self
    where
        A: Assignments,
    {
        Self {
            query: self.query.values(assignments),
            executor: self.executor,
        }
    }

    pub fn values_many<I, A>(self, rows: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: Assignments,
    {
        Self {
            query: self.query.values_many(rows),
            executor: self.executor,
        }
    }

    pub fn on_conflict<T>(self, target: T) -> AttachedInsertConflictBuilder<'a, E, R>
    where
        T: ConflictTarget,
    {
        AttachedInsertConflictBuilder {
            builder: self.query.on_conflict(target),
            executor: self.executor,
        }
    }

    pub fn returning<S>(self, selection: S) -> AttachedInsertQuery<'a, E, S::Record>
    where
        S: Selectable,
    {
        AttachedInsertQuery {
            query: self.query.returning(selection),
            executor: self.executor,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        self.query.render()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        InsertQuery<R>: Send + 'a,
    {
        execute_query(self.executor, self.query)
    }

    pub fn fetch_all<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        InsertQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_one<Row>(self) -> PgFuture<'a, Row>
    where
        InsertQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_optional<Row>(self) -> PgFuture<'a, Option<Row>>
    where
        InsertQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_optional_query(self.executor, self.query)
    }
}

pub struct AttachedInsertConflictBuilder<'a, E: Executor, R = ()> {
    builder: InsertConflictBuilder<R>,
    executor: &'a E,
}

impl<'a, E, R> AttachedInsertConflictBuilder<'a, E, R>
where
    E: Executor,
{
    pub fn do_nothing(self) -> AttachedInsertQuery<'a, E, R> {
        AttachedInsertQuery {
            query: self.builder.do_nothing(),
            executor: self.executor,
        }
    }

    pub fn do_update<F, A>(self, f: F) -> AttachedInsertQuery<'a, E, R>
    where
        F: FnOnce(Excluded) -> A,
        A: Assignments,
    {
        AttachedInsertQuery {
            query: self.builder.do_update(f),
            executor: self.executor,
        }
    }
}

pub struct AttachedUpdateQuery<'a, E: Executor, R = ()> {
    query: UpdateQuery<R>,
    executor: &'a E,
}

impl<'a, E, R> AttachedUpdateQuery<'a, E, R>
where
    E: Executor,
{
    pub fn set<A>(self, assignments: A) -> Self
    where
        A: Assignments,
    {
        Self {
            query: self.query.set(assignments),
            executor: self.executor,
        }
    }

    pub fn where_(self, condition: fuwa_core::Condition) -> Self {
        Self {
            query: self.query.where_(condition),
            executor: self.executor,
        }
    }

    pub fn returning<S>(self, selection: S) -> AttachedUpdateQuery<'a, E, S::Record>
    where
        S: Selectable,
    {
        AttachedUpdateQuery {
            query: self.query.returning(selection),
            executor: self.executor,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        self.query.render()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        UpdateQuery<R>: Send + 'a,
    {
        execute_query(self.executor, self.query)
    }

    pub fn fetch_all<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        UpdateQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_one<Row>(self) -> PgFuture<'a, Row>
    where
        UpdateQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_optional<Row>(self) -> PgFuture<'a, Option<Row>>
    where
        UpdateQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_optional_query(self.executor, self.query)
    }
}

pub struct AttachedDeleteQuery<'a, E: Executor, R = ()> {
    query: DeleteQuery<R>,
    executor: &'a E,
}

impl<'a, E, R> AttachedDeleteQuery<'a, E, R>
where
    E: Executor,
{
    pub fn where_(self, condition: fuwa_core::Condition) -> Self {
        Self {
            query: self.query.where_(condition),
            executor: self.executor,
        }
    }

    pub fn returning<S>(self, selection: S) -> AttachedDeleteQuery<'a, E, S::Record>
    where
        S: Selectable,
    {
        AttachedDeleteQuery {
            query: self.query.returning(selection),
            executor: self.executor,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        self.query.render()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        DeleteQuery<R>: Send + 'a,
    {
        execute_query(self.executor, self.query)
    }

    pub fn fetch_all<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        DeleteQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_one<Row>(self) -> PgFuture<'a, Row>
    where
        DeleteQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_optional<Row>(self) -> PgFuture<'a, Option<Row>>
    where
        DeleteQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_optional_query(self.executor, self.query)
    }
}

pub struct AttachedRawQuery<'a, E: Executor, R = ()> {
    query: RawQuery<R>,
    executor: &'a E,
}

impl<'a, E, R> AttachedRawQuery<'a, E, R>
where
    E: Executor,
{
    pub fn bind<T>(self, value: T) -> Self
    where
        T: fuwa_core::IntoBindValue,
    {
        Self {
            query: self.query.bind(value),
            executor: self.executor,
        }
    }

    pub fn record<T>(self) -> AttachedRawQuery<'a, E, T> {
        AttachedRawQuery {
            query: self.query.record(),
            executor: self.executor,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        self.query.render()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        RawQuery<R>: Send + 'a,
    {
        execute_query(self.executor, self.query)
    }

    pub fn fetch_all<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        RawQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_one<Row>(self) -> PgFuture<'a, Row>
    where
        RawQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_optional<Row>(self) -> PgFuture<'a, Option<Row>>
    where
        RawQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_optional_query(self.executor, self.query)
    }

    pub fn fetch_stream<Row>(self) -> PgFuture<'a, PgStream<'a, Row>>
    where
        RawQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_stream_query(self.executor, self.query)
    }

    pub fn fetch_chunked<Row>(self, n: usize) -> PgFuture<'a, PgStream<'a, Vec<Row>>>
    where
        RawQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_chunked_query(self.executor, self.query, n)
    }
}
