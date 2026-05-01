//! Async `tokio-postgres` execution and row decoding for `fuwa`.

mod copy;
mod pg_error;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures_core::Stream;
use futures_util::stream;
pub use futures_util::StreamExt;
use fuwa_core::{
    raw, AliasedSubquery, Assignments, ConflictTarget, Context, DeleteQuery, Excluded, ExprList,
    InOperand, InOperandNode, InsertConflictBuilder, InsertQuery, IntoExistsQuery, JoinTarget,
    NotSingleColumn, OrderByList, RawQuery, RenderQuery, RenderedQuery, SelectQuery, Selectable,
    Table, TableSource, UpdateQuery, WithBuilder,
};

use copy::copy_in_binary_with_executor;
pub use copy::{CopyInColumn, CopyInColumnMeta, CopyInColumns, CopyInRow, CopyInWriter, PgType};
use pg_error::map_pg_error;
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
    "fetch_stream and fetch_chunked require an existing transaction; wrap the call in dsl.transaction(async |dsl| { ... })";

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

/// Marker for executors that yield a transaction-bound connection.
///
/// Implemented for `&Transaction<'_>` and friends. Methods that require a
/// PostgreSQL portal (e.g. `fetch_stream` / `fetch_chunked`) are bounded on
/// this trait so calling them on a pool or bare client is a compile error.
pub trait TransactionalExecutor: Executor {}

#[doc(hidden)]
pub trait AcquiredConnection {
    type Client: GenericClient + Sync + ?Sized;

    fn as_client(&self) -> &Self::Client;

    fn as_transaction(&self) -> Option<&Transaction<'_>> {
        None
    }

    /// Open a binary `COPY ... FROM STDIN` sink against the underlying connection.
    fn copy_in_binary_sink<'a>(
        &'a self,
        _sql: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<tokio_postgres::CopyInSink<bytes::Bytes>>> + Send + 'a>>
    {
        Box::pin(async {
            Err(Error::execution(
                "copy_in is not supported by this connection type",
            ))
        })
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

    fn copy_in_binary_sink<'a>(
        &'a self,
        sql: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<tokio_postgres::CopyInSink<bytes::Bytes>>> + Send + 'a>>
    {
        Box::pin(async move {
            Client::copy_in::<_, bytes::Bytes>(self, sql)
                .await
                .map_err(map_pg_error)
        })
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

impl<'transaction, 'client> TransactionalExecutor for &'transaction Transaction<'client> where
    'client: 'transaction
{
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

impl<'transaction, 'client> TransactionalExecutor for &'transaction mut Transaction<'client> where
    'client: 'transaction
{
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

    fn copy_in_binary_sink<'a>(
        &'a self,
        sql: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<tokio_postgres::CopyInSink<bytes::Bytes>>> + Send + 'a>>
    {
        Box::pin(async move {
            Transaction::copy_in::<_, bytes::Bytes>(self, sql)
                .await
                .map_err(map_pg_error)
        })
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

    fn copy_in_binary_sink<'a>(
        &'a self,
        sql: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<tokio_postgres::CopyInSink<bytes::Bytes>>> + Send + 'a>>
    {
        Box::pin(async move {
            Client::copy_in::<_, bytes::Bytes>(&***self, sql)
                .await
                .map_err(map_pg_error)
        })
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

    fn copy_in_binary_sink<'a>(
        &'a self,
        sql: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<tokio_postgres::CopyInSink<bytes::Bytes>>> + Send + 'a>>
    {
        Box::pin(async move {
            Client::copy_in::<_, bytes::Bytes>(self, sql)
                .await
                .map_err(map_pg_error)
        })
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

    /// Run any built query (free-function form) and decode rows.
    pub fn fetch_all<'a, Q>(&'a self, query: Q) -> PgFuture<'a, Vec<Q::Row>>
    where
        Q: fuwa_core::Query + Send + 'a,
        Q::Row: FromRow + Send + 'a,
    {
        fetch_all_query(&self.executor, query)
    }

    /// Run any built query and decode rows into an explicit override type.
    pub fn fetch_all_as<'a, Q, Row>(&'a self, query: Q) -> PgFuture<'a, Vec<Row>>
    where
        Q: RenderQuery + Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(&self.executor, query)
    }

    /// Run any built query and decode exactly one row.
    pub fn fetch_one<'a, Q>(&'a self, query: Q) -> PgFuture<'a, Q::Row>
    where
        Q: fuwa_core::Query + Send + 'a,
        Q::Row: FromRow + Send + 'a,
    {
        fetch_one_query(&self.executor, query)
    }

    pub fn fetch_one_as<'a, Q, Row>(&'a self, query: Q) -> PgFuture<'a, Row>
    where
        Q: RenderQuery + Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(&self.executor, query)
    }

    /// Run any built query and decode an optional row.
    pub fn fetch_optional<'a, Q>(&'a self, query: Q) -> PgFuture<'a, Option<Q::Row>>
    where
        Q: fuwa_core::Query + Send + 'a,
        Q::Row: FromRow + Send + 'a,
    {
        fetch_optional_query(&self.executor, query)
    }

    pub fn fetch_optional_as<'a, Q, Row>(&'a self, query: Q) -> PgFuture<'a, Option<Row>>
    where
        Q: RenderQuery + Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_optional_query(&self.executor, query)
    }

    /// Run any built query and return rows-affected.
    pub fn execute<'a, Q>(&'a self, query: Q) -> PgFuture<'a, u64>
    where
        Q: RenderQuery + Send + 'a,
    {
        execute_query(&self.executor, query)
    }

    /// Run a typed binary `COPY ... FROM STDIN` against this executor.
    ///
    /// The closure receives a [`CopyInWriter`] for sending rows; the writer is
    /// committed when the closure returns and the row count is returned.
    pub async fn copy_in_binary<C, F>(&self, table: Table, columns: C, f: F) -> Result<u64>
    where
        C: CopyInColumns,
        F: for<'w> AsyncFnOnce(&'w mut CopyInWriter<C::Row>) -> Result<()>,
    {
        copy_in_binary_with_executor(&self.executor, table, columns, f).await
    }

    /// Send a `NOTIFY channel, 'payload'` via `pg_notify(...)`.
    pub async fn notify(&self, channel: &str, payload: Option<&str>) -> Result<()> {
        let attached = self
            .raw("select pg_notify($1, $2)")
            .bind(channel.to_owned());
        let attached = attached.bind(payload.unwrap_or("").to_owned());
        attached.execute().await?;
        Ok(())
    }
}

async fn transaction_with_client<F, T>(client: &mut Client, f: F) -> Result<T>
where
    F: for<'tx> AsyncFnOnce(DslContext<&'tx Transaction<'tx>>) -> Result<T>,
{
    let transaction = client.transaction().await.map_err(map_pg_error)?;

    match f(DslContext::new(&transaction)).await {
        Ok(value) => {
            transaction.commit().await.map_err(map_pg_error)?;
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
    F: for<'tx> AsyncFnOnce(DslContext<&'tx Transaction<'tx>>) -> Result<T>,
{
    let transaction = transaction.transaction().await.map_err(map_pg_error)?;

    match f(DslContext::new(&transaction)).await {
        Ok(value) => {
            transaction.commit().await.map_err(map_pg_error)?;
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
        F: for<'tx> AsyncFnOnce(DslContext<&'tx Transaction<'tx>>) -> Result<T>,
    {
        let mut conn = self.executor.acquire().await?;
        transaction_with_client(conn.as_client_mut(), f).await
    }
}

impl DslContext<&Client> {
    /// Reuse this connection for several queries in one callback.
    pub async fn with_connection<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'conn> AsyncFnOnce(DslContext<&'conn Client>) -> Result<T>,
    {
        f(DslContext::new(self.executor)).await
    }
}

impl DslContext<&mut Client> {
    /// Run a closure inside a PostgreSQL transaction.
    pub async fn transaction<F, T>(&mut self, f: F) -> Result<T>
    where
        F: for<'tx> AsyncFnOnce(DslContext<&'tx Transaction<'tx>>) -> Result<T>,
    {
        transaction_with_client(self.executor, f).await
    }

    /// Reuse this connection for several queries in one callback.
    pub async fn with_connection<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'conn> AsyncFnOnce(DslContext<&'conn Client>) -> Result<T>,
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
        F: for<'tx> AsyncFnOnce(DslContext<&'tx Transaction<'tx>>) -> Result<T>,
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
        F: for<'conn> AsyncFnOnce(DslContext<&'conn Transaction<'client>>) -> Result<T>,
    {
        f(DslContext::new(self.executor)).await
    }
}

impl DslContext<Pool> {
    /// Acquire one physical connection and run multiple queries against it.
    pub async fn with_connection<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'conn> AsyncFnOnce(DslContext<&'conn Client>) -> Result<T>,
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
        F: for<'conn> AsyncFnOnce(DslContext<&'conn Client>) -> Result<T>,
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
    u32,
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
                    .map_err(map_pg_error)?;
                state.current = Some(Box::pin(row_stream));
                state.current_yielded = false;
            }

            let row_stream = state.current.as_mut().expect("portal stream is present");
            match row_stream.as_mut().next().await {
                Some(row) => {
                    state.current_yielded = true;
                    let row = row.map_err(map_pg_error)?;
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
        #[cfg(feature = "tracing")]
        let span = tracing::debug_span!(
            "fuwa.execute",
            sql = %rendered.sql(),
            bind_count = params.len()
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();
        let conn = executor.acquire().await?;
        let result = conn
            .as_client()
            .execute(rendered.sql(), params.as_slice())
            .await
            .map_err(map_pg_error);
        #[cfg(feature = "tracing")]
        if let Ok(rows) = &result {
            tracing::debug!(rows_affected = rows, "execute succeeded");
        }
        result
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
        #[cfg(feature = "tracing")]
        let span = tracing::debug_span!(
            "fuwa.fetch_all",
            sql = %rendered.sql(),
            bind_count = params.len()
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();
        let rows = {
            let conn = executor.acquire().await?;
            conn.as_client()
                .query(rendered.sql(), params.as_slice())
                .await
                .map_err(map_pg_error)?
        };
        #[cfg(feature = "tracing")]
        tracing::debug!(rows = rows.len(), "fetch_all succeeded");

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
        #[cfg(feature = "tracing")]
        let span = tracing::debug_span!(
            "fuwa.fetch_one",
            sql = %rendered.sql(),
            bind_count = params.len()
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();
        let row = {
            let conn = executor.acquire().await?;
            conn.as_client()
                .query_one(rendered.sql(), params.as_slice())
                .await
                .map_err(map_pg_error)?
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
        #[cfg(feature = "tracing")]
        let span = tracing::debug_span!(
            "fuwa.fetch_optional",
            sql = %rendered.sql(),
            bind_count = params.len()
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();
        let row = {
            let conn = executor.acquire().await?;
            conn.as_client()
                .query_opt(rendered.sql(), params.as_slice())
                .await
                .map_err(map_pg_error)?
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
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: "fuwa",
            sql = %rendered.sql(),
            bind_count = params.len(),
            "fetch_stream"
        );
        let conn = executor.acquire().await?;
        let transaction = conn
            .as_transaction()
            .ok_or_else(|| Error::execution(STREAMING_REQUIRES_TRANSACTION))?;
        let portal = transaction
            .bind(rendered.sql(), params.as_slice())
            .await
            .map_err(map_pg_error)?;

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

impl<'a, E: Executor, R, S> IntoExistsQuery<R, S> for AttachedSelectQuery<'a, E, R, S> {
    fn into_select_query(self) -> SelectQuery<(), NotSingleColumn> {
        <SelectQuery<R, S> as IntoExistsQuery<R, S>>::into_select_query(self.query)
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

    pub fn right_join(self, target: JoinTarget) -> Self {
        Self {
            query: self.query.right_join(target),
            executor: self.executor,
        }
    }

    pub fn full_join(self, target: JoinTarget) -> Self {
        Self {
            query: self.query.full_join(target),
            executor: self.executor,
        }
    }

    pub fn cross_join<T>(self, source: T) -> Self
    where
        T: TableSource,
    {
        Self {
            query: self.query.cross_join(source),
            executor: self.executor,
        }
    }

    pub fn join_lateral(self, target: JoinTarget) -> Self {
        Self {
            query: self.query.join_lateral(target),
            executor: self.executor,
        }
    }

    pub fn left_join_lateral(self, target: JoinTarget) -> Self {
        Self {
            query: self.query.left_join_lateral(target),
            executor: self.executor,
        }
    }

    pub fn cross_join_lateral<T>(self, source: T) -> Self
    where
        T: TableSource,
    {
        Self {
            query: self.query.cross_join_lateral(source),
            executor: self.executor,
        }
    }

    pub fn for_update(self) -> Self {
        Self {
            query: self.query.for_update(),
            executor: self.executor,
        }
    }

    pub fn for_no_key_update(self) -> Self {
        Self {
            query: self.query.for_no_key_update(),
            executor: self.executor,
        }
    }

    pub fn for_share(self) -> Self {
        Self {
            query: self.query.for_share(),
            executor: self.executor,
        }
    }

    pub fn for_key_share(self) -> Self {
        Self {
            query: self.query.for_key_share(),
            executor: self.executor,
        }
    }

    pub fn of<L>(self, tables: L) -> Self
    where
        L: fuwa_core::LockTargetList,
    {
        Self {
            query: self.query.of(tables),
            executor: self.executor,
        }
    }

    pub fn skip_locked(self) -> Self {
        Self {
            query: self.query.skip_locked(),
            executor: self.executor,
        }
    }

    pub fn no_wait(self) -> Self {
        Self {
            query: self.query.no_wait(),
            executor: self.executor,
        }
    }

    pub fn union<Q>(self, other: Q) -> Self
    where
        Q: IntoDetachedSelectQuery<R, S>,
    {
        Self {
            query: self.query.union(other.into_select_query()),
            executor: self.executor,
        }
    }

    pub fn union_all<Q>(self, other: Q) -> Self
    where
        Q: IntoDetachedSelectQuery<R, S>,
    {
        Self {
            query: self.query.union_all(other.into_select_query()),
            executor: self.executor,
        }
    }

    pub fn except<Q>(self, other: Q) -> Self
    where
        Q: IntoDetachedSelectQuery<R, S>,
    {
        Self {
            query: self.query.except(other.into_select_query()),
            executor: self.executor,
        }
    }

    pub fn except_all<Q>(self, other: Q) -> Self
    where
        Q: IntoDetachedSelectQuery<R, S>,
    {
        Self {
            query: self.query.except_all(other.into_select_query()),
            executor: self.executor,
        }
    }

    pub fn intersect<Q>(self, other: Q) -> Self
    where
        Q: IntoDetachedSelectQuery<R, S>,
    {
        Self {
            query: self.query.intersect(other.into_select_query()),
            executor: self.executor,
        }
    }

    pub fn intersect_all<Q>(self, other: Q) -> Self
    where
        Q: IntoDetachedSelectQuery<R, S>,
    {
        Self {
            query: self.query.intersect_all(other.into_select_query()),
            executor: self.executor,
        }
    }

    pub fn where_<C: fuwa_core::IntoCondition>(self, condition: C) -> Self {
        Self {
            query: self.query.where_(condition),
            executor: self.executor,
        }
    }

    /// AND another condition into WHERE (verb form for dynamic loops).
    pub fn and_where<C: fuwa_core::IntoCondition>(self, condition: C) -> Self {
        self.where_(condition)
    }

    /// AND another condition into HAVING (verb form for dynamic loops).
    pub fn and_having<C: fuwa_core::IntoCondition>(self, condition: C) -> Self {
        self.having(condition)
    }

    /// Append a raw `SelectItem` to the projection without re-typing the query.
    pub fn push_select_item(self, item: fuwa_core::SelectItem) -> Self {
        Self {
            query: self.query.push_select_item(item),
            executor: self.executor,
        }
    }

    /// Append a raw `OrderExpr` to ORDER BY without re-typing the query.
    pub fn push_order_by(self, order: fuwa_core::OrderExpr) -> Self {
        Self {
            query: self.query.push_order_by(order),
            executor: self.executor,
        }
    }

    /// Append a raw `Join` to the FROM clause without re-typing the query.
    pub fn push_join(self, join: fuwa_core::Join) -> Self {
        Self {
            query: self.query.push_join(join),
            executor: self.executor,
        }
    }

    /// Append an `ExprNode` to GROUP BY without re-typing the query.
    pub fn push_group_by(self, expr: fuwa_core::ExprNode) -> Self {
        Self {
            query: self.query.push_group_by(expr),
            executor: self.executor,
        }
    }

    /// Alias for [`where_`](Self::where_) for diesel-style ergonomics.
    pub fn filter<C: fuwa_core::IntoCondition>(self, condition: C) -> Self {
        self.where_(condition)
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

    pub fn having<C: fuwa_core::IntoCondition>(self, condition: C) -> Self {
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

    /// Render this query without consuming it.
    pub fn render_ref(&self) -> Result<RenderedQuery> {
        self.query.render_ref()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        SelectQuery<R, S>: Send + 'a,
    {
        execute_query(self.executor, self.query)
    }

    /// Run the query and decode each row into the projection's record type.
    pub fn fetch_all(self) -> PgFuture<'a, Vec<R>>
    where
        SelectQuery<R, S>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    /// Run the query and decode each row into an explicit override type.
    pub fn fetch_all_as<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        SelectQuery<R, S>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_one(self) -> PgFuture<'a, R>
    where
        SelectQuery<R, S>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_one_as<Row>(self) -> PgFuture<'a, Row>
    where
        SelectQuery<R, S>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_optional(self) -> PgFuture<'a, Option<R>>
    where
        SelectQuery<R, S>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_optional_query(self.executor, self.query)
    }

    pub fn fetch_optional_as<Row>(self) -> PgFuture<'a, Option<Row>>
    where
        SelectQuery<R, S>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_optional_query(self.executor, self.query)
    }

    pub fn fetch_stream(self) -> PgFuture<'a, PgStream<'a, R>>
    where
        E: TransactionalExecutor,
        SelectQuery<R, S>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_stream_query(self.executor, self.query)
    }

    pub fn fetch_stream_as<Row>(self) -> PgFuture<'a, PgStream<'a, Row>>
    where
        E: TransactionalExecutor,
        SelectQuery<R, S>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_stream_query(self.executor, self.query)
    }

    pub fn fetch_chunked(self, n: usize) -> PgFuture<'a, PgStream<'a, Vec<R>>>
    where
        E: TransactionalExecutor,
        SelectQuery<R, S>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_chunked_query(self.executor, self.query, n)
    }

    pub fn fetch_chunked_as<Row>(self, n: usize) -> PgFuture<'a, PgStream<'a, Vec<Row>>>
    where
        E: TransactionalExecutor,
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

    /// Insert a single record using its `Assignments` impl.
    pub fn value<A>(self, record: A) -> Self
    where
        A: Assignments,
    {
        self.values(record)
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

    pub fn columns<C>(self, columns: C) -> Self
    where
        C: ConflictTarget,
    {
        Self {
            query: self.query.columns(columns),
            executor: self.executor,
        }
    }

    pub fn from_select<R2, S, Q>(self, query: Q) -> Self
    where
        Q: IntoDetachedSelectQuery<R2, S>,
    {
        Self {
            query: self.query.from_select(query.into_select_query()),
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

    /// Render this query without consuming it.
    pub fn render_ref(&self) -> Result<RenderedQuery> {
        self.query.render_ref()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        InsertQuery<R>: Send + 'a,
    {
        execute_query(self.executor, self.query)
    }

    pub fn fetch_all(self) -> PgFuture<'a, Vec<R>>
    where
        InsertQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_all_as<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        InsertQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_one(self) -> PgFuture<'a, R>
    where
        InsertQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_one_as<Row>(self) -> PgFuture<'a, Row>
    where
        InsertQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_optional(self) -> PgFuture<'a, Option<R>>
    where
        InsertQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_optional_query(self.executor, self.query)
    }

    pub fn fetch_optional_as<Row>(self) -> PgFuture<'a, Option<Row>>
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

    pub fn from<S>(self, source: S) -> Self
    where
        S: TableSource,
    {
        Self {
            query: self.query.from(source),
            executor: self.executor,
        }
    }

    pub fn where_<C: fuwa_core::IntoCondition>(self, condition: C) -> Self {
        Self {
            query: self.query.where_(condition),
            executor: self.executor,
        }
    }

    /// Alias for [`where_`](Self::where_) for diesel-style ergonomics.
    pub fn filter<C: fuwa_core::IntoCondition>(self, condition: C) -> Self {
        self.where_(condition)
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

    /// Render this query without consuming it.
    pub fn render_ref(&self) -> Result<RenderedQuery> {
        self.query.render_ref()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        UpdateQuery<R>: Send + 'a,
    {
        execute_query(self.executor, self.query)
    }

    pub fn fetch_all(self) -> PgFuture<'a, Vec<R>>
    where
        UpdateQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_all_as<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        UpdateQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_one(self) -> PgFuture<'a, R>
    where
        UpdateQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_one_as<Row>(self) -> PgFuture<'a, Row>
    where
        UpdateQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_optional(self) -> PgFuture<'a, Option<R>>
    where
        UpdateQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_optional_query(self.executor, self.query)
    }

    pub fn fetch_optional_as<Row>(self) -> PgFuture<'a, Option<Row>>
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
    pub fn using<S>(self, source: S) -> Self
    where
        S: TableSource,
    {
        Self {
            query: self.query.using(source),
            executor: self.executor,
        }
    }

    pub fn where_<C: fuwa_core::IntoCondition>(self, condition: C) -> Self {
        Self {
            query: self.query.where_(condition),
            executor: self.executor,
        }
    }

    /// Alias for [`where_`](Self::where_) for diesel-style ergonomics.
    pub fn filter<C: fuwa_core::IntoCondition>(self, condition: C) -> Self {
        self.where_(condition)
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

    /// Render this query without consuming it.
    pub fn render_ref(&self) -> Result<RenderedQuery> {
        self.query.render_ref()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        DeleteQuery<R>: Send + 'a,
    {
        execute_query(self.executor, self.query)
    }

    pub fn fetch_all(self) -> PgFuture<'a, Vec<R>>
    where
        DeleteQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_all_as<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        DeleteQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_one(self) -> PgFuture<'a, R>
    where
        DeleteQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_one_as<Row>(self) -> PgFuture<'a, Row>
    where
        DeleteQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_optional(self) -> PgFuture<'a, Option<R>>
    where
        DeleteQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_optional_query(self.executor, self.query)
    }

    pub fn fetch_optional_as<Row>(self) -> PgFuture<'a, Option<Row>>
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

    /// Render this query without consuming it.
    pub fn render_ref(&self) -> Result<RenderedQuery> {
        self.query.render_ref()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        RawQuery<R>: Send + 'a,
    {
        execute_query(self.executor, self.query)
    }

    pub fn fetch_all(self) -> PgFuture<'a, Vec<R>>
    where
        RawQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_all_as<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        RawQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.executor, self.query)
    }

    pub fn fetch_one(self) -> PgFuture<'a, R>
    where
        RawQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_one_as<Row>(self) -> PgFuture<'a, Row>
    where
        RawQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.executor, self.query)
    }

    pub fn fetch_optional(self) -> PgFuture<'a, Option<R>>
    where
        RawQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_optional_query(self.executor, self.query)
    }

    pub fn fetch_optional_as<Row>(self) -> PgFuture<'a, Option<Row>>
    where
        RawQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_optional_query(self.executor, self.query)
    }

    pub fn fetch_stream(self) -> PgFuture<'a, PgStream<'a, R>>
    where
        E: TransactionalExecutor,
        RawQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_stream_query(self.executor, self.query)
    }

    pub fn fetch_stream_as<Row>(self) -> PgFuture<'a, PgStream<'a, Row>>
    where
        E: TransactionalExecutor,
        RawQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_stream_query(self.executor, self.query)
    }

    pub fn fetch_chunked(self, n: usize) -> PgFuture<'a, PgStream<'a, Vec<R>>>
    where
        E: TransactionalExecutor,
        RawQuery<R>: Send + 'a,
        R: FromRow + Send + 'a,
    {
        fetch_chunked_query(self.executor, self.query, n)
    }

    pub fn fetch_chunked_as<Row>(self, n: usize) -> PgFuture<'a, PgStream<'a, Vec<Row>>>
    where
        E: TransactionalExecutor,
        RawQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_chunked_query(self.executor, self.query, n)
    }
}
