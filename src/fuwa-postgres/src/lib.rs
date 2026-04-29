//! Async `tokio-postgres` execution and row decoding for `fuwa`.

use std::future::Future;
use std::pin::Pin;

use futures_core::Stream;
use futures_util::{stream, StreamExt};
use fuwa_core::{
    raw, AliasedSubquery, Assignments, ConflictTarget, Context, DeleteQuery, Excluded, ExprList,
    InOperand, InOperandNode, InsertConflictBuilder, InsertQuery, JoinTarget, OrderByList,
    RawQuery, RenderQuery, RenderedQuery, SelectQuery, Selectable, Table, TableSource, UpdateQuery,
    WithBuilder,
};
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

/// Entry point for creating a connection-bound DSL context.
#[derive(Debug, Clone, Copy, Default)]
pub struct Dsl;

impl Dsl {
    pub fn using<C>(connection: C) -> DslContext<C>
    where
        C: ConnectionRef,
    {
        DslContext::new(connection)
    }
}

#[doc(hidden)]
pub trait ConnectionRef {
    type Client: GenericClient + Sync + ?Sized;

    fn as_client(&self) -> &Self::Client;
}

impl<C> ConnectionRef for &C
where
    C: GenericClient + Sync + ?Sized,
{
    type Client = C;

    fn as_client(&self) -> &Self::Client {
        *self
    }
}

impl<C> ConnectionRef for &mut C
where
    C: GenericClient + Sync + ?Sized,
{
    type Client = C;

    fn as_client(&self) -> &Self::Client {
        &**self
    }
}

/// A jOOQ-style, connection-bound DSL context.
pub struct DslContext<C> {
    connection: C,
    context: Context,
}

impl<C> DslContext<C> {
    fn new(connection: C) -> Self {
        Self {
            connection,
            context: Context::new(),
        }
    }
}

impl<C> DslContext<C>
where
    C: ConnectionRef,
{
    pub fn select<S>(
        &self,
        selection: S,
    ) -> AttachedSelectQuery<'_, C::Client, S::Record, S::SingleSql>
    where
        S: Selectable,
    {
        AttachedSelectQuery {
            query: self.context.select(selection),
            client: self.connection.as_client(),
        }
    }

    pub fn with<R, S, Q>(&self, name: &'static str, query: Q) -> AttachedWithBuilder<'_, C::Client>
    where
        Q: IntoDetachedSelectQuery<R, S>,
    {
        AttachedWithBuilder {
            builder: self.context.with(name, query.into_select_query()),
            client: self.connection.as_client(),
        }
    }

    pub fn insert_into(&self, table: Table) -> AttachedInsertQuery<'_, C::Client> {
        AttachedInsertQuery {
            query: self.context.insert_into(table),
            client: self.connection.as_client(),
        }
    }

    pub fn update(&self, table: Table) -> AttachedUpdateQuery<'_, C::Client> {
        AttachedUpdateQuery {
            query: self.context.update(table),
            client: self.connection.as_client(),
        }
    }

    pub fn delete_from(&self, table: Table) -> AttachedDeleteQuery<'_, C::Client> {
        AttachedDeleteQuery {
            query: self.context.delete_from(table),
            client: self.connection.as_client(),
        }
    }

    pub fn raw(&self, sql: impl Into<String>) -> AttachedRawQuery<'_, C::Client> {
        AttachedRawQuery {
            query: raw(sql),
            client: self.connection.as_client(),
        }
    }
}

impl<C> DslContext<&mut C>
where
    C: GenericClient + Sync + ?Sized,
{
    /// Run a closure inside a PostgreSQL transaction.
    pub async fn transaction<F, T>(&mut self, f: F) -> Result<T>
    where
        F: for<'tx> FnOnce(DslContext<&'tx Transaction<'tx>>) -> TransactionFuture<'tx, T>,
    {
        let tx = self
            .connection
            .transaction()
            .await
            .map_err(|err| Error::execution(err.to_string()))?;

        match f(DslContext::new(&tx)).await {
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

fn execute_query<'a, C, Q>(client: &'a C, query: Q) -> PgFuture<'a, u64>
where
    C: GenericClient + Sync + ?Sized,
    Q: RenderQuery + Send + 'a,
{
    Box::pin(async move {
        let rendered = query.render()?;
        let params = rendered.bind_refs();
        client
            .execute(rendered.sql(), params.as_slice())
            .await
            .map_err(|err| Error::execution(err.to_string()))
    })
}

fn fetch_all_query<'a, C, Q, R>(client: &'a C, query: Q) -> PgFuture<'a, Vec<R>>
where
    C: GenericClient + Sync + ?Sized,
    Q: RenderQuery + Send + 'a,
    R: FromRow + Send + 'a,
{
    Box::pin(async move {
        let rendered = query.render()?;
        let params = rendered.bind_refs();
        let rows = client
            .query(rendered.sql(), params.as_slice())
            .await
            .map_err(|err| Error::execution(err.to_string()))?;

        rows.iter().map(R::from_row).collect()
    })
}

fn fetch_one_query<'a, C, Q, R>(client: &'a C, query: Q) -> PgFuture<'a, R>
where
    C: GenericClient + Sync + ?Sized,
    Q: RenderQuery + Send + 'a,
    R: FromRow + Send + 'a,
{
    Box::pin(async move {
        let rendered = query.render()?;
        let params = rendered.bind_refs();
        let row = client
            .query_one(rendered.sql(), params.as_slice())
            .await
            .map_err(|err| Error::execution(err.to_string()))?;

        R::from_row(&row)
    })
}

fn fetch_optional_query<'a, C, Q, R>(client: &'a C, query: Q) -> PgFuture<'a, Option<R>>
where
    C: GenericClient + Sync + ?Sized,
    Q: RenderQuery + Send + 'a,
    R: FromRow + Send + 'a,
{
    Box::pin(async move {
        let rendered = query.render()?;
        let params = rendered.bind_refs();
        let row = client
            .query_opt(rendered.sql(), params.as_slice())
            .await
            .map_err(|err| Error::execution(err.to_string()))?;

        row.as_ref().map(R::from_row).transpose()
    })
}

fn fetch_stream_query<'tx, 'client, Q, R>(
    transaction: &'tx Transaction<'client>,
    query: Q,
) -> PgFuture<'tx, PgStream<'tx, R>>
where
    'client: 'tx,
    Q: RenderQuery + Send + 'tx,
    R: FromRow + Send + 'tx,
{
    Box::pin(async move {
        let rendered = query.render()?;
        let params = rendered.bind_refs();
        let portal = transaction
            .bind(rendered.sql(), params.as_slice())
            .await
            .map_err(|err| Error::execution(err.to_string()))?;

        Ok(portal_stream(transaction, portal))
    })
}

fn fetch_chunked_query<'tx, 'client, Q, R>(
    transaction: &'tx Transaction<'client>,
    query: Q,
    n: usize,
) -> PgFuture<'tx, PgStream<'tx, Vec<R>>>
where
    'client: 'tx,
    Q: RenderQuery + Send + 'tx,
    R: FromRow + Send + 'tx,
{
    Box::pin(async move {
        if n == 0 {
            return Err(Error::invalid_query_shape(
                "fetch_chunked requires a chunk size greater than 0",
            ));
        }

        let rows = fetch_stream_query::<_, R>(transaction, query).await?;
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

#[doc(hidden)]
pub trait IntoDetachedSelectQuery<R, S> {
    fn into_select_query(self) -> SelectQuery<R, S>;
}

impl<R, S> IntoDetachedSelectQuery<R, S> for SelectQuery<R, S> {
    fn into_select_query(self) -> SelectQuery<R, S> {
        self
    }
}

pub struct AttachedSelectQuery<'a, C: ?Sized, R = (), S = fuwa_core::NotSingleColumn> {
    query: SelectQuery<R, S>,
    client: &'a C,
}

impl<'a, C, R, S> IntoDetachedSelectQuery<R, S> for AttachedSelectQuery<'a, C, R, S>
where
    C: ?Sized,
{
    fn into_select_query(self) -> SelectQuery<R, S> {
        self.query
    }
}

impl<'a, C, T, R> InOperand<T> for AttachedSelectQuery<'a, C, R, T>
where
    C: ?Sized,
{
    fn into_in_operand(self) -> InOperandNode {
        self.query.into_in_operand()
    }
}

impl<'a, C, R, S> AttachedSelectQuery<'a, C, R, S>
where
    C: GenericClient + Sync + ?Sized,
{
    pub fn distinct(self) -> Self {
        Self {
            query: self.query.distinct(),
            client: self.client,
        }
    }

    pub fn distinct_on<E>(self, columns: E) -> Self
    where
        E: ExprList,
    {
        Self {
            query: self.query.distinct_on(columns),
            client: self.client,
        }
    }

    pub fn from<Source>(self, source: Source) -> Self
    where
        Source: TableSource,
    {
        Self {
            query: self.query.from(source),
            client: self.client,
        }
    }

    pub fn join(self, target: JoinTarget) -> Self {
        Self {
            query: self.query.join(target),
            client: self.client,
        }
    }

    pub fn left_join(self, target: JoinTarget) -> Self {
        Self {
            query: self.query.left_join(target),
            client: self.client,
        }
    }

    pub fn where_(self, condition: fuwa_core::Condition) -> Self {
        Self {
            query: self.query.where_(condition),
            client: self.client,
        }
    }

    pub fn group_by<E>(self, group_by: E) -> Self
    where
        E: ExprList,
    {
        Self {
            query: self.query.group_by(group_by),
            client: self.client,
        }
    }

    pub fn having(self, condition: fuwa_core::Condition) -> Self {
        Self {
            query: self.query.having(condition),
            client: self.client,
        }
    }

    pub fn order_by<O>(self, order_by: O) -> Self
    where
        O: OrderByList,
    {
        Self {
            query: self.query.order_by(order_by),
            client: self.client,
        }
    }

    pub fn limit(self, limit: i64) -> Self {
        Self {
            query: self.query.limit(limit),
            client: self.client,
        }
    }

    pub fn offset(self, offset: i64) -> Self {
        Self {
            query: self.query.offset(offset),
            client: self.client,
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
        execute_query(self.client, self.query)
    }

    pub fn fetch_all<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        SelectQuery<R, S>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.client, self.query)
    }

    pub fn fetch_one<Row>(self) -> PgFuture<'a, Row>
    where
        SelectQuery<R, S>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.client, self.query)
    }

    pub fn fetch_optional<Row>(self) -> PgFuture<'a, Option<Row>>
    where
        SelectQuery<R, S>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_optional_query(self.client, self.query)
    }
}

impl<'tx, 'client, R, S> AttachedSelectQuery<'tx, Transaction<'client>, R, S>
where
    'client: 'tx,
{
    pub fn fetch_stream<Row>(self) -> PgFuture<'tx, PgStream<'tx, Row>>
    where
        SelectQuery<R, S>: Send + 'tx,
        Row: FromRow + Send + 'tx,
    {
        fetch_stream_query(self.client, self.query)
    }

    pub fn fetch_chunked<Row>(self, n: usize) -> PgFuture<'tx, PgStream<'tx, Vec<Row>>>
    where
        SelectQuery<R, S>: Send + 'tx,
        Row: FromRow + Send + 'tx,
    {
        fetch_chunked_query(self.client, self.query, n)
    }
}

pub struct AttachedWithBuilder<'a, C: ?Sized> {
    builder: WithBuilder,
    client: &'a C,
}

impl<'a, C> AttachedWithBuilder<'a, C>
where
    C: GenericClient + Sync + ?Sized,
{
    pub fn with<R, S, Q>(self, name: &'static str, query: Q) -> Self
    where
        Q: IntoDetachedSelectQuery<R, S>,
    {
        Self {
            builder: self.builder.with(name, query.into_select_query()),
            client: self.client,
        }
    }

    pub fn select<S>(self, selection: S) -> AttachedSelectQuery<'a, C, S::Record, S::SingleSql>
    where
        S: Selectable,
    {
        AttachedSelectQuery {
            query: self.builder.select(selection),
            client: self.client,
        }
    }
}

pub struct AttachedInsertQuery<'a, C: ?Sized, R = ()> {
    query: InsertQuery<R>,
    client: &'a C,
}

impl<'a, C, R> AttachedInsertQuery<'a, C, R>
where
    C: GenericClient + Sync + ?Sized,
{
    pub fn values<A>(self, assignments: A) -> Self
    where
        A: Assignments,
    {
        Self {
            query: self.query.values(assignments),
            client: self.client,
        }
    }

    pub fn values_many<I, A>(self, rows: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: Assignments,
    {
        Self {
            query: self.query.values_many(rows),
            client: self.client,
        }
    }

    pub fn on_conflict<T>(self, target: T) -> AttachedInsertConflictBuilder<'a, C, R>
    where
        T: ConflictTarget,
    {
        AttachedInsertConflictBuilder {
            builder: self.query.on_conflict(target),
            client: self.client,
        }
    }

    pub fn returning<S>(self, selection: S) -> AttachedInsertQuery<'a, C, S::Record>
    where
        S: Selectable,
    {
        AttachedInsertQuery {
            query: self.query.returning(selection),
            client: self.client,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        self.query.render()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        InsertQuery<R>: Send + 'a,
    {
        execute_query(self.client, self.query)
    }

    pub fn fetch_all<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        InsertQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.client, self.query)
    }

    pub fn fetch_one<Row>(self) -> PgFuture<'a, Row>
    where
        InsertQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.client, self.query)
    }

    pub fn fetch_optional<Row>(self) -> PgFuture<'a, Option<Row>>
    where
        InsertQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_optional_query(self.client, self.query)
    }
}

pub struct AttachedInsertConflictBuilder<'a, C: ?Sized, R = ()> {
    builder: InsertConflictBuilder<R>,
    client: &'a C,
}

impl<'a, C, R> AttachedInsertConflictBuilder<'a, C, R>
where
    C: GenericClient + Sync + ?Sized,
{
    pub fn do_nothing(self) -> AttachedInsertQuery<'a, C, R> {
        AttachedInsertQuery {
            query: self.builder.do_nothing(),
            client: self.client,
        }
    }

    pub fn do_update<F, A>(self, f: F) -> AttachedInsertQuery<'a, C, R>
    where
        F: FnOnce(Excluded) -> A,
        A: Assignments,
    {
        AttachedInsertQuery {
            query: self.builder.do_update(f),
            client: self.client,
        }
    }
}

pub struct AttachedUpdateQuery<'a, C: ?Sized, R = ()> {
    query: UpdateQuery<R>,
    client: &'a C,
}

impl<'a, C, R> AttachedUpdateQuery<'a, C, R>
where
    C: GenericClient + Sync + ?Sized,
{
    pub fn set<A>(self, assignments: A) -> Self
    where
        A: Assignments,
    {
        Self {
            query: self.query.set(assignments),
            client: self.client,
        }
    }

    pub fn where_(self, condition: fuwa_core::Condition) -> Self {
        Self {
            query: self.query.where_(condition),
            client: self.client,
        }
    }

    pub fn returning<S>(self, selection: S) -> AttachedUpdateQuery<'a, C, S::Record>
    where
        S: Selectable,
    {
        AttachedUpdateQuery {
            query: self.query.returning(selection),
            client: self.client,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        self.query.render()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        UpdateQuery<R>: Send + 'a,
    {
        execute_query(self.client, self.query)
    }

    pub fn fetch_all<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        UpdateQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.client, self.query)
    }

    pub fn fetch_one<Row>(self) -> PgFuture<'a, Row>
    where
        UpdateQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.client, self.query)
    }

    pub fn fetch_optional<Row>(self) -> PgFuture<'a, Option<Row>>
    where
        UpdateQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_optional_query(self.client, self.query)
    }
}

pub struct AttachedDeleteQuery<'a, C: ?Sized, R = ()> {
    query: DeleteQuery<R>,
    client: &'a C,
}

impl<'a, C, R> AttachedDeleteQuery<'a, C, R>
where
    C: GenericClient + Sync + ?Sized,
{
    pub fn where_(self, condition: fuwa_core::Condition) -> Self {
        Self {
            query: self.query.where_(condition),
            client: self.client,
        }
    }

    pub fn returning<S>(self, selection: S) -> AttachedDeleteQuery<'a, C, S::Record>
    where
        S: Selectable,
    {
        AttachedDeleteQuery {
            query: self.query.returning(selection),
            client: self.client,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        self.query.render()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        DeleteQuery<R>: Send + 'a,
    {
        execute_query(self.client, self.query)
    }

    pub fn fetch_all<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        DeleteQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.client, self.query)
    }

    pub fn fetch_one<Row>(self) -> PgFuture<'a, Row>
    where
        DeleteQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.client, self.query)
    }

    pub fn fetch_optional<Row>(self) -> PgFuture<'a, Option<Row>>
    where
        DeleteQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_optional_query(self.client, self.query)
    }
}

pub struct AttachedRawQuery<'a, C: ?Sized, R = ()> {
    query: RawQuery<R>,
    client: &'a C,
}

impl<'a, C, R> AttachedRawQuery<'a, C, R>
where
    C: GenericClient + Sync + ?Sized,
{
    pub fn bind<T>(self, value: T) -> Self
    where
        T: fuwa_core::IntoBindValue,
    {
        Self {
            query: self.query.bind(value),
            client: self.client,
        }
    }

    pub fn record<T>(self) -> AttachedRawQuery<'a, C, T> {
        AttachedRawQuery {
            query: self.query.record(),
            client: self.client,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        self.query.render()
    }

    pub fn execute(self) -> PgFuture<'a, u64>
    where
        RawQuery<R>: Send + 'a,
    {
        execute_query(self.client, self.query)
    }

    pub fn fetch_all<Row>(self) -> PgFuture<'a, Vec<Row>>
    where
        RawQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_all_query(self.client, self.query)
    }

    pub fn fetch_one<Row>(self) -> PgFuture<'a, Row>
    where
        RawQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_one_query(self.client, self.query)
    }

    pub fn fetch_optional<Row>(self) -> PgFuture<'a, Option<Row>>
    where
        RawQuery<R>: Send + 'a,
        Row: FromRow + Send + 'a,
    {
        fetch_optional_query(self.client, self.query)
    }
}

impl<'tx, 'client, R> AttachedRawQuery<'tx, Transaction<'client>, R>
where
    'client: 'tx,
{
    pub fn fetch_stream<Row>(self) -> PgFuture<'tx, PgStream<'tx, Row>>
    where
        RawQuery<R>: Send + 'tx,
        Row: FromRow + Send + 'tx,
    {
        fetch_stream_query(self.client, self.query)
    }

    pub fn fetch_chunked<Row>(self, n: usize) -> PgFuture<'tx, PgStream<'tx, Vec<Row>>>
    where
        RawQuery<R>: Send + 'tx,
        Row: FromRow + Send + 'tx,
    {
        fetch_chunked_query(self.client, self.query, n)
    }
}
