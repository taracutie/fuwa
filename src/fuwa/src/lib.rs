//! Public facade crate for `fuwa`.
//!
//! `fuwa` re-exports the core typed SQL DSL and the PostgreSQL executor-bound
//! execution context. Most applications should depend on this crate rather than the
//! internal crates.
//!
//! ```compile_fail
//! use fuwa::prelude::*;
//!
//! #[derive(FromRow)]
//! struct UnsupportedField {
//!     value: std::rc::Rc<String>,
//! }
//! ```
//!
//! Streaming requires a transaction-bound executor; calling `fetch_stream` on
//! a pool is a compile error:
//!
//! ```compile_fail
//! use fuwa::prelude::*;
//!
//! async fn example(dsl: DslContext<Pool>) -> fuwa::Result<()> {
//!     let _stream = dsl.raw("select 1::bigint").fetch_stream_as::<i64>().await?;
//!     Ok(())
//! }
//! ```

pub mod core {
    pub use fuwa_core::*;
}

pub use fuwa_core::{
    abs, array_agg, avg, bind, bool_and, bool_or, case_when, cast, ceil, coalesce, concat, count,
    count_star, current_row, date_trunc, delete_from, dense_rank, exists, extract, first_value,
    floor, following, greatest, insert_into, jsonb_array_length, lag, last_value, lead, least,
    length, lower, max, min, not, not_exists, now, ntile, nullable, nullif, partition_by,
    preceding, quote_ident, rank, round, row_number, select, selection_as, string_agg, sum, trim,
    unbounded_following, unbounded_preceding, update, upper, with, AliasedExpr, AliasedSubquery,
    ArithmeticOp, ArrayAggInput, ArrayQuantifier, Assignment, Assignments, AvgOutput, BinaryOp,
    BindParam, BindValue, CaseWhen, CaseWhenStart, CoalesceArgs, CoalesceNullability,
    CoalesceNullabilityList, Condition, ConflictTarget, DeleteQuery, Error, Excluded, Expr,
    ExprList, ExprNode, Field, FieldRef, FieldSources, InOperand, InOperandNode,
    InsertConflictBuilder, InsertQuery, IntoBindValue, IntoCondition, IntoExistsQuery, IntoExpr,
    Join, JoinKind, JoinTarget, NotNull, NotSingleColumn, NullabilityOutput, Nullable,
    NullableIfEither, OrderByList, OrderDirection, OrderExpr, RenderQuery, RenderedQuery, Result,
    RoundingOutput, SelectItem, SelectQuery, Selectable, SelectionAs, SqlArrayElement, SqlJsonb,
    SqlNumeric, SqlTemporal, SqlType, SumOutput, Table, TableSource, TableSourceRef, UnaryOp,
    UpdateQuery, WindowFrame, WindowFrameBound, WindowFrameUnit, WindowFunction, WindowSpec,
    WithBuilder,
};
pub use fuwa_derive::{FromRow, Insertable, Patch};
pub use fuwa_postgres::{
    create_pool, create_pool_with_options, AttachedDeleteQuery, AttachedInsertConflictBuilder,
    AttachedInsertQuery, AttachedRawQuery, AttachedSelectQuery, AttachedUpdateQuery,
    AttachedWithBuilder, CopyInColumn, CopyInColumns, CopyInRow, CopyInWriter, Dsl, DslContext,
    Executor, FromRow, PgFuture, PgStream, PgType, Pool, PoolOptions, Row, StreamExt,
    TransactionFuture, TransactionalExecutor,
};
pub use fuwa_query_macro::query;

/// Re-exported external types used by generated schema modules.
pub mod types {
    pub use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
    pub use rust_decimal::Decimal;
    pub use serde_json::Value;
    pub use uuid::Uuid;
}

/// PostgreSQL-specific execution exports.
pub mod postgres {
    pub use fuwa_postgres::{
        create_pool, create_pool_with_options, types, AttachedDeleteQuery,
        AttachedInsertConflictBuilder, AttachedInsertQuery, AttachedRawQuery, AttachedSelectQuery,
        AttachedUpdateQuery, AttachedWithBuilder, CopyInColumn, CopyInColumns, CopyInRow,
        CopyInWriter, Dsl, DslContext, Executor, FromRow, PgFuture, PgStream, PgType, Pool,
        PoolOptions, Row, StreamExt, TransactionFuture, TransactionalExecutor,
    };
}

/// Common imports for hand-written queries and generated schema modules.
pub mod prelude {
    pub use fuwa_core::{
        abs, array_agg, avg, bind, bool_and, bool_or, case_when, cast, ceil, coalesce, concat,
        count, count_star, current_row, date_trunc, delete_from, dense_rank, exists, extract,
        first_value, floor, following, greatest, insert_into, jsonb_array_length, lag, last_value,
        lead, least, length, lower, max, min, not, not_exists, now, ntile, nullable, nullif,
        partition_by, preceding, rank, round, row_number, select, selection_as, string_agg, sum,
        trim, unbounded_following, unbounded_preceding, update, upper, with, AliasedExpr,
        AliasedSubquery, ArrayAggInput, Assignment, Assignments, AvgOutput, CaseWhen,
        CaseWhenStart, Condition, ConflictTarget, DeleteQuery, Error, Excluded, Expr, ExprList,
        Field, FieldSources, InOperand, InsertConflictBuilder, InsertQuery, IntoCondition,
        IntoExpr, NotNull, NotSingleColumn, Nullable, OrderByList, OrderDirection, OrderExpr,
        RenderQuery, RenderedQuery, Result, RoundingOutput, SelectQuery, Selectable,
        SqlArrayElement, SqlJsonb, SqlTemporal, SqlType, SumOutput, Table, TableSource,
        UpdateQuery, WindowFrameBound, WindowFunction, WindowSpec, WithBuilder,
    };
    pub use fuwa_derive::{FromRow, Insertable, Patch};
    pub use fuwa_postgres::{
        create_pool, create_pool_with_options, Dsl, DslContext, Executor, FromRow, PgStream, Pool,
        PoolOptions, StreamExt, TransactionFuture, TransactionalExecutor,
    };
    pub use fuwa_query_macro::query;
}
