//! Public facade crate for `fuwa`.
//!
//! `fuwa` re-exports the core typed SQL DSL and the PostgreSQL connection-bound
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

pub mod core {
    pub use fuwa_core::*;
}

pub use fuwa_core::{
    array_agg, avg, bind, bool_and, bool_or, case_when, coalesce, concat, count, count_star,
    jsonb_array_length, max, min, not, nullable, nullif, quote_ident, selection_as, string_agg,
    sum, AliasedSubquery, ArithmeticOp, ArrayAggInput, ArrayQuantifier, Assignment, Assignments,
    AvgOutput, BinaryOp, BindParam, BindValue, CaseWhen, CaseWhenStart, CoalesceArgs,
    CoalesceNullability, CoalesceNullabilityList, Condition, ConflictTarget, DeleteQuery, Error,
    Excluded, Expr, ExprList, ExprNode, Field, FieldRef, InOperand, InOperandNode,
    InsertConflictBuilder, InsertQuery, IntoBindValue, IntoExpr, Join, JoinKind, JoinTarget,
    NotNull, NotSingleColumn, NullabilityOutput, Nullable, NullableIfEither, OrderByList,
    OrderDirection, OrderExpr, RenderQuery, RenderedQuery, Result, SelectItem, SelectQuery,
    Selectable, SelectionAs, SqlArrayElement, SqlJsonb, SqlNumeric, SumOutput, Table, TableSource,
    TableSourceRef, UnaryOp, UpdateQuery, WithBuilder,
};
pub use fuwa_derive::FromRow;
pub use fuwa_postgres::{
    AttachedDeleteQuery, AttachedInsertConflictBuilder, AttachedInsertQuery, AttachedRawQuery,
    AttachedSelectQuery, AttachedUpdateQuery, AttachedWithBuilder, Dsl, DslContext, FromRow,
    PgFuture, PgStream, Row, TransactionFuture,
};

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
        types, AttachedDeleteQuery, AttachedInsertConflictBuilder, AttachedInsertQuery,
        AttachedRawQuery, AttachedSelectQuery, AttachedUpdateQuery, AttachedWithBuilder, Dsl,
        DslContext, FromRow, PgFuture, PgStream, Row, TransactionFuture,
    };
}

/// Common imports for hand-written queries and generated schema modules.
pub mod prelude {
    pub use fuwa_core::{
        array_agg, avg, bind, bool_and, bool_or, case_when, coalesce, concat, count, count_star,
        jsonb_array_length, max, min, not, nullable, nullif, selection_as, string_agg, sum,
        AliasedSubquery, ArrayAggInput, Assignment, Assignments, AvgOutput, CaseWhen,
        CaseWhenStart, Condition, ConflictTarget, DeleteQuery, Error, Excluded, Expr, ExprList,
        Field, InOperand, InsertConflictBuilder, InsertQuery, IntoExpr, NotNull, NotSingleColumn,
        Nullable, OrderByList, OrderDirection, OrderExpr, RenderQuery, RenderedQuery, Result,
        SelectQuery, Selectable, SqlArrayElement, SqlJsonb, SumOutput, Table, TableSource,
        UpdateQuery, WithBuilder,
    };
    pub use fuwa_derive::FromRow;
    pub use fuwa_postgres::{Dsl, DslContext, FromRow, PgStream, TransactionFuture};
}
