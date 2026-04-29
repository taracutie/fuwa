//! Public facade crate for `fuwa`.
//!
//! `fuwa` re-exports the core typed SQL DSL and the PostgreSQL async execution extension
//! methods. Most applications should depend on this crate rather than the internal crates.
//!
//! ```compile_fail
//! use fuwa::prelude::*;
//!
//! #[derive(FromRow)]
//! struct UnsupportedField {
//!     value: std::rc::Rc<String>,
//! }
//! ```

pub use fuwa_core::*;
pub use fuwa_derive::FromRow;
pub use fuwa_postgres::{
    transaction, FromRow, PgFuture, PgQueryExt, PgStream, Row, TransactionFuture,
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
        transaction, types, FromRow, PgFuture, PgQueryExt, PgStream, Row, TransactionFuture,
    };
}

/// Common imports for hand-written queries and generated schema modules.
pub mod prelude {
    pub use fuwa_core::prelude::*;
    pub use fuwa_derive::FromRow;
    pub use fuwa_postgres::{transaction, FromRow, PgQueryExt, PgStream, TransactionFuture};
}
