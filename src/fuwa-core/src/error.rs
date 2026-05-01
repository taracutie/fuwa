use thiserror::Error as ThisError;

/// Result type used across `fuwa` crates.
pub type Result<T> = std::result::Result<T, Error>;

/// Structured error type for rendering, execution, decoding, and code generation.
#[derive(Debug, ThisError)]
pub enum Error {
    /// SQL rendering failed.
    #[error("render error: {0}")]
    Render(String),

    /// A query builder was missing required parts or had inconsistent parts.
    #[error("invalid query shape: {0}")]
    InvalidQueryShape(String),

    /// Query execution failed in an executor crate.
    #[error("execution error: {0}")]
    Execution(String),

    /// A database row could not be decoded into the requested Rust shape.
    #[error("row decoding error: {0}")]
    RowDecode(String),

    /// Schema introspection or generated source creation failed.
    #[error("codegen error: {0}")]
    Codegen(String),

    /// Introspection found a PostgreSQL type that the MVP does not map yet.
    #[error("unsupported PostgreSQL type: {0}")]
    UnsupportedPostgresType(String),

    /// Filesystem I/O failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Postgres unique-constraint violation (SQLSTATE 23505).
    #[error("unique violation: {message}")]
    UniqueViolation {
        constraint: Option<String>,
        message: String,
    },

    /// Postgres foreign-key violation (SQLSTATE 23503).
    #[error("foreign key violation: {message}")]
    ForeignKeyViolation {
        constraint: Option<String>,
        message: String,
    },

    /// Postgres check-constraint violation (SQLSTATE 23514).
    #[error("check violation: {message}")]
    CheckViolation {
        constraint: Option<String>,
        message: String,
    },

    /// Postgres not-null violation (SQLSTATE 23502).
    #[error("not null violation: {message}")]
    NotNullViolation {
        column: Option<String>,
        message: String,
    },

    /// Serialization failure (SQLSTATE 40001) ~ retry the transaction.
    #[error("serialization failure: {0}")]
    SerializationFailure(String),

    /// Deadlock detected (SQLSTATE 40P01) ~ retry the transaction.
    #[error("deadlock detected: {0}")]
    DeadlockDetected(String),

    /// Generic PostgreSQL error with SQLSTATE for callers to match on.
    #[error("postgres error ({sqlstate:?}): {message}")]
    Postgres {
        sqlstate: Option<String>,
        message: String,
    },
}

impl Error {
    pub fn render(message: impl Into<String>) -> Self {
        Self::Render(message.into())
    }

    pub fn invalid_query_shape(message: impl Into<String>) -> Self {
        Self::InvalidQueryShape(message.into())
    }

    pub fn execution(message: impl Into<String>) -> Self {
        Self::Execution(message.into())
    }

    pub fn row_decode(message: impl Into<String>) -> Self {
        Self::RowDecode(message.into())
    }

    pub fn codegen(message: impl Into<String>) -> Self {
        Self::Codegen(message.into())
    }

    pub fn unsupported_postgres_type(message: impl Into<String>) -> Self {
        Self::UnsupportedPostgresType(message.into())
    }

    pub fn postgres(sqlstate: Option<String>, message: impl Into<String>) -> Self {
        Self::Postgres {
            sqlstate,
            message: message.into(),
        }
    }
}
