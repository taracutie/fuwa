//! Map `tokio_postgres::Error` into typed `fuwa_core::Error` variants.

use tokio_postgres::error::{DbError, SqlState};

use fuwa_core::Error;

/// Convert a `tokio_postgres::Error` into a typed `fuwa_core::Error`.
///
/// Recognised SQLSTATE codes get their own variants so callers can match
/// for retryable / constraint-violation conditions; everything else falls
/// through to `Error::Postgres { sqlstate, message }`.
pub(crate) fn map_pg_error(err: tokio_postgres::Error) -> Error {
    let db_err: Option<&DbError> = err.as_db_error();
    let message = match db_err {
        Some(db) => format!("{}: {}", db.severity(), db.message()),
        None => err.to_string(),
    };
    let code = db_err.map(DbError::code).cloned();
    let constraint = db_err.and_then(|e| e.constraint().map(str::to_owned));
    let column = db_err.and_then(|e| e.column().map(str::to_owned));

    match code.as_ref() {
        Some(code) if code == &SqlState::UNIQUE_VIOLATION => Error::UniqueViolation {
            constraint,
            message,
        },
        Some(code) if code == &SqlState::FOREIGN_KEY_VIOLATION => Error::ForeignKeyViolation {
            constraint,
            message,
        },
        Some(code) if code == &SqlState::CHECK_VIOLATION => Error::CheckViolation {
            constraint,
            message,
        },
        Some(code) if code == &SqlState::NOT_NULL_VIOLATION => {
            Error::NotNullViolation { column, message }
        }
        Some(code) if code == &SqlState::T_R_SERIALIZATION_FAILURE => {
            Error::SerializationFailure(message)
        }
        Some(code) if code == &SqlState::T_R_DEADLOCK_DETECTED => Error::DeadlockDetected(message),
        _ => Error::Postgres {
            sqlstate: code.map(|c| c.code().to_owned()),
            message,
        },
    }
}
