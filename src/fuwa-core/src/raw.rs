use std::marker::PhantomData;

use crate::{BindValue, IntoBindValue, RenderQuery, RenderedQuery, Result};

/// A raw SQL statement with owned bind parameters.
///
/// This is intended as a safe escape hatch for SQL constructs the typed DSL does not
/// model yet. Values must still be supplied through `.bind(...)`; callers should not
/// interpolate user data into the SQL string.
#[derive(Debug)]
pub struct RawQuery<R = ()> {
    sql: String,
    binds: Vec<BindValue>,
    marker: PhantomData<fn() -> R>,
}

/// Create a raw SQL query with separately collected bind values.
pub fn raw(sql: impl Into<String>) -> RawQuery {
    RawQuery {
        sql: sql.into(),
        binds: Vec::new(),
        marker: PhantomData,
    }
}

impl<R> RawQuery<R> {
    /// Add a bind value. The SQL string should contain the matching PostgreSQL `$N`
    /// placeholder.
    pub fn bind<T>(mut self, value: T) -> Self
    where
        T: IntoBindValue,
    {
        self.binds.push(Box::new(value.into_stored()));
        self
    }

    /// Override the associated record marker for documentation and type inference.
    pub fn record<T>(self) -> RawQuery<T> {
        RawQuery {
            sql: self.sql,
            binds: self.binds,
            marker: PhantomData,
        }
    }

    pub fn render(self) -> Result<RenderedQuery> {
        RenderQuery::render(self)
    }
}

impl<R> RenderQuery for RawQuery<R> {
    fn render(self) -> Result<RenderedQuery> {
        Ok(RenderedQuery::new(self.sql, self.binds))
    }
}
