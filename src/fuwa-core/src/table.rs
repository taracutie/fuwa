use std::marker::PhantomData;

use crate::JoinTarget;

/// Marker for non-null SQL expressions and fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NotNull {}

/// Marker for nullable SQL expressions and fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Nullable {}

/// A PostgreSQL table identifier, optionally schema-qualified and aliased.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Table {
    schema: Option<&'static str>,
    name: &'static str,
    alias: Option<&'static str>,
}

impl Table {
    /// Create a schema-qualified table.
    pub const fn new(schema: &'static str, name: &'static str) -> Self {
        Self {
            schema: Some(schema),
            name,
            alias: None,
        }
    }

    /// Create an unqualified table.
    pub const fn unqualified(name: &'static str) -> Self {
        Self {
            schema: None,
            name,
            alias: None,
        }
    }

    /// Return a copy of this table with an alias.
    pub const fn as_(self, alias: &'static str) -> Self {
        Self {
            schema: self.schema,
            name: self.name,
            alias: Some(alias),
        }
    }

    /// Create a field attached to this table.
    pub const fn field<T, N>(self, name: &'static str) -> Field<T, N> {
        Field::new(self, name)
    }

    /// Attach an `ON` condition for use with `join` or `left_join`.
    pub fn on(self, condition: crate::Condition) -> JoinTarget {
        JoinTarget {
            table: self,
            on: condition,
        }
    }

    pub const fn schema(self) -> Option<&'static str> {
        self.schema
    }

    pub const fn name(self) -> &'static str {
        self.name
    }

    pub const fn alias(self) -> Option<&'static str> {
        self.alias
    }

    pub(crate) fn same_identity(self, other: Self) -> bool {
        self.schema == other.schema && self.name == other.name
    }
}

/// A typed field belonging to a table.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Field<T, N = NotNull> {
    table: Table,
    name: &'static str,
    marker: PhantomData<fn() -> (T, N)>,
}

impl<T, N> Clone for Field<T, N> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, N> Copy for Field<T, N> {}

impl<T, N> Field<T, N> {
    /// Create a typed field.
    pub const fn new(table: Table, name: &'static str) -> Self {
        Self {
            table,
            name,
            marker: PhantomData,
        }
    }

    pub const fn table(self) -> Table {
        self.table
    }

    pub const fn name(self) -> &'static str {
        self.name
    }
}
