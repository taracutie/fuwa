use std::marker::PhantomData;

use crate::{JoinTarget, NotSingleColumn, SelectQuery};

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
            source: self.into_table_source(),
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

/// A concrete source that can appear in a `FROM` or `JOIN` clause.
#[derive(Debug)]
pub struct TableSourceRef {
    pub(crate) kind: TableSourceKind,
}

#[derive(Debug)]
pub(crate) enum TableSourceKind {
    Table(Table),
    Subquery(AliasedSubquery),
}

impl TableSourceRef {
    pub(crate) const fn table(table: Table) -> Self {
        Self {
            kind: TableSourceKind::Table(table),
        }
    }

    pub(crate) fn subquery(subquery: AliasedSubquery) -> Self {
        Self {
            kind: TableSourceKind::Subquery(subquery),
        }
    }
}

/// Something that can appear in a `FROM` or `JOIN` clause.
pub trait TableSource {
    #[doc(hidden)]
    fn into_table_source(self) -> TableSourceRef;

    /// Attach an `ON` condition for use with `join` or `left_join`.
    fn on(self, condition: crate::Condition) -> JoinTarget
    where
        Self: Sized,
    {
        JoinTarget {
            source: self.into_table_source(),
            on: condition,
        }
    }
}

impl TableSource for Table {
    fn into_table_source(self) -> TableSourceRef {
        TableSourceRef::table(self)
    }
}

/// A `SELECT` query aliased for use as a `FROM` or `JOIN` source.
#[derive(Debug)]
pub struct AliasedSubquery {
    pub(crate) query: Box<SelectQuery<(), NotSingleColumn>>,
    pub(crate) alias: &'static str,
}

impl AliasedSubquery {
    pub(crate) fn new<R, S>(query: SelectQuery<R, S>, alias: &'static str) -> Self {
        Self {
            query: Box::new(query.erase_record()),
            alias,
        }
    }

    /// Create a typed field attached to this subquery alias.
    pub fn field<T, N>(&self, name: &'static str) -> Field<T, N> {
        Table::unqualified(self.alias).field(name)
    }

    /// Attach an `ON` condition for use with `join` or `left_join`.
    pub fn on(self, condition: crate::Condition) -> JoinTarget {
        JoinTarget {
            source: self.into_table_source(),
            on: condition,
        }
    }

    pub const fn alias(&self) -> &'static str {
        self.alias
    }
}

impl TableSource for AliasedSubquery {
    fn into_table_source(self) -> TableSourceRef {
        TableSourceRef::subquery(self)
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
