use std::marker::PhantomData;

use crate::{ExprNode, JoinTarget, NotSingleColumn, SelectQuery};

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

    /// Create a typed field attached to this table using another field's name
    /// and type markers.
    ///
    /// This preserves the source field's nullability exactly. If a query can
    /// produce nulls, such as a field selected from the nullable side of a left
    /// join, use a nullable source field or create the alias field explicitly
    /// with `field::<T, Nullable>(...)`.
    pub const fn field_of<T, N>(self, source: Field<T, N>) -> Field<T, N> {
        self.field(source.name())
    }

    /// Create typed fields attached to this table using another field or tuple
    /// of fields as the source.
    ///
    /// This is useful for CTE handles created with `Table::unqualified(...)`.
    /// It preserves source field nullability exactly and does not infer
    /// nullable promotion from joins inside the CTE.
    pub fn fields_of<F>(self, source: F) -> F::Output
    where
        F: FieldSources,
    {
        source.rebind_to(self)
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

    /// Create a typed field attached to this subquery alias using another
    /// field's name and type markers.
    ///
    /// This preserves the source field's nullability exactly. If the subquery
    /// projection can produce nulls, such as a field selected from the nullable
    /// side of a left join, use a nullable source field or create the alias
    /// field explicitly with `field::<T, Nullable>(...)`.
    ///
    /// Panics if the source field is not selected directly by this
    /// subquery. Computed expressions and renamed columns still need
    /// `field::<T, N>(...)`.
    pub fn field_of<T, N>(&self, source: Field<T, N>) -> Field<T, N> {
        self.assert_field_selected(source);
        Table::unqualified(self.alias).field_of(source)
    }

    /// Create typed fields attached to this subquery alias using another field
    /// or tuple of fields as the source.
    ///
    /// This preserves source field nullability exactly and panics if any source
    /// field is not selected directly by this subquery.
    pub fn fields_of<F>(&self, source: F) -> F::Output
    where
        F: FieldSources,
    {
        source.assert_selected_by(self);
        source.rebind_to(Table::unqualified(self.alias))
    }

    fn assert_field_selected<T, N>(&self, source: Field<T, N>) {
        let source_ref = crate::FieldRef::new(source.table(), source.name());
        let selected = self.query.selection.iter().any(|item| {
            matches!(
                &item.expr,
                ExprNode::Field(field) if *field == source_ref
            )
        });

        assert!(
            selected,
            "field_of source field `{}` is not selected by subquery alias `{}`",
            source.name(),
            self.alias
        );
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

/// A field or tuple of fields that can be rebound to a table or subquery alias.
#[doc(hidden)]
pub trait FieldSources {
    type Output;

    #[doc(hidden)]
    fn rebind_to(self, table: Table) -> Self::Output;

    #[doc(hidden)]
    fn assert_selected_by(&self, subquery: &AliasedSubquery);
}

impl<T, N> FieldSources for Field<T, N> {
    type Output = Field<T, N>;

    fn rebind_to(self, table: Table) -> Self::Output {
        table.field_of(self)
    }

    fn assert_selected_by(&self, subquery: &AliasedSubquery) {
        subquery.assert_field_selected(*self)
    }
}

macro_rules! impl_tuple_field_sources {
    ($($ty:ident $var:ident),+ $(,)?) => {
        impl<$($ty),+> FieldSources for ($($ty,)+)
        where
            $($ty: FieldSources),+
        {
            type Output = ($($ty::Output,)+);

            fn rebind_to(self, table: Table) -> Self::Output {
                let ($($var,)+) = self;
                ($($var.rebind_to(table),)+)
            }

            fn assert_selected_by(&self, subquery: &AliasedSubquery) {
                let ($($var,)+) = self;
                $(
                    $var.assert_selected_by(subquery);
                )+
            }
        }
    };
}

impl_tuple_field_sources!(A a);
impl_tuple_field_sources!(A a, B b);
impl_tuple_field_sources!(A a, B b, C c);
impl_tuple_field_sources!(A a, B b, C c, D d);
impl_tuple_field_sources!(A a, B b, C c, D d, E e);
impl_tuple_field_sources!(A a, B b, C c, D d, E e, F f);
impl_tuple_field_sources!(A a, B b, C c, D d, E e, F f, G g);
impl_tuple_field_sources!(A a, B b, C c, D d, E e, F f, G g, H h);
