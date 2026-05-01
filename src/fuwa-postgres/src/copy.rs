//! Binary `COPY ... FROM STDIN` writer for fast bulk inserts.

use std::marker::PhantomData;
use std::pin::Pin;

use postgres_types::{ToSql, Type};
use tokio_postgres::binary_copy::BinaryCopyInWriter;

use fuwa_core::{quote_ident, Field, NotNull, NullabilityOutput, Nullable, Table};

use crate::{pg_error::map_pg_error, AcquiredConnection, Error, Executor, Result};

/// Maps a Rust scalar type to its `tokio_postgres::types::Type`.
pub trait PgType {
    fn pg_type() -> Type;
}

macro_rules! impl_pg_type {
    ($($ty:ty => $variant:ident),+ $(,)?) => {
        $(impl PgType for $ty {
            fn pg_type() -> Type { Type::$variant }
        })+
    };
}

impl_pg_type!(
    i16 => INT2,
    i32 => INT4,
    i64 => INT8,
    u32 => OID,
    f32 => FLOAT4,
    f64 => FLOAT8,
    bool => BOOL,
    String => TEXT,
    chrono::NaiveDate => DATE,
    chrono::NaiveDateTime => TIMESTAMP,
    chrono::DateTime<chrono::Utc> => TIMESTAMPTZ,
    rust_decimal::Decimal => NUMERIC,
    serde_json::Value => JSONB,
    uuid::Uuid => UUID,
    Vec<u8> => BYTEA,
);

impl PgType for Vec<i16> {
    fn pg_type() -> Type {
        Type::INT2_ARRAY
    }
}
impl PgType for Vec<i32> {
    fn pg_type() -> Type {
        Type::INT4_ARRAY
    }
}
impl PgType for Vec<i64> {
    fn pg_type() -> Type {
        Type::INT8_ARRAY
    }
}
impl PgType for Vec<String> {
    fn pg_type() -> Type {
        Type::TEXT_ARRAY
    }
}
impl PgType for Vec<bool> {
    fn pg_type() -> Type {
        Type::BOOL_ARRAY
    }
}

impl_pg_type!(
    Vec<f32> => FLOAT4_ARRAY,
    Vec<f64> => FLOAT8_ARRAY,
    Vec<u32> => OID_ARRAY,
    Vec<chrono::NaiveDate> => DATE_ARRAY,
    Vec<chrono::NaiveDateTime> => TIMESTAMP_ARRAY,
    Vec<chrono::DateTime<chrono::Utc>> => TIMESTAMPTZ_ARRAY,
    Vec<rust_decimal::Decimal> => NUMERIC_ARRAY,
    Vec<serde_json::Value> => JSONB_ARRAY,
    Vec<uuid::Uuid> => UUID_ARRAY,
    Vec<Vec<u8>> => BYTEA_ARRAY,
);

/// A column in a `COPY ... (col1, col2, ...) FROM STDIN BINARY` statement.
pub trait CopyInColumn {
    type Value: ToSql + Sync + Send + 'static;
    fn table(&self) -> Table;
    fn name(&self) -> &'static str;
    fn pg_type() -> Type;
    fn copy_pg_type(&self) -> Type {
        Self::pg_type()
    }

    fn meta(&self) -> CopyInColumnMeta {
        CopyInColumnMeta::new(self.table(), self.name(), self.copy_pg_type())
    }
}

/// Metadata for a column in a `COPY ... FROM STDIN BINARY` statement.
#[derive(Debug, Clone)]
pub struct CopyInColumnMeta {
    table: Table,
    name: &'static str,
    pg_type: Type,
}

impl CopyInColumnMeta {
    pub fn new(table: Table, name: &'static str, pg_type: Type) -> Self {
        Self {
            table,
            name,
            pg_type,
        }
    }

    pub const fn table(&self) -> Table {
        self.table
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub fn pg_type(&self) -> Type {
        self.pg_type.clone()
    }
}

fn pg_type_from_field_metadata(pg_type: &str) -> Option<Type> {
    match pg_type {
        "json" => Some(Type::JSON),
        "jsonb" => Some(Type::JSONB),
        "_int2" => Some(Type::INT2_ARRAY),
        "_int4" => Some(Type::INT4_ARRAY),
        "_int8" => Some(Type::INT8_ARRAY),
        "_float4" => Some(Type::FLOAT4_ARRAY),
        "_float8" => Some(Type::FLOAT8_ARRAY),
        "_numeric" => Some(Type::NUMERIC_ARRAY),
        "_bool" => Some(Type::BOOL_ARRAY),
        "_text" => Some(Type::TEXT_ARRAY),
        "_varchar" => Some(Type::VARCHAR_ARRAY),
        "_bpchar" => Some(Type::BPCHAR_ARRAY),
        "_name" => Some(Type::NAME_ARRAY),
        "_uuid" => Some(Type::UUID_ARRAY),
        "_timestamp" => Some(Type::TIMESTAMP_ARRAY),
        "_timestamptz" => Some(Type::TIMESTAMPTZ_ARRAY),
        "_date" => Some(Type::DATE_ARRAY),
        "_json" => Some(Type::JSON_ARRAY),
        "_jsonb" => Some(Type::JSONB_ARRAY),
        "_oid" => Some(Type::OID_ARRAY),
        "_bytea" => Some(Type::BYTEA_ARRAY),
        _ => None,
    }
}

/// Maps a `Field<T, NotNull>` to a non-null COPY column value.
impl<T> CopyInColumn for Field<T, NotNull>
where
    T: PgType + ToSql + Sync + Send + 'static,
    NotNull: NullabilityOutput<T>,
    <NotNull as NullabilityOutput<T>>::Output: ToSql + Sync + Send + 'static,
{
    type Value = <NotNull as NullabilityOutput<T>>::Output;
    fn table(&self) -> Table {
        Field::table(*self)
    }
    fn name(&self) -> &'static str {
        Field::name(*self)
    }
    fn pg_type() -> Type {
        T::pg_type()
    }
    fn copy_pg_type(&self) -> Type {
        Field::pg_type_name(*self)
            .and_then(pg_type_from_field_metadata)
            .unwrap_or_else(T::pg_type)
    }
}

/// Maps a `Field<T, Nullable>` to an `Option<T>` COPY column value.
impl<T> CopyInColumn for Field<T, Nullable>
where
    T: PgType + ToSql + Sync + Send + 'static,
    Nullable: NullabilityOutput<T>,
    <Nullable as NullabilityOutput<T>>::Output: ToSql + Sync + Send + 'static,
{
    type Value = <Nullable as NullabilityOutput<T>>::Output;
    fn table(&self) -> Table {
        Field::table(*self)
    }
    fn name(&self) -> &'static str {
        Field::name(*self)
    }
    fn pg_type() -> Type {
        T::pg_type()
    }
    fn copy_pg_type(&self) -> Type {
        Field::pg_type_name(*self)
            .and_then(pg_type_from_field_metadata)
            .unwrap_or_else(T::pg_type)
    }
}

/// A tuple of `CopyInColumn` declaring a row's shape.
pub trait CopyInColumns {
    type Row: CopyInRow;
    fn into_meta(self) -> Vec<CopyInColumnMeta>;
}

macro_rules! impl_copy_in_columns {
    ($($ty:ident $var:ident),+ $(,)?) => {
        impl<$($ty),+> CopyInColumns for ($($ty,)+)
        where
            $($ty: CopyInColumn),+,
            ($(<$ty as CopyInColumn>::Value,)+): CopyInRow,
        {
            type Row = ($(<$ty as CopyInColumn>::Value,)+);
            fn into_meta(self) -> Vec<CopyInColumnMeta> {
                let ($($var,)+) = self;
                vec![$($var.meta(),)+]
            }
        }
    };
}

impl_copy_in_columns!(A a);
impl_copy_in_columns!(A a, B b);
impl_copy_in_columns!(A a, B b, C c);
impl_copy_in_columns!(A a, B b, C c, D d);
impl_copy_in_columns!(A a, B b, C c, D d, E e);
impl_copy_in_columns!(A a, B b, C c, D d, E e, F f);
impl_copy_in_columns!(A a, B b, C c, D d, E e, F f, G g);
impl_copy_in_columns!(A a, B b, C c, D d, E e, F f, G g, H h);

/// A tuple of values forming a single row to be sent to the COPY writer.
pub trait CopyInRow: Send {
    fn collect_refs<'a>(&'a self) -> Vec<&'a (dyn ToSql + Sync)>;
}

macro_rules! impl_copy_in_row {
    ($($ty:ident $idx:tt),+ $(,)?) => {
        impl<$($ty),+> CopyInRow for ($($ty,)+)
        where
            $($ty: ToSql + Sync + Send),+
        {
            fn collect_refs<'a>(&'a self) -> Vec<&'a (dyn ToSql + Sync)> {
                vec![$(&self.$idx as &(dyn ToSql + Sync),)+]
            }
        }
    };
}

impl_copy_in_row!(A 0);
impl_copy_in_row!(A 0, B 1);
impl_copy_in_row!(A 0, B 1, C 2);
impl_copy_in_row!(A 0, B 1, C 2, D 3);
impl_copy_in_row!(A 0, B 1, C 2, D 3, E 4);
impl_copy_in_row!(A 0, B 1, C 2, D 3, E 4, F 5);
impl_copy_in_row!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
impl_copy_in_row!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);

/// A binary COPY writer, ready to receive typed rows.
pub struct CopyInWriter<R> {
    writer: Pin<Box<BinaryCopyInWriter>>,
    _phantom: PhantomData<fn(R)>,
}

impl<R: CopyInRow> CopyInWriter<R> {
    /// Send a single row to the writer.
    pub async fn send(&mut self, row: R) -> Result<()> {
        let refs = row.collect_refs();
        self.writer
            .as_mut()
            .write(&refs)
            .await
            .map_err(map_pg_error)
    }
}

fn validate_copy_columns(table: Table, columns: &[CopyInColumnMeta]) -> Result<()> {
    for column in columns {
        let column_table = column.table();
        if column_table.schema() != table.schema() || column_table.name() != table.name() {
            return Err(Error::invalid_query_shape(format!(
                "COPY column {} does not belong to target table {}",
                column.name(),
                table.name()
            )));
        }
    }
    Ok(())
}

fn build_copy_sql(table: Table, columns: &[CopyInColumnMeta]) -> Result<String> {
    let mut sql = String::from("copy ");
    if let Some(schema) = table.schema() {
        sql.push_str(&quote_ident(schema)?);
        sql.push('.');
    }
    sql.push_str(&quote_ident(table.name())?);
    sql.push_str(" (");
    for (idx, column) in columns.iter().enumerate() {
        if idx > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&quote_ident(column.name())?);
    }
    sql.push_str(") from stdin binary");
    Ok(sql)
}

pub(crate) async fn copy_in_binary_with_executor<E, C, F>(
    executor: &E,
    table: Table,
    columns: C,
    f: F,
) -> Result<u64>
where
    E: Executor,
    C: CopyInColumns,
    F: for<'w> AsyncFnOnce(&'w mut CopyInWriter<C::Row>) -> Result<()>,
{
    let columns = columns.into_meta();
    validate_copy_columns(table, &columns)?;
    let types = columns
        .iter()
        .map(CopyInColumnMeta::pg_type)
        .collect::<Vec<_>>();
    let sql = build_copy_sql(table, &columns)?;
    let conn = executor.acquire().await?;
    let sink = conn.copy_in_binary_sink(sql.as_str()).await?;
    let writer = BinaryCopyInWriter::new(sink, &types);
    let mut writer = CopyInWriter {
        writer: Box::pin(writer),
        _phantom: PhantomData,
    };
    f(&mut writer).await?;
    let rows = writer
        .writer
        .as_mut()
        .finish()
        .await
        .map_err(map_pg_error)?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(non_upper_case_globals)]
    mod users {
        use fuwa_core::{Field, NotNull, Table};

        pub const table: Table = Table::new("public", "users");
        pub const id: Field<i64, NotNull> = Field::new(table, "id");
        pub const email: Field<String, NotNull> = Field::new(table, "email");
        pub const json_profile: Field<serde_json::Value, NotNull> =
            Field::new_with_pg_type(table, "json_profile", "json");
        pub const jsonb_profile: Field<serde_json::Value, NotNull> =
            Field::new_with_pg_type(table, "jsonb_profile", "jsonb");
        pub const untyped_profile: Field<serde_json::Value, NotNull> =
            Field::new(table, "untyped_profile");
        pub const text_tags: Field<Vec<String>, NotNull> = Field::new(table, "text_tags");
        pub const varchar_tags: Field<Vec<String>, NotNull> =
            Field::new_with_pg_type(table, "varchar_tags", "_varchar");
        pub const bpchar_tags: Field<Vec<String>, NotNull> =
            Field::new_with_pg_type(table, "bpchar_tags", "_bpchar");
        pub const json_items: Field<Vec<serde_json::Value>, NotNull> =
            Field::new_with_pg_type(table, "json_items", "_json");
        pub const jsonb_items: Field<Vec<serde_json::Value>, NotNull> =
            Field::new_with_pg_type(table, "jsonb_items", "_jsonb");
        pub const untyped_json_items: Field<Vec<serde_json::Value>, NotNull> =
            Field::new(table, "untyped_json_items");
    }

    #[allow(non_upper_case_globals)]
    mod posts {
        use fuwa_core::{Field, NotNull, Table};

        pub const table: Table = Table::new("blog", "posts");
        pub const id: Field<i64, NotNull> = Field::new(table, "id");
    }

    #[test]
    fn copy_sql_uses_validated_column_names() {
        let columns = (users::id, users::email).into_meta();

        validate_copy_columns(users::table, &columns).unwrap();
        let sql = build_copy_sql(users::table, &columns).unwrap();

        assert_eq!(
            sql,
            r#"copy "public"."users" ("id", "email") from stdin binary"#
        );
    }

    #[test]
    fn copy_columns_must_belong_to_target_table() {
        let columns = (posts::id,).into_meta();
        let result = validate_copy_columns(users::table, &columns);

        assert!(matches!(
            result,
            Err(Error::InvalidQueryShape(message))
                if message.contains("COPY column id")
                    && message.contains("target table users")
        ));
    }

    #[test]
    fn copy_json_columns_use_field_pg_type_metadata() {
        assert_eq!(users::json_profile.meta().pg_type(), Type::JSON);
        assert_eq!(users::jsonb_profile.meta().pg_type(), Type::JSONB);
        assert_eq!(users::untyped_profile.meta().pg_type(), Type::JSONB);
    }

    #[test]
    fn copy_array_columns_use_field_pg_type_metadata() {
        assert_eq!(users::text_tags.meta().pg_type(), Type::TEXT_ARRAY);
        assert_eq!(users::varchar_tags.meta().pg_type(), Type::VARCHAR_ARRAY);
        assert_eq!(users::bpchar_tags.meta().pg_type(), Type::BPCHAR_ARRAY);
        assert_eq!(users::json_items.meta().pg_type(), Type::JSON_ARRAY);
        assert_eq!(users::jsonb_items.meta().pg_type(), Type::JSONB_ARRAY);
        assert_eq!(
            users::untyped_json_items.meta().pg_type(),
            Type::JSONB_ARRAY
        );
    }
}
