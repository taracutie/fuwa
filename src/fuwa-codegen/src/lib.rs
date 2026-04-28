//! PostgreSQL introspection and Rust schema generation for `fuwa-codegen`.

use std::collections::{BTreeMap, BTreeSet};

use fuwa_core::{Error, Result};
use tokio_postgres::GenericClient;

/// PostgreSQL startup options used by `fuwa-codegen` connections.
pub const READ_ONLY_STARTUP_OPTIONS: &str = "-c default_transaction_read_only=on";

/// Introspected database schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseSchema {
    pub tables: Vec<TableDef>,
}

/// Introspected table definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

/// Introspected column definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub ordinal_position: i16,
    pub pg_type: String,
    pub pg_type_kind: String,
    pub rust_type: RustType,
    pub nullable: bool,
    pub default_expression: Option<String>,
    pub primary_key: bool,
}

/// Rust type chosen for a PostgreSQL type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RustType {
    path: String,
}

impl RustType {
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// A table selector accepted by `fuwa-codegen --table`.
///
/// Unqualified names match the selected schemas. Qualified names use
/// `schema.table` and cause that schema to be introspected even if it was not
/// separately listed with `--schema`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableFilter {
    schema: Option<String>,
    name: String,
}

impl TableFilter {
    pub fn parse(value: &str) -> Result<Self> {
        let value = value.trim();
        if value.is_empty() {
            return Err(Error::codegen("table filter cannot be empty"));
        }

        match value.split_once('.') {
            Some((schema, name)) => {
                if schema.is_empty() || name.is_empty() || name.contains('.') {
                    return Err(Error::codegen(format!(
                        "invalid table filter {value:?}; expected table or schema.table"
                    )));
                }
                Ok(Self {
                    schema: Some(schema.to_owned()),
                    name: name.to_owned(),
                })
            }
            None => Ok(Self {
                schema: None,
                name: value.to_owned(),
            }),
        }
    }

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn matches(&self, schema: &str, table: &str) -> bool {
        self.name == table
            && match self.schema.as_deref() {
                Some(filter) => filter == schema,
                None => true,
            }
    }
}

impl AsRef<TableFilter> for TableFilter {
    fn as_ref(&self) -> &TableFilter {
        self
    }
}

const INTROSPECTION_SQL: &str = r#"
select
    n.nspname as schema_name,
    c.relname as table_name,
    a.attname as column_name,
    a.attnum as ordinal_position,
    not a.attnotnull as nullable,
    t.typname as pg_type,
    t.typtype::text as pg_type_kind,
    pg_get_expr(ad.adbin, ad.adrelid) as default_expression,
    exists (
        select 1
        from pg_index i
        where i.indrelid = c.oid
          and i.indisprimary
          and a.attnum = any(i.indkey)
    ) as primary_key
from pg_attribute a
join pg_class c on c.oid = a.attrelid
join pg_namespace n on n.oid = c.relnamespace
join pg_type t on t.oid = a.atttypid
left join pg_attrdef ad on ad.adrelid = a.attrelid and ad.adnum = a.attnum
where n.nspname = $1
  and c.relkind in ('r', 'p')
  and a.attnum > 0
  and not a.attisdropped
order by n.nspname, c.relname, a.attnum
"#;

/// Build the PostgreSQL connection config used by schema generation.
///
/// The resulting connection starts with `default_transaction_read_only=on`, so implicit
/// transactions are read-only before any codegen query is sent.
pub fn read_only_connection_config(database_url: &str) -> Result<tokio_postgres::Config> {
    let mut config = database_url
        .parse::<tokio_postgres::Config>()
        .map_err(|err| Error::codegen(format!("invalid database URL: {err}")))?;
    config.options(READ_ONLY_STARTUP_OPTIONS);
    config.application_name("fuwa-codegen");
    Ok(config)
}

/// Verify that the connected session has the read-only transaction default enabled.
pub async fn ensure_read_only_connection(client: &impl GenericClient) -> Result<()> {
    let row = client
        .query_one(
            "select current_setting('default_transaction_read_only')",
            &[],
        )
        .await
        .map_err(|err| Error::codegen(err.to_string()))?;
    let setting: String = row.get(0);

    if setting == "on" {
        Ok(())
    } else {
        Err(Error::codegen(format!(
            "schema generation connection is not read-only: default_transaction_read_only={setting}"
        )))
    }
}

/// Introspect a PostgreSQL schema using `pg_catalog`.
pub async fn introspect_schema(
    client: &impl GenericClient,
    schema_name: &str,
) -> Result<DatabaseSchema> {
    introspect_schemas(client, [schema_name], std::iter::empty::<TableFilter>()).await
}

/// Introspect PostgreSQL schemas using `pg_catalog`, optionally limiting output
/// to selected tables.
pub async fn introspect_schemas<S, T>(
    client: &impl GenericClient,
    schema_names: S,
    table_filters: T,
) -> Result<DatabaseSchema>
where
    S: IntoIterator,
    S::Item: AsRef<str>,
    T: IntoIterator,
    T::Item: AsRef<TableFilter>,
{
    let table_filters: Vec<TableFilter> = table_filters
        .into_iter()
        .map(|filter| filter.as_ref().clone())
        .collect();
    let mut schema_names: BTreeSet<String> = schema_names
        .into_iter()
        .map(|schema| schema.as_ref().to_owned())
        .collect();
    schema_names.extend(
        table_filters
            .iter()
            .filter_map(|filter| filter.schema().map(ToOwned::to_owned)),
    );

    if schema_names.is_empty() {
        return Err(Error::codegen(
            "schema introspection requires at least one schema",
        ));
    }

    let mut tables: BTreeMap<(String, String), Vec<ColumnDef>> = BTreeMap::new();

    for schema_name in schema_names {
        let rows = client
            .query(INTROSPECTION_SQL, &[&schema_name])
            .await
            .map_err(|err| Error::codegen(err.to_string()))?;

        for row in rows {
            let schema: String = row.get("schema_name");
            let table: String = row.get("table_name");

            if !table_filters.is_empty()
                && !table_filters
                    .iter()
                    .any(|filter| filter.matches(&schema, &table))
            {
                continue;
            }

            let column: String = row.get("column_name");
            let ordinal_position: i16 = row.get("ordinal_position");
            let nullable: bool = row.get("nullable");
            let pg_type: String = row.get("pg_type");
            let pg_type_kind: String = row.get("pg_type_kind");
            let default_expression: Option<String> = row.get("default_expression");
            let primary_key: bool = row.get("primary_key");
            let rust_type = map_pg_type_with_kind(&pg_type, &pg_type_kind)?;

            tables.entry((schema, table)).or_default().push(ColumnDef {
                name: column,
                ordinal_position,
                pg_type,
                pg_type_kind,
                rust_type,
                nullable,
                default_expression,
                primary_key,
            });
        }
    }

    Ok(DatabaseSchema {
        tables: tables
            .into_iter()
            .map(|((schema, name), mut columns)| {
                columns.sort_by_key(|column| column.ordinal_position);
                TableDef {
                    schema,
                    name,
                    columns,
                }
            })
            .collect(),
    })
}

/// Introspect a schema inside an explicit read-only transaction.
pub async fn introspect_schema_read_only(
    client: &impl GenericClient,
    schema_name: &str,
) -> Result<DatabaseSchema> {
    introspect_schemas_read_only(client, [schema_name], std::iter::empty::<TableFilter>()).await
}

/// Introspect schemas inside an explicit read-only transaction.
pub async fn introspect_schemas_read_only<S, T>(
    client: &impl GenericClient,
    schema_names: S,
    table_filters: T,
) -> Result<DatabaseSchema>
where
    S: IntoIterator,
    S::Item: AsRef<str>,
    T: IntoIterator,
    T::Item: AsRef<TableFilter>,
{
    client
        .batch_execute("begin read only")
        .await
        .map_err(|err| Error::codegen(err.to_string()))?;

    let result = introspect_schemas(client, schema_names, table_filters).await;
    let rollback = client.batch_execute("rollback").await;

    match (result, rollback) {
        (Ok(schema), Ok(())) => Ok(schema),
        (Err(err), _) => Err(err),
        (Ok(_), Err(err)) => Err(Error::codegen(format!(
            "failed to close read-only introspection transaction: {err}"
        ))),
    }
}

/// Map a PostgreSQL scalar type name to a Rust type path.
pub fn map_pg_type(pg_type: &str) -> Result<RustType> {
    map_pg_type_with_kind(pg_type, "b")
}

/// Map a PostgreSQL type name and type kind to a Rust type path.
pub fn map_pg_type_with_kind(pg_type: &str, pg_type_kind: &str) -> Result<RustType> {
    if pg_type_kind == "e" {
        return Ok(RustType {
            path: "String".to_owned(),
        });
    }

    let path = match pg_type {
        "int2" => "i16",
        "int4" => "i32",
        "int8" => "i64",
        "float4" => "f32",
        "float8" => "f64",
        "numeric" => "fuwa::types::Decimal",
        "bool" => "bool",
        "bytea" => "Vec<u8>",
        "text" | "varchar" | "bpchar" => "String",
        "uuid" => "fuwa::types::Uuid",
        "timestamp" => "fuwa::types::NaiveDateTime",
        "timestamptz" => "fuwa::types::DateTime<fuwa::types::Utc>",
        "date" => "fuwa::types::NaiveDate",
        "json" | "jsonb" => "fuwa::types::Value",
        "_int2" => "Vec<i16>",
        "_int4" => "Vec<i32>",
        "_int8" => "Vec<i64>",
        "_float4" => "Vec<f32>",
        "_float8" => "Vec<f64>",
        "_numeric" => "Vec<fuwa::types::Decimal>",
        "_bool" => "Vec<bool>",
        "_text" | "_varchar" | "_bpchar" => "Vec<String>",
        "_uuid" => "Vec<fuwa::types::Uuid>",
        "_timestamp" => "Vec<fuwa::types::NaiveDateTime>",
        "_timestamptz" => "Vec<fuwa::types::DateTime<fuwa::types::Utc>>",
        "_date" => "Vec<fuwa::types::NaiveDate>",
        "_json" | "_jsonb" => "Vec<fuwa::types::Value>",
        other => return Err(Error::unsupported_postgres_type(other)),
    };

    Ok(RustType {
        path: path.to_owned(),
    })
}

/// Generate a Rust schema module.
pub fn generate_schema_module(schema: &DatabaseSchema) -> Result<String> {
    let mut source = String::new();
    source.push_str("#![allow(non_upper_case_globals)]\n");

    for table in &schema.tables {
        render_table_module(&mut source, table);
    }

    let parsed = syn::parse_file(&source).map_err(|err| Error::codegen(err.to_string()))?;
    let formatted = prettyplease::unparse(&parsed);
    Ok(insert_generated_comment(formatted))
}

fn insert_generated_comment(source: String) -> String {
    const GENERATED: &str = "// @generated by fuwa-codegen. Do not edit by hand.\n";

    if let Some(rest) = source.strip_prefix("#![allow(non_upper_case_globals)]\n") {
        format!("#![allow(non_upper_case_globals)]\n{GENERATED}{rest}")
    } else {
        format!("{GENERATED}{source}")
    }
}

fn render_table_module(source: &mut String, table: &TableDef) {
    let module_name = rust_ident(&table.name);
    source.push_str("pub mod ");
    source.push_str(&module_name);
    source.push_str(" {\n");
    source.push_str("use fuwa::prelude::*;\n\n");
    source.push_str("pub const table: Table = Table::new(");
    source.push_str(&rust_string(&table.schema));
    source.push_str(", ");
    source.push_str(&rust_string(&table.name));
    source.push_str(");\n\n");

    for column in &table.columns {
        let field_name = rust_ident(&column.name);
        source.push_str("pub const ");
        source.push_str(&field_name);
        source.push_str(": Field<");
        source.push_str(column.rust_type.path());
        source.push_str(", ");
        source.push_str(if column.nullable {
            "Nullable"
        } else {
            "NotNull"
        });
        source.push_str("> = Field::new(table, ");
        source.push_str(&rust_string(&column.name));
        source.push_str(");\n");
    }

    source.push_str("\n#[derive(Debug, Clone)]\n");
    source.push_str("pub struct Record {\n");
    for column in &table.columns {
        source.push_str("pub ");
        source.push_str(&rust_ident(&column.name));
        source.push_str(": ");
        source.push_str(&record_field_type(column));
        source.push_str(",\n");
    }
    source.push_str("}\n\n");

    render_record_decoder(source, table);
    render_all_function(source, table);
    source.push_str("}\n\n");
}

fn render_record_decoder(source: &mut String, table: &TableDef) {
    source.push_str("impl fuwa::FromRow for Record {\n");
    source.push_str("fn from_row(pg_row: &fuwa::postgres::Row) -> fuwa::Result<Self> {\n");
    source.push_str("if pg_row.len() != ");
    source.push_str(&table.columns.len().to_string());
    source.push_str(" { return Err(fuwa::Error::row_decode(format!(\"expected ");
    source.push_str(&table.columns.len().to_string());
    source.push_str(" columns, got {}\", pg_row.len()))); }\n");
    source.push_str("Ok(Self {\n");
    for (index, column) in table.columns.iter().enumerate() {
        source.push_str(&rust_ident(&column.name));
        source.push_str(": pg_row.try_get(");
        source.push_str(&index.to_string());
        source
            .push_str(").map_err(|err| fuwa::Error::row_decode(format!(\"failed to decode column ");
        source.push_str(&index.to_string());
        source.push_str(": {}\", err)))?,\n");
    }
    source.push_str("})\n");
    source.push_str("}\n");
    source.push_str("}\n\n");
}

fn render_all_function(source: &mut String, table: &TableDef) {
    source.push_str("pub struct All;\n\n");
    source.push_str("impl Selectable for All {\n");
    source.push_str("type Record = Record;\n\n");
    source.push_str("fn into_select_items(self) -> Vec<fuwa::SelectItem> {\n");
    source.push_str("let mut items = Vec::new();\n");
    for column in &table.columns {
        source.push_str("items.extend(");
        source.push_str(&rust_ident(&column.name));
        source.push_str(".into_select_items());\n");
    }
    source.push_str("items\n");
    source.push_str("}\n");
    source.push_str("}\n\n");
    source.push_str("pub fn all() -> All {\n");
    source.push_str("All\n");
    source.push_str("}\n");
}

fn record_field_type(column: &ColumnDef) -> String {
    if column.nullable {
        format!("Option<{}>", column.rust_type.path())
    } else {
        column.rust_type.path().to_owned()
    }
}

fn rust_string(value: &str) -> String {
    format!("{value:?}")
}

fn rust_ident(value: &str) -> String {
    let mut ident = String::new();
    let mut previous_was_underscore = false;
    let mut previous_was_lower_or_digit = false;
    let chars: Vec<char> = value.chars().collect();

    for (index, ch) in chars.iter().copied().enumerate() {
        if ch.is_ascii_alphanumeric() {
            let next_is_lower = chars
                .get(index + 1)
                .is_some_and(|next| next.is_ascii_lowercase());
            let should_split_upper = ch.is_ascii_uppercase()
                && !ident.is_empty()
                && !previous_was_underscore
                && (previous_was_lower_or_digit || next_is_lower);
            if should_split_upper {
                ident.push('_');
            }
            ident.push(ch.to_ascii_lowercase());
            previous_was_underscore = false;
            previous_was_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        } else if !previous_was_underscore {
            ident.push('_');
            previous_was_underscore = true;
            previous_was_lower_or_digit = false;
        }
    }

    let ident = ident.trim_matches('_');
    let mut ident = if ident.is_empty() {
        "unnamed".to_owned()
    } else {
        ident.to_owned()
    };

    if ident.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        ident.insert(0, '_');
    }

    if is_rust_keyword(&ident) {
        format!("r#{ident}")
    } else {
        ident
    }
}

fn is_rust_keyword(value: &str) -> bool {
    matches!(
        value,
        "as" | "break"
            | "const"
            | "continue"
            | "crate"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "fn"
            | "for"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "Self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "type"
            | "unsafe"
            | "use"
            | "where"
            | "while"
            | "async"
            | "await"
            | "dyn"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_common_types() {
        assert_eq!(map_pg_type("int8").unwrap().path(), "i64");
        assert_eq!(map_pg_type("varchar").unwrap().path(), "String");
        assert_eq!(map_pg_type("bytea").unwrap().path(), "Vec<u8>");
        assert_eq!(
            map_pg_type("numeric").unwrap().path(),
            "fuwa::types::Decimal"
        );
        assert_eq!(
            map_pg_type("_numeric").unwrap().path(),
            "Vec<fuwa::types::Decimal>"
        );
        assert_eq!(map_pg_type("jsonb").unwrap().path(), "fuwa::types::Value");
        assert_eq!(map_pg_type("_int4").unwrap().path(), "Vec<i32>");
        assert_eq!(map_pg_type("_text").unwrap().path(), "Vec<String>");
        assert_eq!(
            map_pg_type_with_kind("UserImagePreferenceType", "e")
                .unwrap()
                .path(),
            "String"
        );
        assert!(map_pg_type("inet").is_err());
    }

    #[test]
    fn parses_table_filters() {
        let unqualified = TableFilter::parse("users").unwrap();
        assert_eq!(unqualified.schema(), None);
        assert_eq!(unqualified.name(), "users");
        assert!(unqualified.matches("public", "users"));

        let qualified = TableFilter::parse("admin.users").unwrap();
        assert_eq!(qualified.schema(), Some("admin"));
        assert_eq!(qualified.name(), "users");
        assert!(qualified.matches("admin", "users"));
        assert!(!qualified.matches("public", "users"));

        assert!(TableFilter::parse("").is_err());
        assert!(TableFilter::parse("admin.").is_err());
        assert!(TableFilter::parse("too.many.parts").is_err());
    }

    #[test]
    fn generates_schema_module() {
        let schema = DatabaseSchema {
            tables: vec![TableDef {
                schema: "public".to_owned(),
                name: "RecentImagePair".to_owned(),
                columns: vec![
                    ColumnDef {
                        name: "id".to_owned(),
                        ordinal_position: 1,
                        pg_type: "int8".to_owned(),
                        pg_type_kind: "b".to_owned(),
                        rust_type: map_pg_type("int8").unwrap(),
                        nullable: false,
                        default_expression: None,
                        primary_key: true,
                    },
                    ColumnDef {
                        name: "userId".to_owned(),
                        ordinal_position: 2,
                        pg_type: "text".to_owned(),
                        pg_type_kind: "b".to_owned(),
                        rust_type: map_pg_type("text").unwrap(),
                        nullable: true,
                        default_expression: None,
                        primary_key: false,
                    },
                    ColumnDef {
                        name: "recentImages".to_owned(),
                        ordinal_position: 3,
                        pg_type: "_text".to_owned(),
                        pg_type_kind: "b".to_owned(),
                        rust_type: map_pg_type("_text").unwrap(),
                        nullable: false,
                        default_expression: None,
                        primary_key: false,
                    },
                    ColumnDef {
                        name: "score".to_owned(),
                        ordinal_position: 4,
                        pg_type: "numeric".to_owned(),
                        pg_type_kind: "b".to_owned(),
                        rust_type: map_pg_type("numeric").unwrap(),
                        nullable: false,
                        default_expression: None,
                        primary_key: false,
                    },
                    ColumnDef {
                        name: "adjustments".to_owned(),
                        ordinal_position: 5,
                        pg_type: "_numeric".to_owned(),
                        pg_type_kind: "b".to_owned(),
                        rust_type: map_pg_type("_numeric").unwrap(),
                        nullable: true,
                        default_expression: None,
                        primary_key: false,
                    },
                ],
            }],
        };

        let generated = generate_schema_module(&schema).unwrap();
        assert!(generated.contains("// @generated by fuwa-codegen"));
        assert!(generated.contains("pub mod recent_image_pair"));
        assert!(generated.contains("pub const id: Field<i64, NotNull>"));
        assert!(generated.contains("pub const user_id: Field<String, Nullable>"));
        assert!(generated.contains("pub const recent_images: Field<Vec<String>, NotNull>"));
        assert!(generated.contains("pub const score: Field<fuwa::types::Decimal, NotNull>"));
        assert!(
            generated.contains("pub const adjustments: Field<Vec<fuwa::types::Decimal>, Nullable>")
        );
        assert!(generated.contains("pub user_id: Option<String>"));
        assert!(generated.contains("pub recent_images: Vec<String>"));
        assert!(generated.contains("pub score: fuwa::types::Decimal"));
        assert!(generated.contains("pub adjustments: Option<Vec<fuwa::types::Decimal>>"));
        assert!(generated.contains("impl fuwa::FromRow for Record"));
        assert!(generated.contains("pub struct All"));
        assert!(generated.contains("items.extend(id.into_select_items())"));
    }
}
