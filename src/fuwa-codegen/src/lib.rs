//! PostgreSQL introspection and Rust schema generation for `fuwa-codegen`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use fuwa_core::{Error, Result};
use serde::{Deserialize, Serialize};
use tokio_postgres::GenericClient;
use tokio_postgres::NoTls;

mod prisma;

/// PostgreSQL startup options used by `fuwa-codegen` connections.
pub const READ_ONLY_STARTUP_OPTIONS: &str = "-c default_transaction_read_only=on";

/// Introspected database schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseSchema {
    pub tables: Vec<TableDef>,
    #[serde(default)]
    pub enums: Vec<EnumDef>,
}

impl DatabaseSchema {
    /// Return this schema narrowed to the selected PostgreSQL schemas and tables.
    pub fn filter_tables<S, T>(mut self, schema_names: S, table_filters: T) -> Self
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

        self.tables.retain(|table| {
            schema_names.contains(&table.schema)
                && (table_filters.is_empty()
                    || table_filters
                        .iter()
                        .any(|filter| filter.matches(&table.schema, &table.name)))
        });

        let referenced_enums: BTreeSet<String> = self
            .tables
            .iter()
            .flat_map(|table| &table.columns)
            .filter(|column| column.rust_type.is_enum())
            .map(|column| column.rust_type.path().to_owned())
            .collect();
        self.enums
            .retain(|enum_def| referenced_enums.contains(&enum_def.rust_name));

        self
    }
}

/// Introspected table definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDef {
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub uniques: Vec<Vec<String>>,
}

/// Introspected column definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub ordinal_position: i16,
    pub pg_type: String,
    pub pg_type_kind: String,
    pub rust_type: RustType,
    pub nullable: bool,
    pub default_expression: Option<String>,
    pub primary_key: bool,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub relation: Option<RelationDef>,
}

/// Introspected or schema-declared foreign-key relation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationDef {
    pub model: String,
    pub fields: Vec<String>,
    pub references: Vec<String>,
}

/// Introspected or schema-declared enum definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnumDef {
    pub schema: String,
    pub name: String,
    pub rust_name: String,
    pub variants: Vec<EnumVariantDef>,
}

/// Enum variant mapping from Rust variant to database value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnumVariantDef {
    pub rust_name: String,
    pub db_name: String,
}

/// Rust type chosen for a PostgreSQL type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RustType {
    path: String,
    #[serde(default)]
    enum_type: bool,
}

impl RustType {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            enum_type: false,
        }
    }

    pub fn enum_type(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            enum_type: true,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn is_enum(&self) -> bool {
        self.enum_type
    }
}

/// Serializable schema snapshot consumed by offline code generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    pub version: u32,
    pub schema: DatabaseSchema,
}

impl SchemaSnapshot {
    pub const VERSION: u32 = 1;

    pub fn new(schema: DatabaseSchema) -> Self {
        Self {
            version: Self::VERSION,
            schema,
        }
    }

    pub fn from_database(database_url: &str) -> Result<Self> {
        Self::from_database_with_options(
            database_url,
            ["public"],
            std::iter::empty::<TableFilter>(),
        )
    }

    pub fn from_database_with_options<S, T>(
        database_url: &str,
        schema_names: S,
        table_filters: T,
    ) -> Result<Self>
    where
        S: IntoIterator,
        S::Item: AsRef<str>,
        T: IntoIterator,
        T::Item: AsRef<TableFilter>,
    {
        let schema_names: Vec<String> = schema_names
            .into_iter()
            .map(|schema| schema.as_ref().to_owned())
            .collect();
        let table_filters: Vec<TableFilter> = table_filters
            .into_iter()
            .map(|filter| filter.as_ref().clone())
            .collect();
        let database_url = database_url.to_owned();
        block_on_codegen(async move {
            let schema =
                introspect_database_url_read_only(&database_url, &schema_names, &table_filters)
                    .await?;
            Ok(Self::new(schema))
        })
    }

    pub fn from_snapshot(path: impl AsRef<Path>) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let snapshot: Self = serde_json::from_str(&text)
            .map_err(|err| Error::codegen(format!("invalid fuwa schema snapshot: {err}")))?;
        if snapshot.version != Self::VERSION {
            return Err(Error::codegen(format!(
                "unsupported fuwa schema snapshot version {}; expected {}",
                snapshot.version,
                Self::VERSION
            )));
        }
        Ok(snapshot)
    }

    pub fn from_prisma(path: impl AsRef<Path>) -> Result<Self> {
        let schema = prisma::schema_from_prisma_file(path.as_ref())?;
        Ok(Self::new(schema))
    }

    pub fn from_prisma_with_default_schema(
        path: impl AsRef<Path>,
        default_schema: Option<&str>,
    ) -> Result<Self> {
        let schema =
            prisma::schema_from_prisma_file_with_default_schema(path.as_ref(), default_schema)?;
        Ok(Self::new(schema))
    }

    pub fn write_to(&self, path: impl AsRef<Path>) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|err| Error::codegen(format!("failed to serialize schema snapshot: {err}")))?;
        std::fs::write(path, format!("{json}\n"))?;
        Ok(())
    }
}

/// Options for `fuwa_codegen::generate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerateOptions {
    pub source: CodegenSource,
}

impl GenerateOptions {
    pub fn new(source: CodegenSource) -> Self {
        Self { source }
    }
}

/// Schema source accepted by `fuwa_codegen::generate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodegenSource {
    Database {
        database_url: String,
        schemas: Vec<String>,
        tables: Vec<TableFilter>,
    },
    Snapshot(PathBuf),
    Prisma(PathBuf),
    Schema(DatabaseSchema),
    SnapshotValue(SchemaSnapshot),
}

/// Generate a Rust schema module from any supported source.
pub fn generate(opts: GenerateOptions) -> Result<String> {
    let schema = match opts.source {
        CodegenSource::Database {
            database_url,
            schemas,
            tables,
        } => SchemaSnapshot::from_database_with_options(&database_url, schemas, tables)?.schema,
        CodegenSource::Snapshot(path) => SchemaSnapshot::from_snapshot(path)?.schema,
        CodegenSource::Prisma(path) => SchemaSnapshot::from_prisma(path)?.schema,
        CodegenSource::Schema(schema) => schema,
        CodegenSource::SnapshotValue(snapshot) => snapshot.schema,
    };
    generate_schema_module(&schema)
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
    ) as primary_key,
    exists (
        select 1
        from pg_index i
        where i.indrelid = c.oid
          and i.indisunique
          and not i.indisprimary
          and i.indnkeyatts = 1
          and i.indexprs is null
          and i.indpred is null
          and exists (
              select 1
              from unnest(i.indkey) with ordinality as idx_key(attnum, ordinality)
              where idx_key.ordinality <= i.indnkeyatts
                and idx_key.attnum = a.attnum
          )
    ) as unique_key
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

const UNIQUE_INDEXES_SQL: &str = r#"
select
    n.nspname as schema_name,
    c.relname as table_name,
    array_agg(a.attname::text order by idx_key.ordinality) as column_names
from pg_index i
join pg_class c on c.oid = i.indrelid
join pg_namespace n on n.oid = c.relnamespace
join unnest(i.indkey) with ordinality as idx_key(attnum, ordinality)
  on idx_key.ordinality <= i.indnkeyatts
join pg_attribute a on a.attrelid = c.oid and a.attnum = idx_key.attnum
where n.nspname = $1
  and c.relkind in ('r', 'p')
  and i.indisunique
  and not i.indisprimary
  and i.indnkeyatts > 0
  and i.indexprs is null
  and i.indpred is null
group by n.nspname, c.relname, i.indexrelid
order by n.nspname, c.relname, min(a.attnum), i.indexrelid
"#;

const PRIMARY_KEYS_SQL: &str = r#"
select
    n.nspname as schema_name,
    c.relname as table_name,
    array_agg(a.attname::text order by idx_key.ordinality) as column_names
from pg_index i
join pg_class c on c.oid = i.indrelid
join pg_namespace n on n.oid = c.relnamespace
join unnest(i.indkey) with ordinality as idx_key(attnum, ordinality)
  on idx_key.ordinality <= i.indnkeyatts
join pg_attribute a on a.attrelid = c.oid and a.attnum = idx_key.attnum
where n.nspname = $1
  and c.relkind in ('r', 'p')
  and i.indisprimary
  and i.indnkeyatts > 0
  and i.indexprs is null
  and i.indpred is null
group by n.nspname, c.relname, i.indexrelid
order by n.nspname, c.relname, i.indexrelid
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

async fn introspect_database_url_read_only(
    database_url: &str,
    schema_names: &[String],
    table_filters: &[TableFilter],
) -> Result<DatabaseSchema> {
    let config = read_only_connection_config(database_url)?;
    let (client, connection) = config
        .connect(NoTls)
        .await
        .map_err(|err| Error::codegen(err.to_string()))?;

    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("fuwa-codegen PostgreSQL connection task failed: {err}");
        }
    });

    ensure_read_only_connection(&client).await?;
    introspect_schemas_read_only(&client, schema_names, table_filters).await
}

fn block_on_codegen<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| Error::codegen(format!("failed to start codegen runtime: {err}")))?;
        runtime.block_on(future)
    })
    .join()
    .map_err(|_| Error::codegen("codegen runtime thread panicked"))?
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
    let mut primary_keys: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    let mut unique_indexes: BTreeMap<(String, String), Vec<Vec<String>>> = BTreeMap::new();

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
            let unique: bool = row.get("unique_key");
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
                unique,
                relation: None,
            });
        }

        let rows = client
            .query(PRIMARY_KEYS_SQL, &[&schema_name])
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

            let column_names: Vec<String> = row.get("column_names");
            primary_keys.insert((schema, table), column_names);
        }

        let rows = client
            .query(UNIQUE_INDEXES_SQL, &[&schema_name])
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

            let column_names: Vec<String> = row.get("column_names");
            unique_indexes
                .entry((schema, table))
                .or_default()
                .push(column_names);
        }
    }

    Ok(DatabaseSchema {
        tables: tables
            .into_iter()
            .map(|((schema, name), mut columns)| {
                columns.sort_by_key(|column| column.ordinal_position);
                let uniques = unique_indexes
                    .remove(&(schema.clone(), name.clone()))
                    .unwrap_or_default();
                let primary_key = primary_keys
                    .remove(&(schema.clone(), name.clone()))
                    .unwrap_or_default();
                TableDef {
                    schema,
                    name,
                    primary_key,
                    uniques,
                    columns,
                }
            })
            .collect(),
        enums: Vec::new(),
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
        return Ok(RustType::new("String"));
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

    Ok(RustType::new(path))
}

/// Generate a Rust schema module.
pub fn generate_schema_module(schema: &DatabaseSchema) -> Result<String> {
    let mut source = String::new();

    for enum_def in &schema.enums {
        render_enum(&mut source, enum_def);
    }

    for table in &schema.tables {
        render_table_module(&mut source, table);
    }

    let parsed = syn::parse_file(&source).map_err(|err| Error::codegen(err.to_string()))?;
    let formatted = prettyplease::unparse(&parsed);
    Ok(insert_generated_comment(formatted))
}

fn insert_generated_comment(source: String) -> String {
    const GENERATED: &str = "// @generated by fuwa-codegen. Do not edit by hand.\n";

    format!("{GENERATED}{source}")
}

fn render_enum(source: &mut String, enum_def: &EnumDef) {
    source.push_str("#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]\n");
    source.push_str("pub enum ");
    source.push_str(&enum_def.rust_name);
    source.push_str(" {\n");
    for variant in &enum_def.variants {
        source.push_str(&variant.rust_name);
        source.push_str(",\n");
    }
    source.push_str("}\n\n");

    source.push_str("impl ");
    source.push_str(&enum_def.rust_name);
    source.push_str(" {\n");
    source.push_str("pub const fn as_db_str(self) -> &'static str {\n");
    source.push_str("match self {\n");
    for variant in &enum_def.variants {
        source.push_str("Self::");
        source.push_str(&variant.rust_name);
        source.push_str(" => ");
        source.push_str(&rust_string(&variant.db_name));
        source.push_str(",\n");
    }
    source.push_str("}\n}\n");
    source.push_str("pub fn from_db(value: &str) -> fuwa::Result<Self> {\n");
    source.push_str("match value {\n");
    for variant in &enum_def.variants {
        source.push_str(&rust_string(&variant.db_name));
        source.push_str(" => Ok(Self::");
        source.push_str(&variant.rust_name);
        source.push_str("),\n");
    }
    source.push_str("other => Err(fuwa::Error::row_decode(format!(\"unknown ");
    source.push_str(&enum_def.rust_name);
    source.push_str(" enum value: {}\", other))),\n");
    source.push_str("}\n}\n");
    source.push_str("fn accepts_db_type(ty: &fuwa::postgres::types::Type) -> bool {\n");
    source.push_str("ty.schema() == ");
    source.push_str(&rust_string(&enum_def.schema));
    source.push_str(" && ty.name() == ");
    source.push_str(&rust_string(&enum_def.name));
    source.push_str(" && matches!(ty.kind(), fuwa::postgres::types::Kind::Enum(_))\n");
    source.push_str("}\n");
    source.push_str("}\n\n");

    source.push_str("impl fuwa::IntoBindValue for ");
    source.push_str(&enum_def.rust_name);
    source.push_str(" {\n");
    source.push_str("type Sql = ");
    source.push_str(&enum_def.rust_name);
    source.push_str(";\n");
    source.push_str("type Nullability = fuwa::NotNull;\n");
    source.push_str("type Stored = ");
    source.push_str(&enum_def.rust_name);
    source.push_str(";\n");
    source.push_str("fn into_stored(self) -> Self::Stored { self }\n");
    source.push_str("}\n\n");

    source.push_str("impl<'a> fuwa::postgres::types::FromSql<'a> for ");
    source.push_str(&enum_def.rust_name);
    source.push_str(" {\n");
    source.push_str("fn from_sql(ty: &fuwa::postgres::types::Type, raw: &'a [u8]) -> std::result::Result<Self, std::boxed::Box<dyn std::error::Error + Sync + Send>> {\n");
    source.push_str("let value = <&str as fuwa::postgres::types::FromSql>::from_sql(ty, raw)?;\n");
    source.push_str("Self::from_db(value).map_err(|err| -> std::boxed::Box<dyn std::error::Error + Sync + Send> { std::boxed::Box::new(err) })\n");
    source.push_str("}\n");
    source.push_str("fn accepts(ty: &fuwa::postgres::types::Type) -> bool {\n");
    source.push_str("Self::accepts_db_type(ty)\n");
    source.push_str("}\n");
    source.push_str("}\n\n");

    source.push_str("impl fuwa::postgres::types::ToSql for ");
    source.push_str(&enum_def.rust_name);
    source.push_str(" {\n");
    source.push_str("fn to_sql(&self, ty: &fuwa::postgres::types::Type, out: &mut fuwa::postgres::types::private::BytesMut) -> std::result::Result<fuwa::postgres::types::IsNull, std::boxed::Box<dyn std::error::Error + Sync + Send>> {\n");
    source.push_str("<&str as fuwa::postgres::types::ToSql>::to_sql(&self.as_db_str(), ty, out)\n");
    source.push_str("}\n");
    source.push_str("fn accepts(ty: &fuwa::postgres::types::Type) -> bool {\n");
    source.push_str("Self::accepts_db_type(ty)\n");
    source.push_str("}\n");
    source.push_str("fn to_sql_checked(&self, ty: &fuwa::postgres::types::Type, out: &mut fuwa::postgres::types::private::BytesMut) -> std::result::Result<fuwa::postgres::types::IsNull, std::boxed::Box<dyn std::error::Error + Sync + Send>> {\n");
    source.push_str("if !<Self as fuwa::postgres::types::ToSql>::accepts(ty) { return Err(std::boxed::Box::new(fuwa::postgres::types::WrongType::new::<Self>(ty.clone()))); }\n");
    source.push_str("<Self as fuwa::postgres::types::ToSql>::to_sql(self, ty, out)\n");
    source.push_str("}\n");
    source.push_str("}\n\n");

    source.push_str("impl fuwa::FromRow for ");
    source.push_str(&enum_def.rust_name);
    source.push_str(" {\n");
    source.push_str("fn from_row(pg_row: &fuwa::postgres::Row) -> fuwa::Result<Self> {\n");
    source.push_str("if pg_row.len() != 1 { return Err(fuwa::Error::row_decode(format!(\"expected 1 columns, got {}\", pg_row.len()))); }\n");
    source.push_str("pg_row.try_get(0).map_err(|err| fuwa::Error::row_decode(format!(\"failed to decode column 0: {}\", err)))\n");
    source.push_str("}\n");
    source.push_str("}\n\n");
}

fn render_table_module(source: &mut String, table: &TableDef) {
    let module_name = rust_ident(&table.name);
    source.push_str("#[allow(non_upper_case_globals)]\n");
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
        source.push_str(&table_module_type_path(&column.rust_type));
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
        source.push_str(": ");
        render_column_decoder(source, index, column);
        source.push_str(",\n");
    }
    source.push_str("})\n");
    source.push_str("}\n");
    source.push_str("}\n\n");
}

fn render_column_decoder(source: &mut String, index: usize, _column: &ColumnDef) {
    source.push_str("pg_row.try_get(");
    source.push_str(&index.to_string());
    source.push_str(").map_err(|err| fuwa::Error::row_decode(format!(\"failed to decode column ");
    source.push_str(&index.to_string());
    source.push_str(": {}\", err)))?");
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
    let type_path = table_module_type_path(&column.rust_type);
    if column.nullable {
        format!("Option<{type_path}>")
    } else {
        type_path
    }
}

fn table_module_type_path(rust_type: &RustType) -> String {
    if rust_type.is_enum() && !rust_type.path().contains("::") {
        format!("super::{}", rust_type.path())
    } else {
        rust_type.path().to_owned()
    }
}

fn rust_string(value: &str) -> String {
    format!("{value:?}")
}

pub(crate) fn rust_ident(value: &str) -> String {
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

pub(crate) fn rust_type_ident(value: &str) -> String {
    let ident = rust_ident(value);
    let raw = ident.strip_prefix("r#").unwrap_or(&ident);
    let mut out = String::new();
    for part in raw.split('_').filter(|part| !part.is_empty()) {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            out.push(first.to_ascii_uppercase());
            for ch in chars {
                out.push(ch);
            }
        }
    }
    let ident = if out.is_empty() {
        "Unnamed".to_owned()
    } else if out.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        format!("_{out}")
    } else {
        out
    };

    if is_rust_keyword(&ident) {
        format!("_{ident}")
    } else {
        ident
    }
}

pub(crate) fn rust_variant_ident(value: &str) -> String {
    rust_type_ident(value)
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
    fn sanitizes_pascal_idents_after_case_conversion() {
        assert_eq!(rust_type_ident("Self"), "_Self");
        assert_eq!(rust_variant_ident("SELF"), "_Self");
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
                primary_key: vec!["id".to_owned()],
                uniques: Vec::new(),
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
                        unique: true,
                        relation: None,
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
                        unique: false,
                        relation: None,
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
                        unique: false,
                        relation: None,
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
                        unique: false,
                        relation: None,
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
                        unique: false,
                        relation: None,
                    },
                ],
            }],
            enums: Vec::new(),
        };

        let generated = generate_schema_module(&schema).unwrap();
        assert!(!generated.contains("#![allow(non_upper_case_globals)]"));
        assert!(generated.contains("#[allow(non_upper_case_globals)]\npub mod recent_image_pair"));
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

    #[test]
    fn qualifies_enum_column_types_inside_table_modules() {
        let schema = DatabaseSchema {
            tables: vec![TableDef {
                schema: "public".to_owned(),
                name: "widgets".to_owned(),
                primary_key: vec!["id".to_owned()],
                uniques: Vec::new(),
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
                        unique: true,
                        relation: None,
                    },
                    ColumnDef {
                        name: "kind".to_owned(),
                        ordinal_position: 2,
                        pg_type: "widget_kind".to_owned(),
                        pg_type_kind: "e".to_owned(),
                        rust_type: RustType::enum_type("Table"),
                        nullable: false,
                        default_expression: None,
                        primary_key: false,
                        unique: false,
                        relation: None,
                    },
                ],
            }],
            enums: vec![EnumDef {
                schema: "public".to_owned(),
                name: "widget_kind".to_owned(),
                rust_name: "Table".to_owned(),
                variants: vec![EnumVariantDef {
                    rust_name: "_Self".to_owned(),
                    db_name: "SELF".to_owned(),
                }],
            }],
        };

        let generated = generate_schema_module(&schema).unwrap();
        assert!(!generated.contains("use super::*"));
        assert!(generated.contains("pub enum Table"));
        assert!(generated.contains("_Self => \"SELF\""));
        assert!(generated.contains("pub const kind: Field<super::Table, NotNull>"));
        assert!(generated.contains("pub kind: super::Table"));
    }
}
