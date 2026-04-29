use std::path::PathBuf;

use clap::{Parser, Subcommand};
use fuwa_codegen::{generate, CodegenSource, GenerateOptions, SchemaSnapshot, TableFilter};
use fuwa_core::{Error, Result};

#[derive(Debug, Parser)]
#[command(
    name = "fuwa-codegen",
    version,
    about = "Generate fuwa schema modules from PostgreSQL, snapshots, or Prisma schemas"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// PostgreSQL connection string. Falls back to DATABASE_URL when no snapshot or Prisma source is selected.
    #[arg(long)]
    database_url: Option<String>,

    /// Fuwa schema snapshot JSON.
    #[arg(long, value_name = "fuwa.schema.json")]
    snapshot: Option<PathBuf>,

    /// Prisma schema file.
    #[arg(long, value_name = "schema.prisma")]
    prisma: Option<PathBuf>,

    /// PostgreSQL schema to generate. May be repeated or comma-separated.
    ///
    /// Database sources default to `public`; offline sources include all schemas unless this is set.
    #[arg(long, value_name = "SCHEMA", value_delimiter = ',')]
    schema: Vec<String>,

    /// Table to generate. Accepts `table` or `schema.table`; may be repeated or comma-separated.
    #[arg(long, value_name = "TABLE", value_delimiter = ',')]
    table: Vec<String>,

    /// Output Rust file.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Write a fuwa schema snapshot JSON file from a live PostgreSQL database.
    Snapshot(SnapshotCli),
}

#[derive(Debug, Parser)]
struct SnapshotCli {
    /// PostgreSQL connection string.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// PostgreSQL schema to introspect. May be repeated or comma-separated.
    #[arg(
        long,
        value_name = "SCHEMA",
        value_delimiter = ',',
        default_value = "public"
    )]
    schema: Vec<String>,

    /// Table to snapshot. Accepts `table` or `schema.table`; may be repeated or comma-separated.
    #[arg(long, value_name = "TABLE", value_delimiter = ',')]
    table: Vec<String>,

    /// Output snapshot JSON file.
    #[arg(long)]
    out: PathBuf,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Snapshot(snapshot)) => write_snapshot(snapshot),
        None => write_generated(cli),
    }
}

fn write_generated(cli: Cli) -> Result<()> {
    let out = cli
        .out
        .ok_or_else(|| Error::codegen("fuwa-codegen requires --out for generation"))?;
    let source = generation_source(
        cli.database_url,
        cli.snapshot,
        cli.prisma,
        cli.schema,
        cli.table,
    )?;
    let generated = generate(GenerateOptions::new(source))?;
    std::fs::write(out, generated)?;
    Ok(())
}

fn write_snapshot(cli: SnapshotCli) -> Result<()> {
    let table_filters = parse_table_filters(&cli.table)?;
    let snapshot =
        SchemaSnapshot::from_database_with_options(&cli.database_url, &cli.schema, &table_filters)?;
    snapshot.write_to(cli.out)
}

fn generation_source(
    database_url: Option<String>,
    snapshot: Option<PathBuf>,
    prisma: Option<PathBuf>,
    schemas: Vec<String>,
    tables: Vec<String>,
) -> Result<CodegenSource> {
    let database_url = database_url.or_else(|| {
        if snapshot.is_none() && prisma.is_none() {
            std::env::var("DATABASE_URL").ok()
        } else {
            None
        }
    });
    let selected =
        database_url.is_some() as usize + snapshot.is_some() as usize + prisma.is_some() as usize;
    if selected != 1 {
        return Err(Error::codegen(
            "choose exactly one schema source: --database-url, --snapshot, or --prisma",
        ));
    }

    let table_filters = parse_table_filters(&tables)?;
    if let Some(database_url) = database_url {
        let schemas = if schemas.is_empty() {
            vec!["public".to_owned()]
        } else {
            schemas
        };
        return Ok(CodegenSource::Database {
            database_url,
            schemas,
            tables: table_filters,
        });
    }
    if let Some(snapshot) = snapshot {
        let schema = filter_offline_schema(
            SchemaSnapshot::from_snapshot(snapshot)?.schema,
            &schemas,
            &table_filters,
        );
        return Ok(CodegenSource::Schema(schema));
    }
    if let Some(prisma) = prisma {
        let default_schema = single_schema_filter(&schemas);
        let schema = filter_offline_schema(
            SchemaSnapshot::from_prisma_with_default_schema(prisma, default_schema)?.schema,
            &schemas,
            &table_filters,
        );
        return Ok(CodegenSource::Schema(schema));
    }
    unreachable!("exactly one source was validated")
}

fn filter_offline_schema(
    schema: fuwa_codegen::DatabaseSchema,
    schemas: &[String],
    table_filters: &[TableFilter],
) -> fuwa_codegen::DatabaseSchema {
    if schemas.is_empty() && table_filters.is_empty() {
        return schema;
    }

    if schemas.is_empty() {
        let schema_names: Vec<_> = schema
            .tables
            .iter()
            .map(|table| table.schema.clone())
            .collect();
        return schema.filter_tables(schema_names, table_filters);
    }

    schema.filter_tables(schemas, table_filters)
}

fn single_schema_filter(schemas: &[String]) -> Option<&str> {
    match schemas {
        [schema] if !schema.is_empty() => Some(schema.as_str()),
        _ => None,
    }
}

fn parse_table_filters(tables: &[String]) -> Result<Vec<TableFilter>> {
    tables
        .iter()
        .map(|table| TableFilter::parse(table))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fuwa_codegen::{ColumnDef, DatabaseSchema, RustType, TableDef};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn generation_cli_leaves_schema_empty_when_omitted() {
        let cli = Cli::parse_from([
            "fuwa-codegen",
            "--snapshot",
            "fuwa.schema.json",
            "--out",
            "schema.rs",
        ]);

        assert!(cli.schema.is_empty());
    }

    #[test]
    fn database_source_defaults_to_public_when_schema_is_omitted() -> TestResult {
        let source = generation_source(
            Some("postgres://example".to_owned()),
            None,
            None,
            Vec::new(),
            Vec::new(),
        )?;

        let CodegenSource::Database { schemas, .. } = source else {
            panic!("database URL should select database source");
        };
        assert_eq!(schemas, vec!["public"]);
        Ok(())
    }

    #[test]
    fn snapshot_source_without_schema_filter_keeps_all_schemas() -> TestResult {
        let dir = temp_dir("snapshot-all-schemas");
        fs::create_dir_all(&dir)?;
        let snapshot_path = dir.join("fuwa.schema.json");
        let snapshot = SchemaSnapshot::new(DatabaseSchema {
            tables: vec![table("public", "User"), table("auth", "Session")],
            enums: Vec::new(),
        });
        snapshot.write_to(&snapshot_path)?;

        let source = generation_source(None, Some(snapshot_path), None, Vec::new(), Vec::new())?;

        let CodegenSource::Schema(schema) = source else {
            panic!("offline snapshot source should be pre-filtered");
        };
        let tables: Vec<_> = schema
            .tables
            .iter()
            .map(|table| (table.schema.as_str(), table.name.as_str()))
            .collect();
        assert_eq!(tables, vec![("public", "User"), ("auth", "Session")]);
        Ok(())
    }

    #[test]
    fn prisma_source_without_schema_filter_keeps_all_schemas() -> TestResult {
        let dir = temp_dir("prisma-all-schemas");
        fs::create_dir_all(&dir)?;
        let prisma_path = dir.join("schema.prisma");
        fs::write(
            &prisma_path,
            r#"
            datasource db {
              provider = "postgresql"
              schemas = ["public", "auth"]
            }

            enum Role {
              USER
              ADMIN

              @@schema("auth")
            }

            model User {
              id   String @id
              role Role?

              @@schema("auth")
            }

            model Post {
              id String @id
            }
            "#,
        )?;

        let source = generation_source(None, None, Some(prisma_path), Vec::new(), Vec::new())?;

        let CodegenSource::Schema(schema) = source else {
            panic!("offline Prisma source should be pre-filtered");
        };
        let tables: Vec<_> = schema
            .tables
            .iter()
            .map(|table| (table.schema.as_str(), table.name.as_str()))
            .collect();
        let enums: Vec<_> = schema
            .enums
            .iter()
            .map(|enum_def| (enum_def.schema.as_str(), enum_def.rust_name.as_str()))
            .collect();
        assert_eq!(tables, vec![("auth", "User"), ("public", "Post")]);
        assert_eq!(enums, vec![("auth", "Role")]);
        Ok(())
    }

    #[test]
    fn prisma_source_uses_single_schema_filter_as_default_schema() -> TestResult {
        let dir = temp_dir("prisma-default-schema");
        fs::create_dir_all(&dir)?;
        let prisma_path = dir.join("schema.prisma");
        fs::write(
            &prisma_path,
            r#"
            datasource db {
              provider = "postgresql"
            }

            model User {
              id String @id
            }
            "#,
        )?;

        let source = generation_source(
            None,
            None,
            Some(prisma_path),
            vec!["app".to_owned()],
            Vec::new(),
        )?;

        let CodegenSource::Schema(schema) = source else {
            panic!("offline Prisma source should be pre-filtered");
        };
        let tables: Vec<_> = schema
            .tables
            .iter()
            .map(|table| (table.schema.as_str(), table.name.as_str()))
            .collect();
        assert_eq!(tables, vec![("app", "User")]);
        Ok(())
    }

    #[test]
    fn snapshot_source_applies_table_filters() -> TestResult {
        let dir = temp_dir("snapshot-filter");
        fs::create_dir_all(&dir)?;
        let snapshot_path = dir.join("fuwa.schema.json");
        let snapshot = SchemaSnapshot::new(DatabaseSchema {
            tables: vec![table("public", "User"), table("public", "Post")],
            enums: Vec::new(),
        });
        snapshot.write_to(&snapshot_path)?;

        let source = generation_source(
            None,
            Some(snapshot_path),
            None,
            vec!["public".to_owned()],
            vec!["User".to_owned()],
        )?;

        let CodegenSource::Schema(schema) = source else {
            panic!("offline snapshot source should be pre-filtered");
        };
        let tables: Vec<_> = schema
            .tables
            .iter()
            .map(|table| (table.schema.as_str(), table.name.as_str()))
            .collect();
        assert_eq!(tables, vec![("public", "User")]);
        Ok(())
    }

    #[test]
    fn prisma_source_applies_qualified_table_filters_and_prunes_enums() -> TestResult {
        let dir = temp_dir("prisma-filter");
        fs::create_dir_all(&dir)?;
        let prisma_path = dir.join("schema.prisma");
        fs::write(
            &prisma_path,
            r#"
            datasource db {
              provider = "postgresql"
              schemas = ["public", "auth"]
            }

            enum Role {
              USER
              ADMIN

              @@schema("auth")
            }

            model User {
              id   String @id
              role Role?

              @@schema("auth")
            }

            model Post {
              id String @id
            }
            "#,
        )?;

        let source = generation_source(
            None,
            None,
            Some(prisma_path),
            vec!["public".to_owned()],
            vec!["auth.User".to_owned()],
        )?;

        let CodegenSource::Schema(schema) = source else {
            panic!("offline Prisma source should be pre-filtered");
        };
        let tables: Vec<_> = schema
            .tables
            .iter()
            .map(|table| (table.schema.as_str(), table.name.as_str()))
            .collect();
        let enums: Vec<_> = schema
            .enums
            .iter()
            .map(|enum_def| enum_def.rust_name.as_str())
            .collect();
        assert_eq!(tables, vec![("auth", "User")]);
        assert_eq!(enums, vec!["Role"]);
        Ok(())
    }

    fn table(schema: &str, name: &str) -> TableDef {
        TableDef {
            schema: schema.to_owned(),
            name: name.to_owned(),
            columns: vec![ColumnDef {
                name: "id".to_owned(),
                ordinal_position: 1,
                pg_type: "text".to_owned(),
                pg_type_kind: "b".to_owned(),
                rust_type: RustType::new("String"),
                nullable: false,
                default_expression: None,
                primary_key: true,
                unique: true,
                relation: None,
            }],
            primary_key: vec!["id".to_owned()],
            uniques: vec![vec!["id".to_owned()]],
        }
    }

    fn temp_dir(kind: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "fuwa-codegen-main-{kind}-{}-{nonce}",
            std::process::id()
        ))
    }
}
