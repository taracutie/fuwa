//! Helpers for `build.rs` scripts.
//!
//! ```no_run
//! // build.rs
//! fn main() {
//!     fuwa_codegen::build::run().unwrap();
//! }
//! ```
//!
//! Or with explicit options:
//!
//! ```no_run
//! // build.rs
//! fn main() {
//!     fuwa_codegen::build::Builder::new()
//!         .schemas(["public"])
//!         .snapshot_path("fuwa.schema.json")
//!         .run()
//!         .unwrap();
//! }
//! ```
//!
//! Source-selection rules:
//!
//! - If `FUWA_OFFLINE=1` is set, the snapshot is loaded (error if missing).
//! - Otherwise, `DATABASE_URL` is used. If the live database is unreachable,
//!   the snapshot is used as a fallback if it exists on disk.
//! - If neither a database URL nor a snapshot is available, generation fails
//!   with an actionable error.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use fuwa_core::{Error, Result};

use crate::{
    generate, generate_live_database_source, CodegenSource, GenerateOptions, SchemaSnapshot,
    TableFilter,
};

const DEFAULT_SNAPSHOT_FILENAME: &str = "fuwa.schema.json";
const DEFAULT_OUT_FILENAME: &str = "schema.rs";
const DEFAULT_SCHEMA: &str = "public";

/// Generate `schema.rs` into `OUT_DIR` using the default settings.
///
/// Equivalent to `Builder::new().run()`.
pub fn run() -> Result<()> {
    Builder::new().run()
}

/// Builder for `build.rs` codegen invocations.
#[derive(Debug, Clone)]
pub struct Builder {
    schemas: Vec<String>,
    tables: Vec<String>,
    snapshot_path: PathBuf,
    out_filename: String,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    pub fn new() -> Self {
        Self {
            schemas: Vec::new(),
            tables: Vec::new(),
            snapshot_path: PathBuf::from(DEFAULT_SNAPSHOT_FILENAME),
            out_filename: DEFAULT_OUT_FILENAME.to_owned(),
        }
    }

    /// Restrict generation to one or more PostgreSQL schemas.
    ///
    /// Defaults to `public` for live database sources, and to all schemas
    /// for offline snapshots.
    pub fn schemas<I, S>(mut self, schemas: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.schemas = schemas.into_iter().map(Into::into).collect();
        self
    }

    /// Restrict generation to specific tables.
    ///
    /// Each entry is parsed by `TableFilter::parse` (`name` or `schema.name`).
    pub fn tables<I, S>(mut self, tables: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tables = tables.into_iter().map(Into::into).collect();
        self
    }

    /// Path to the offline snapshot JSON file (defaults to `fuwa.schema.json`).
    ///
    /// Resolved relative to the crate's `CARGO_MANIFEST_DIR` if set.
    pub fn snapshot_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.snapshot_path = path.into();
        self
    }

    /// Output filename inside `OUT_DIR` (defaults to `schema.rs`).
    pub fn out_filename<S: Into<String>>(mut self, name: S) -> Self {
        self.out_filename = name.into();
        self
    }

    pub fn run(self) -> Result<()> {
        let manifest_dir = env::var_os("CARGO_MANIFEST_DIR").map(PathBuf::from);
        let snapshot_path = match &manifest_dir {
            Some(dir) if self.snapshot_path.is_relative() => dir.join(&self.snapshot_path),
            _ => self.snapshot_path.clone(),
        };
        let out_dir = env::var_os("OUT_DIR")
            .map(PathBuf::from)
            .ok_or_else(|| Error::codegen("fuwa_codegen::build expected OUT_DIR to be set"))?;
        let out_path = out_dir.join(&self.out_filename);

        emit_rerun_directives(&snapshot_path);

        let table_filters = parse_table_filters(&self.tables)?;
        let offline = env::var_os("FUWA_OFFLINE").is_some_and(|v| !v.is_empty() && v != "0");
        let database_url = env::var("DATABASE_URL").ok();

        let generated = if offline {
            let source = load_snapshot(&snapshot_path, &self.schemas, &table_filters)?;
            generate(GenerateOptions::new(source))?
        } else if let Some(database_url) = database_url {
            let resolved_schemas = if self.schemas.is_empty() {
                vec![DEFAULT_SCHEMA.to_owned()]
            } else {
                self.schemas.clone()
            };
            match generate_live_database_source(
                database_url,
                resolved_schemas.clone(),
                table_filters.clone(),
            ) {
                Ok(output) => output,
                Err(db_err) if db_err.is_unreachable() && snapshot_path.exists() => {
                    println!(
                        "cargo:warning=fuwa_codegen::build: live codegen failed ({db_err}); falling back to snapshot at {}",
                        snapshot_path.display()
                    );
                    let source = load_snapshot(&snapshot_path, &resolved_schemas, &table_filters)?;
                    generate(GenerateOptions::new(source))?
                }
                Err(err) => return Err(err.into_error()),
            }
        } else if snapshot_path.exists() {
            let source = load_snapshot(&snapshot_path, &self.schemas, &table_filters)?;
            generate(GenerateOptions::new(source))?
        } else {
            return Err(Error::codegen(format!(
                "fuwa_codegen::build needs DATABASE_URL or a snapshot at {}; set FUWA_OFFLINE=1 to force snapshot use",
                snapshot_path.display()
            )));
        };

        fs::write(&out_path, generated).map_err(Error::from)?;
        Ok(())
    }
}

fn emit_rerun_directives(snapshot_path: &Path) {
    for directive in rerun_directives(snapshot_path) {
        println!("{directive}");
    }
}

fn rerun_directives(snapshot_path: &Path) -> Vec<String> {
    vec![
        "cargo:rerun-if-env-changed=DATABASE_URL".to_owned(),
        "cargo:rerun-if-env-changed=FUWA_OFFLINE".to_owned(),
        format!("cargo:rerun-if-changed={}", snapshot_path.display()),
    ]
}

fn load_snapshot(
    snapshot_path: &Path,
    schemas: &[String],
    table_filters: &[TableFilter],
) -> Result<CodegenSource> {
    if !snapshot_path.exists() {
        return Err(Error::codegen(format!(
            "fuwa_codegen::build snapshot not found at {}",
            snapshot_path.display()
        )));
    }
    let snapshot = SchemaSnapshot::from_snapshot(snapshot_path)?;
    let schema = if schemas.is_empty() && table_filters.is_empty() {
        snapshot.schema
    } else if schemas.is_empty() {
        let names: Vec<String> = snapshot
            .schema
            .tables
            .iter()
            .map(|table| table.schema.clone())
            .collect();
        snapshot.schema.filter_tables(names, table_filters)
    } else {
        snapshot.schema.filter_tables(schemas, table_filters)
    };
    Ok(CodegenSource::Schema(schema))
}

fn parse_table_filters(tables: &[String]) -> Result<Vec<TableFilter>> {
    tables.iter().map(|t| TableFilter::parse(t)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnDef, DatabaseSchema, RustType, SchemaSnapshot, TableDef};
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn temp_dir(kind: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "fuwa-codegen-build-{kind}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn table_def(schema: &str, name: &str) -> TableDef {
        TableDef {
            schema: schema.to_owned(),
            name: name.to_owned(),
            columns: vec![ColumnDef {
                name: "id".to_owned(),
                ordinal_position: 1,
                pg_type: "bigint".to_owned(),
                pg_type_kind: "b".to_owned(),
                rust_type: RustType::new("i64"),
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

    fn sample_snapshot() -> SchemaSnapshot {
        SchemaSnapshot::new(DatabaseSchema {
            tables: vec![table_def("public", "Account")],
            enums: Vec::new(),
        })
    }

    #[test]
    fn rerun_directives_watch_missing_snapshot_path() {
        let snapshot_path = temp_dir("missing-snapshot").join("fuwa.schema.json");
        assert!(!snapshot_path.exists());

        let directives = rerun_directives(&snapshot_path);

        assert!(directives.contains(&"cargo:rerun-if-env-changed=DATABASE_URL".to_owned()));
        assert!(directives.contains(&"cargo:rerun-if-env-changed=FUWA_OFFLINE".to_owned()));
        assert!(directives.contains(&format!(
            "cargo:rerun-if-changed={}",
            snapshot_path.display()
        )));
    }

    #[test]
    fn builder_writes_schema_to_out_dir_via_snapshot() -> Result<()> {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = temp_dir("happy-path");
        fs::create_dir_all(&dir)?;
        let snapshot_path = dir.join("fuwa.schema.json");
        sample_snapshot().write_to(&snapshot_path)?;

        let prev_offline = env::var_os("FUWA_OFFLINE");
        let prev_database_url = env::var_os("DATABASE_URL");
        let prev_out_dir = env::var_os("OUT_DIR");
        env::set_var("FUWA_OFFLINE", "1");
        env::remove_var("DATABASE_URL");
        env::set_var("OUT_DIR", &dir);

        let result = Builder::new().snapshot_path(&snapshot_path).run();

        match prev_offline {
            Some(value) => env::set_var("FUWA_OFFLINE", value),
            None => env::remove_var("FUWA_OFFLINE"),
        }
        match prev_database_url {
            Some(value) => env::set_var("DATABASE_URL", value),
            None => env::remove_var("DATABASE_URL"),
        }
        match prev_out_dir {
            Some(value) => env::set_var("OUT_DIR", value),
            None => env::remove_var("OUT_DIR"),
        }

        result?;
        let generated = fs::read_to_string(dir.join("schema.rs"))?;
        assert!(generated.contains("pub mod account"));
        assert!(generated.contains("pub const table: Table"));
        assert!(generated.contains(r#"Table::new("public", "Account")"#));
        Ok(())
    }

    #[test]
    fn builder_falls_back_to_snapshot_when_database_url_is_unreachable() -> Result<()> {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = temp_dir("fallback");
        fs::create_dir_all(&dir)?;
        let snapshot_path = dir.join("fuwa.schema.json");
        sample_snapshot().write_to(&snapshot_path)?;

        let prev_offline = env::var_os("FUWA_OFFLINE");
        let prev_database_url = env::var_os("DATABASE_URL");
        let prev_out_dir = env::var_os("OUT_DIR");
        env::remove_var("FUWA_OFFLINE");
        env::set_var(
            "DATABASE_URL",
            "host=127.0.0.1 port=1 user=fuwa dbname=fuwa connect_timeout=1",
        );
        env::set_var("OUT_DIR", &dir);

        let result = Builder::new().snapshot_path(&snapshot_path).run();

        match prev_offline {
            Some(value) => env::set_var("FUWA_OFFLINE", value),
            None => env::remove_var("FUWA_OFFLINE"),
        }
        match prev_database_url {
            Some(value) => env::set_var("DATABASE_URL", value),
            None => env::remove_var("DATABASE_URL"),
        }
        match prev_out_dir {
            Some(value) => env::set_var("OUT_DIR", value),
            None => env::remove_var("OUT_DIR"),
        }

        result?;
        let generated = fs::read_to_string(dir.join("schema.rs"))?;
        assert!(generated.contains("pub mod account"));
        Ok(())
    }

    #[test]
    fn fallback_without_schema_filter_uses_live_default_schema() -> Result<()> {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = temp_dir("fallback-default-schema");
        fs::create_dir_all(&dir)?;
        let snapshot_path = dir.join("fuwa.schema.json");
        SchemaSnapshot::new(DatabaseSchema {
            tables: vec![
                table_def("public", "Account"),
                table_def("private", "Account"),
            ],
            enums: Vec::new(),
        })
        .write_to(&snapshot_path)?;

        let prev_offline = env::var_os("FUWA_OFFLINE");
        let prev_database_url = env::var_os("DATABASE_URL");
        let prev_out_dir = env::var_os("OUT_DIR");
        env::remove_var("FUWA_OFFLINE");
        env::set_var(
            "DATABASE_URL",
            "host=127.0.0.1 port=1 user=fuwa dbname=fuwa connect_timeout=1",
        );
        env::set_var("OUT_DIR", &dir);

        let result = Builder::new().snapshot_path(&snapshot_path).run();

        match prev_offline {
            Some(value) => env::set_var("FUWA_OFFLINE", value),
            None => env::remove_var("FUWA_OFFLINE"),
        }
        match prev_database_url {
            Some(value) => env::set_var("DATABASE_URL", value),
            None => env::remove_var("DATABASE_URL"),
        }
        match prev_out_dir {
            Some(value) => env::set_var("OUT_DIR", value),
            None => env::remove_var("OUT_DIR"),
        }

        result?;
        let generated = fs::read_to_string(dir.join("schema.rs"))?;
        assert!(generated.contains(r#"Table::new("public", "Account")"#));
        assert!(!generated.contains(r#"Table::new("private", "Account")"#));
        assert_eq!(generated.matches("pub mod account").count(), 1);
        Ok(())
    }

    #[test]
    fn builder_does_not_fall_back_for_invalid_database_url() -> Result<()> {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = temp_dir("invalid-url");
        fs::create_dir_all(&dir)?;
        let snapshot_path = dir.join("fuwa.schema.json");
        sample_snapshot().write_to(&snapshot_path)?;

        let prev_offline = env::var_os("FUWA_OFFLINE");
        let prev_database_url = env::var_os("DATABASE_URL");
        let prev_out_dir = env::var_os("OUT_DIR");
        env::remove_var("FUWA_OFFLINE");
        env::set_var("DATABASE_URL", "host=localhost port=not-a-port user=fuwa");
        env::set_var("OUT_DIR", &dir);

        let err = Builder::new()
            .snapshot_path(&snapshot_path)
            .run()
            .expect_err("invalid live database source must not fall back to snapshot");

        match prev_offline {
            Some(value) => env::set_var("FUWA_OFFLINE", value),
            None => env::remove_var("FUWA_OFFLINE"),
        }
        match prev_database_url {
            Some(value) => env::set_var("DATABASE_URL", value),
            None => env::remove_var("DATABASE_URL"),
        }
        match prev_out_dir {
            Some(value) => env::set_var("OUT_DIR", value),
            None => env::remove_var("OUT_DIR"),
        }

        assert!(err.to_string().contains("invalid database URL"));
        assert!(!dir.join("schema.rs").exists());
        Ok(())
    }

    #[test]
    fn builder_errors_when_no_source_available() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = temp_dir("no-source");
        fs::create_dir_all(&dir).unwrap();

        let prev_offline = env::var_os("FUWA_OFFLINE");
        let prev_database_url = env::var_os("DATABASE_URL");
        let prev_out_dir = env::var_os("OUT_DIR");
        env::remove_var("FUWA_OFFLINE");
        env::remove_var("DATABASE_URL");
        env::set_var("OUT_DIR", &dir);

        let err = Builder::new()
            .snapshot_path(dir.join("missing.json"))
            .run()
            .expect_err("expected codegen error when no source is available");

        match prev_offline {
            Some(value) => env::set_var("FUWA_OFFLINE", value),
            None => env::remove_var("FUWA_OFFLINE"),
        }
        match prev_database_url {
            Some(value) => env::set_var("DATABASE_URL", value),
            None => env::remove_var("DATABASE_URL"),
        }
        match prev_out_dir {
            Some(value) => env::set_var("OUT_DIR", value),
            None => env::remove_var("OUT_DIR"),
        }

        assert!(err.to_string().contains("DATABASE_URL"));
    }
}
