use std::path::PathBuf;

use clap::Parser;
use fuwa_codegen::{
    ensure_read_only_connection, generate_schema_module, introspect_schemas_read_only,
    read_only_connection_config, TableFilter,
};
use fuwa_core::Result;
use tokio_postgres::NoTls;

#[derive(Debug, Parser)]
#[command(
    name = "fuwa-codegen",
    version,
    about = "Generate fuwa schema modules from PostgreSQL"
)]
struct Cli {
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

    /// Table to generate. Accepts `table` or `schema.table`; may be repeated or comma-separated.
    #[arg(long, value_name = "TABLE", value_delimiter = ',')]
    table: Vec<String>,

    /// Output Rust file.
    #[arg(long)]
    out: PathBuf,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let config = read_only_connection_config(&cli.database_url)?;
    let (client, connection) = config
        .connect(NoTls)
        .await
        .map_err(|err| fuwa_core::Error::codegen(err.to_string()))?;

    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("fuwa-codegen PostgreSQL connection task failed: {err}");
        }
    });

    ensure_read_only_connection(&client).await?;
    let table_filters: Vec<TableFilter> = cli
        .table
        .iter()
        .map(|table| TableFilter::parse(table))
        .collect::<Result<_>>()?;
    let schema = introspect_schemas_read_only(&client, &cli.schema, &table_filters).await?;
    let generated = generate_schema_module(&schema)?;
    std::fs::write(&cli.out, generated)?;
    Ok(())
}
