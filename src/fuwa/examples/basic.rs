use fuwa::prelude::*;

#[allow(non_upper_case_globals)]
mod users {
    use fuwa::prelude::*;

    pub const table: Table = Table::new("public", "users");
    pub const id: Field<i64, NotNull> = Field::new(table, "id");
    pub const email: Field<String, NotNull> = Field::new(table, "email");
    pub const active: Field<bool, NotNull> = Field::new(table, "active");
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let (client, connection) =
        tokio_postgres::connect(&database_url, tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("postgres connection error: {err}");
        }
    });

    let dsl = Dsl::using(&client);
    let sql = dsl
        .select((users::id, users::email))
        .from(users::table)
        .where_(users::active.eq(bind(true)))
        .limit(10)
        .render()?;

    println!("{}", sql.sql());
    Ok(())
}
