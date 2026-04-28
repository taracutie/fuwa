use fuwa::prelude::*;

#[allow(non_upper_case_globals)]
mod users {
    use fuwa::prelude::*;

    pub const table: Table = Table::new("public", "users");
    pub const id: Field<i64, NotNull> = Field::new(table, "id");
    pub const email: Field<String, NotNull> = Field::new(table, "email");
    pub const active: Field<bool, NotNull> = Field::new(table, "active");
}

fn main() -> fuwa::Result<()> {
    let sql = Context::new()
        .select((users::id, users::email))
        .from(users::table)
        .where_(users::active.eq(bind(true)))
        .limit(10)
        .render()?;

    println!("{}", sql.sql());
    Ok(())
}
