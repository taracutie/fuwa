use fuwa::prelude::*;

#[allow(non_upper_case_globals)]
mod users {
    use fuwa::prelude::*;

    pub const table: Table = Table::new("public", "users");
    pub const id: Field<i64, NotNull> = Field::new(table, "id");
    pub const email: Field<String, NotNull> = Field::new(table, "email");
    pub const active: Field<bool, NotNull> = Field::new(table, "active");
}

#[allow(dead_code)]
mod root_derive_exports {
    use fuwa::FromRow;

    #[derive(FromRow)]
    struct ImportedRootDerive {
        id: i64,
    }

    #[derive(fuwa::FromRow)]
    struct QualifiedRootDerive {
        id: i64,
    }
}

#[test]
fn facade_renders_query() {
    let rendered = fuwa::core::Context::new()
        .select((users::id, users::email))
        .from(users::table)
        .where_(users::active.eq(bind(true)))
        .render()
        .unwrap();

    assert_eq!(
        rendered.sql(),
        r#"select "users"."id", "users"."email" from "public"."users" where ("users"."active" = $1)"#
    );
}

#[allow(dead_code)]
fn fetch_api_shape_compiles(client: &tokio_postgres::Client) {
    let dsl = Dsl::using(client);
    let future = dsl
        .select((users::id, users::email))
        .from(users::table)
        .fetch_all::<(i64, String)>();
    drop(future);
}
