use fuwa::prelude::*;

#[allow(non_upper_case_globals)]
mod users {
    use fuwa::prelude::*;

    pub const table: Table = Table::new("public", "users");
    pub const id: Field<i64, NotNull> = Field::new(table, "id");
    pub const email: Field<String, NotNull> = Field::new(table, "email");
    pub const active: Field<bool, NotNull> = Field::new(table, "active");
}

#[allow(non_upper_case_globals)]
mod renamed_columns {
    use fuwa::prelude::*;

    pub const table: Table = Table::new("public", "renamed_columns");
    pub const user_id: Field<i64, NotNull> = Field::new(table, "userId");
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
fn query_macro_validates_against_snapshot() {
    let sql: &str =
        fuwa::query!("select users.id, users.email from users where users.active = true");
    assert!(sql.contains("users.id"));
    assert!(sql.contains("users.email"));
}

#[test]
fn query_macro_folds_unquoted_identifiers_like_postgres() {
    let sql: &str = fuwa::query!("select Users.ID from Users");
    assert!(sql.contains("Users.ID"));
}

#[test]
fn query_macro_validates_aliased_refs_against_snapshot() {
    let sql: &str = fuwa::query!("select u.id, u.email from users u where u.active = true");
    assert!(sql.contains("u.id"));
    assert!(sql.contains("u.email"));
}

#[test]
fn query_macro_validates_dml_aliases_against_snapshot() {
    let update_sql: &str =
        fuwa::query!("update users as u set email = $1 where u.id = $2 returning u.email");
    assert!(update_sql.contains("u.id"));
    assert!(update_sql.contains("u.email"));

    let insert_sql: &str = fuwa::query!(
        "insert into users as u (id, email, active) values ($1, $2, $3) \
         on conflict (email) do update set email = excluded.email where u.active = true"
    );
    assert!(insert_sql.contains("excluded.email"));
    assert!(insert_sql.contains("u.active"));

    let delete_sql: &str =
        fuwa::query!("delete from users as u where u.active = false returning u.id");
    assert!(delete_sql.contains("u.active"));
    assert!(delete_sql.contains("u.id"));
}

#[test]
fn query_macro_validates_schema_qualified_refs_against_snapshot() {
    let sql: &str = fuwa::query!(
        "select public.users.id, public.users.email from public.users where public.users.active = true"
    );
    assert!(sql.contains("public.users.id"));
    assert!(sql.contains("public.users.email"));
}

#[test]
fn query_macro_skips_non_column_qualified_syntax() {
    let sql: &str = fuwa::query!(
        "select pg_catalog.lower(users.email), $1::public.my_type, $$ users.missing $$, \
         $tag$ public.users.missing $tag$ from users where users.active = true"
    );
    assert!(sql.contains("pg_catalog.lower"));
    assert!(sql.contains("public.my_type"));
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

#[test]
fn dynamic_composition_accumulates_filters_in_a_loop() {
    let active = Some(true);
    let min_id: Option<i64> = Some(10);

    let mut q = select((users::id, users::email)).from(users::table);

    if let Some(active) = active {
        q = q.and_where(users::active.eq(bind(active)));
    }
    if let Some(min) = min_id {
        q = q.and_where(users::id.gt(bind(min)));
    }
    q = q.push_order_by(users::id.asc());
    q = q.push_order_by(users::email.desc());

    let rendered = q.render().unwrap();
    let sql = rendered.sql();
    assert!(sql.contains("active"), "expected active filter, got {sql}");
    assert!(sql.contains("\"users\".\"id\""), "expected id filter");
    assert!(sql.contains("order by"), "expected order by clause: {sql}");
}

#[test]
fn insertable_derive_emits_record_assignments() {
    use fuwa::Insertable;

    #[derive(Insertable)]
    #[fuwa(table = users)]
    #[allow(dead_code)]
    struct UserNew {
        email: String,
        active: bool,
    }

    let manual = insert_into(users::table)
        .values((
            users::email.set(bind("a@example.com")),
            users::active.set(bind(true)),
        ))
        .render()
        .unwrap();

    let derived = insert_into(users::table)
        .value(UserNew {
            email: "a@example.com".to_owned(),
            active: true,
        })
        .render()
        .unwrap();

    assert_eq!(manual.sql(), derived.sql());
}

#[test]
fn insertable_derive_uses_generated_column_ident_for_renamed_sql_columns() {
    use fuwa::Insertable;

    #[derive(Insertable)]
    #[fuwa(table = renamed_columns, rename_all = "camelCase")]
    #[allow(dead_code)]
    struct NewRenamed {
        user_id: i64,
    }

    let manual = insert_into(renamed_columns::table)
        .value(renamed_columns::user_id.set(bind(42_i64)))
        .render()
        .unwrap();

    let derived = insert_into(renamed_columns::table)
        .value(NewRenamed { user_id: 42 })
        .render()
        .unwrap();

    assert_eq!(manual.sql(), derived.sql());
}

#[test]
fn patch_derive_only_emits_some_assignments() {
    use fuwa::Patch;

    #[derive(Patch, Default)]
    #[fuwa(table = users)]
    #[allow(dead_code)]
    struct UserPatch {
        email: Option<String>,
        active: Option<bool>,
    }

    let manual = update(users::table)
        .set(users::email.set(bind("c@d.com")))
        .where_(users::id.eq(bind(1_i64)))
        .render()
        .unwrap();

    let derived = update(users::table)
        .set(UserPatch {
            email: Some("c@d.com".to_owned()),
            ..Default::default()
        })
        .where_(users::id.eq(bind(1_i64)))
        .render()
        .unwrap();

    assert_eq!(manual.sql(), derived.sql());
}

#[test]
fn patch_derive_uses_generated_column_ident_for_renamed_sql_columns() {
    use fuwa::Patch;

    #[derive(Patch, Default)]
    #[fuwa(table = renamed_columns)]
    #[allow(dead_code)]
    struct RenamedPatch {
        #[fuwa(rename = "userId")]
        user_id: Option<i64>,
    }

    let manual = update(renamed_columns::table)
        .set(renamed_columns::user_id.set(bind(42_i64)))
        .where_(renamed_columns::user_id.eq(bind(7_i64)))
        .render()
        .unwrap();

    let derived = update(renamed_columns::table)
        .set(RenamedPatch { user_id: Some(42) })
        .where_(renamed_columns::user_id.eq(bind(7_i64)))
        .render()
        .unwrap();

    assert_eq!(manual.sql(), derived.sql());
}

#[test]
fn free_function_select_renders_same_as_dsl_method() {
    let dsl_form = fuwa::core::Context::new()
        .select((users::id, users::email))
        .from(users::table)
        .where_(users::active.eq(bind(true)))
        .render()
        .unwrap();

    let free_form = select((users::id, users::email))
        .from(users::table)
        .where_(users::active.eq(bind(true)))
        .render()
        .unwrap();

    assert_eq!(dsl_form.sql(), free_form.sql());
}

#[allow(dead_code)]
fn dsl_runner_methods_compile(client: &tokio_postgres::Client) {
    let dsl = Dsl::using(client);
    let q = select((users::id, users::email))
        .from(users::table)
        .where_(users::active.eq(bind(true)));
    let future = dsl.fetch_all(q);
    drop(future);

    let q2 = select(users::id).from(users::table);
    let future2 = dsl.fetch_optional(q2);
    drop(future2);

    let q3 = insert_into(users::table)
        .values(users::email.set(bind("a@b.com")))
        .returning(users::id);
    let future3 = dsl.fetch_one(q3);
    drop(future3);

    let q4 = update(users::table)
        .set(users::email.set(bind("c@d.com")))
        .where_(users::id.eq(bind(1_i64)));
    let future4 = dsl.execute(q4);
    drop(future4);

    let q5 = delete_from(users::table).where_(users::active.eq(bind(false)));
    let future5 = dsl.execute(q5);
    drop(future5);

    let future6 = dsl
        .select(users::email)
        .from(users::table)
        .where_(exists(dsl.select(users::id).from(users::table)))
        .fetch_all();
    drop(future6);

    let future7 = dsl
        .select(users::email)
        .from(users::table)
        .where_(not_exists(dsl.select(users::id).from(users::table)))
        .fetch_all();
    drop(future7);
}

#[test]
fn tuple_where_renders_as_and() {
    let chained = fuwa::core::Context::new()
        .select(users::id)
        .from(users::table)
        .where_(users::active.eq(bind(true)).and(users::id.gt(bind(10_i64))))
        .render()
        .unwrap();

    let tupled = fuwa::core::Context::new()
        .select(users::id)
        .from(users::table)
        .where_((users::active.eq(bind(true)), users::id.gt(bind(10_i64))))
        .render()
        .unwrap();

    assert_eq!(chained.sql(), tupled.sql());
}

#[test]
fn render_ref_does_not_consume_query() {
    let query = fuwa::core::Context::new()
        .select((users::id, users::email))
        .from(users::table)
        .where_(users::active.eq(bind(true)));

    let first = query.render_ref().unwrap();
    let second = query.render_ref().unwrap();

    assert_eq!(first.sql(), second.sql());
    assert_eq!(
        first.sql(),
        r#"select "users"."id", "users"."email" from "public"."users" where ("users"."active" = $1)"#
    );
    // Display impl on RenderedQuery emits the SQL string.
    assert_eq!(format!("{first}"), first.sql());

    // The query is still owned and can be consumed normally.
    let consumed = query.render().unwrap();
    assert_eq!(consumed.sql(), first.sql());
}

#[allow(dead_code)]
fn fetch_api_shape_compiles(client: &tokio_postgres::Client) {
    let dsl = Dsl::using(client);
    let future = dsl
        .select((users::id, users::email))
        .from(users::table)
        .fetch_all();
    drop(future);
}

#[allow(dead_code)]
fn borrowed_client_transaction_api_shape_compiles(client: &mut tokio_postgres::Client) {
    let mut dsl = Dsl::using(client);
    let future = dsl.transaction(async |tx| {
        tx.raw("select 1").execute().await?;
        Ok::<_, fuwa::Error>(())
    });
    drop(future);
}

#[allow(dead_code)]
fn borrowed_transaction_api_shape_compiles(transaction: &mut tokio_postgres::Transaction<'_>) {
    let dsl = Dsl::using(transaction);
    let future = dsl.raw("select 1").fetch_one_as::<i64>();
    drop(future);
}

#[allow(dead_code)]
fn borrowed_transaction_nested_transaction_api_shape_compiles(
    transaction: &mut tokio_postgres::Transaction<'_>,
) {
    let mut dsl = Dsl::using(transaction);
    let future = dsl.transaction(async |tx| {
        tx.raw("select 1").execute().await?;
        Ok::<_, fuwa::Error>(())
    });
    drop(future);
}
