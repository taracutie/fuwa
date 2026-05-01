use fuwa_core::{bind, Context, Field, NotNull, Nullable, RenderedQuery, Table};

mod users {
    use super::{Field, NotNull, Nullable, Table};

    pub const TABLE: Table = Table::new("public", "users");
    pub const ID: Field<i64, NotNull> = TABLE.field("id");
    pub const EMAIL: Field<String, NotNull> = TABLE.field("email");
    pub const DISPLAY_NAME: Field<String, Nullable> = TABLE.field("display_name");
    pub const ACTIVE: Field<bool, NotNull> = TABLE.field("active");
    pub const SIGNUP_RANK: Field<i32, NotNull> = TABLE.field("signup_rank");
}

fn assert_same_rendered(implicit: RenderedQuery, explicit: RenderedQuery) {
    assert_eq!(implicit.sql(), explicit.sql());
    assert_eq!(implicit.binds().len(), explicit.binds().len());
    assert_eq!(
        format!("{:?}", implicit.binds()),
        format!("{:?}", explicit.binds())
    );
}

fn render_where(condition: fuwa_core::Condition) -> RenderedQuery {
    Context::new()
        .select(users::ID)
        .from(users::TABLE)
        .where_(condition)
        .render()
        .unwrap()
}

#[test]
fn scalar_comparators_implicitly_bind_values() {
    let implicit = render_where(
        users::ACTIVE
            .eq(true)
            .and(users::ID.ne(8_i64))
            .and(users::SIGNUP_RANK.lt(9_i32))
            .and(users::SIGNUP_RANK.lte(10_i32))
            .and(users::SIGNUP_RANK.gt(1_i32))
            .and(users::SIGNUP_RANK.gte(2_i32)),
    );
    let explicit = render_where(
        users::ACTIVE
            .eq(bind(true))
            .and(users::ID.ne(bind(8_i64)))
            .and(users::SIGNUP_RANK.lt(bind(9_i32)))
            .and(users::SIGNUP_RANK.lte(bind(10_i32)))
            .and(users::SIGNUP_RANK.gt(bind(1_i32)))
            .and(users::SIGNUP_RANK.gte(bind(2_i32))),
    );

    assert_same_rendered(implicit, explicit);
}

#[test]
fn string_comparators_implicitly_bind_values() {
    let implicit = render_where(
        users::EMAIL
            .like("%@example.com")
            .and(users::EMAIL.ilike("ada%")),
    );
    let explicit = render_where(
        users::EMAIL
            .like(bind("%@example.com"))
            .and(users::EMAIL.ilike(bind("ada%"))),
    );

    assert_same_rendered(implicit, explicit);
}

#[test]
fn list_comparators_implicitly_bind_values() {
    let implicit = render_where(
        users::ID
            .in_([1_i64, 2_i64])
            .and(users::EMAIL.not_in(["blocked@example.com", "spam@example.com"])),
    );
    let explicit = render_where(
        users::ID
            .in_([bind(1_i64), bind(2_i64)])
            .and(users::EMAIL.not_in([bind("blocked@example.com"), bind("spam@example.com")])),
    );

    assert_same_rendered(implicit, explicit);
}

#[test]
fn between_implicitly_binds_values() {
    let implicit = render_where(users::SIGNUP_RANK.between(10_i32, 20_i32));
    let explicit = render_where(users::SIGNUP_RANK.between(bind(10_i32), bind(20_i32)));

    assert_same_rendered(implicit, explicit);
}

#[test]
fn assignments_implicitly_bind_values() {
    let implicit_insert = Context::new()
        .insert_into(users::TABLE)
        .values((
            users::EMAIL.set("hi"),
            users::DISPLAY_NAME.set(Some("Ada")),
            users::ACTIVE.set(true),
        ))
        .render()
        .unwrap();
    let explicit_insert = Context::new()
        .insert_into(users::TABLE)
        .values((
            users::EMAIL.set(bind("hi")),
            users::DISPLAY_NAME.set(bind(Some("Ada"))),
            users::ACTIVE.set(bind(true)),
        ))
        .render()
        .unwrap();

    assert_same_rendered(implicit_insert, explicit_insert);

    let implicit_update = Context::new()
        .update(users::TABLE)
        .set((
            users::EMAIL.set("new@example.com"),
            users::ACTIVE.set(false),
        ))
        .where_(users::ID.eq(7_i64))
        .render()
        .unwrap();
    let explicit_update = Context::new()
        .update(users::TABLE)
        .set((
            users::EMAIL.set(bind("new@example.com")),
            users::ACTIVE.set(bind(false)),
        ))
        .where_(users::ID.eq(bind(7_i64)))
        .render()
        .unwrap();

    assert_same_rendered(implicit_update, explicit_update);
}

#[test]
fn expression_and_field_rvalues_still_pass_through() {
    let field_query = render_where(users::ID.eq(users::ID));

    assert_eq!(
        field_query.sql(),
        r#"select "users"."id" from "public"."users" where ("users"."id" = "users"."id")"#
    );
    assert_eq!(field_query.binds().len(), 0);

    let expr_query = render_where(users::SIGNUP_RANK.eq(users::SIGNUP_RANK.expr() + bind(1_i32)));

    assert_eq!(
        expr_query.sql(),
        concat!(
            r#"select "users"."id" from "public"."users" "#,
            r#"where ("users"."signup_rank" = ("users"."signup_rank" + $1))"#
        )
    );
    assert_eq!(expr_query.binds().len(), 1);
}
