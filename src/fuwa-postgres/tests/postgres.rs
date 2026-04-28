use fuwa_core::prelude::*;
use fuwa_postgres::{transaction, PgQueryExt};
use rust_decimal::Decimal;
use tokio_postgres::NoTls;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

#[allow(non_upper_case_globals)]
mod users {
    use fuwa_core::prelude::*;

    pub const table: Table = Table::new("public", "fuwa_test_users");
    pub const id: Field<i64, NotNull> = Field::new(table, "id");
    pub const email: Field<String, NotNull> = Field::new(table, "email");
    pub const active: Field<bool, NotNull> = Field::new(table, "active");
}

#[allow(non_upper_case_globals)]
mod upsert_users {
    use fuwa_core::prelude::*;

    pub const table: Table = Table::new("public", "fuwa_test_upsert_users");
    pub const id: Field<i64, NotNull> = Field::new(table, "id");
    pub const email: Field<String, NotNull> = Field::new(table, "email");
    pub const display_name: Field<String, Nullable> = Field::new(table, "display_name");
    pub const active: Field<bool, NotNull> = Field::new(table, "active");
}

#[allow(non_upper_case_globals)]
mod accounts {
    use fuwa_core::prelude::*;
    use rust_decimal::Decimal;

    pub const table: Table = Table::new("fuwa_it_complex", "accounts");
    pub const id: Field<i64, NotNull> = Field::new(table, "id");
    pub const email: Field<String, NotNull> = Field::new(table, "email");
    pub const display_name: Field<String, Nullable> = Field::new(table, "display_name");
    pub const active: Field<bool, NotNull> = Field::new(table, "active");
    pub const signup_rank: Field<i32, NotNull> = Field::new(table, "signup_rank");
    pub const account_balance: Field<Decimal, NotNull> = Field::new(table, "account_balance");
}

#[allow(non_upper_case_globals)]
mod posts {
    use fuwa_core::prelude::*;

    pub const table: Table = Table::new("fuwa_it_complex", "posts");
    pub const id: Field<i64, NotNull> = Field::new(table, "id");
    pub const account_id: Field<i64, NotNull> = Field::new(table, "account_id");
    pub const title: Field<String, NotNull> = Field::new(table, "title");
    pub const published: Field<bool, NotNull> = Field::new(table, "published");
    pub const score: Field<i32, NotNull> = Field::new(table, "score");
    pub const body: Field<String, Nullable> = Field::new(table, "body");
}

#[allow(non_upper_case_globals)]
mod comments {
    use fuwa_core::prelude::*;

    pub const table: Table = Table::new("fuwa_it_complex", "comments");
    pub const id: Field<i64, NotNull> = Field::new(table, "id");
    pub const post_id: Field<i64, NotNull> = Field::new(table, "post_id");
    pub const body: Field<String, NotNull> = Field::new(table, "body");
}

#[tokio::test]
async fn postgres_round_trip_when_database_url_is_set() -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL integration test: FUWA_TEST_DATABASE_URL is not set");
        return Ok(());
    };

    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("PostgreSQL connection task failed: {err}");
        }
    });

    client
        .batch_execute(
            r#"
            drop table if exists public.fuwa_test_users;
            create table public.fuwa_test_users (
                id bigserial primary key,
                email text not null,
                active boolean not null
            );
            "#,
        )
        .await?;

    let ctx = Context::new();

    let inserted_id = ctx
        .insert_into(users::table)
        .values((
            users::email.set(bind("a@example.com")),
            users::active.set(bind(true)),
        ))
        .returning(users::id)
        .fetch_one::<i64>(&client)
        .await?;

    let rows = ctx
        .select((users::id, users::email))
        .from(users::table)
        .where_(users::id.eq(bind(inserted_id)))
        .fetch_all::<(i64, String)>(&client)
        .await?;
    assert_eq!(rows, vec![(inserted_id, "a@example.com".to_owned())]);

    let missing = ctx
        .select(users::id)
        .from(users::table)
        .where_(users::email.eq(bind("missing@example.com")))
        .fetch_optional::<i64>(&client)
        .await?;
    assert_eq!(missing, None);

    let updated = ctx
        .update(users::table)
        .set(users::email.set(bind("new@example.com")))
        .where_(users::id.eq(bind(inserted_id)))
        .returning(users::email)
        .fetch_one::<String>(&client)
        .await?;
    assert_eq!(updated, "new@example.com");

    let deleted = ctx
        .delete_from(users::table)
        .where_(users::id.eq(bind(inserted_id)))
        .execute(&client)
        .await?;
    assert_eq!(deleted, 1);

    client
        .batch_execute("drop table if exists public.fuwa_test_users;")
        .await?;

    Ok(())
}

#[tokio::test]
async fn insert_conflict_and_transaction_helpers_when_database_url_is_set() -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL integration test: FUWA_TEST_DATABASE_URL is not set");
        return Ok(());
    };

    let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("PostgreSQL connection task failed: {err}");
        }
    });

    client
        .batch_execute(
            r#"
            drop table if exists public.fuwa_test_upsert_users;
            create table public.fuwa_test_upsert_users (
                id bigint primary key,
                email text not null unique,
                display_name text,
                active boolean not null
            );
            "#,
        )
        .await?;

    let ctx = Context::new();

    let inserted = ctx
        .insert_into(upsert_users::table)
        .values_many([
            (
                upsert_users::id.set(bind(1_i64)),
                upsert_users::email.set(bind("ada@example.com")),
                upsert_users::display_name.set(bind(Some("Ada"))),
                upsert_users::active.set(bind(true)),
            ),
            (
                upsert_users::id.set(bind(2_i64)),
                upsert_users::email.set(bind("ben@example.com")),
                upsert_users::display_name.set(bind(Some("Ben"))),
                upsert_users::active.set(bind(false)),
            ),
        ])
        .execute(&client)
        .await?;
    assert_eq!(inserted, 2);

    let rows = ctx
        .select((upsert_users::id, upsert_users::email))
        .from(upsert_users::table)
        .order_by(upsert_users::id.asc())
        .fetch_all::<(i64, String)>(&client)
        .await?;
    assert_eq!(
        rows,
        vec![
            (1, "ada@example.com".to_owned()),
            (2, "ben@example.com".to_owned()),
        ]
    );

    let ignored = ctx
        .insert_into(upsert_users::table)
        .values((
            upsert_users::id.set(bind(3_i64)),
            upsert_users::email.set(bind("ada@example.com")),
            upsert_users::display_name.set(bind(Some("Changed"))),
            upsert_users::active.set(bind(false)),
        ))
        .on_conflict((upsert_users::email,))
        .do_nothing()
        .returning(upsert_users::id)
        .fetch_optional::<i64>(&client)
        .await?;
    assert_eq!(ignored, None);

    let unchanged = ctx
        .select((upsert_users::display_name, upsert_users::active))
        .from(upsert_users::table)
        .where_(upsert_users::email.eq(bind("ada@example.com")))
        .fetch_one::<(Option<String>, bool)>(&client)
        .await?;
    assert_eq!(unchanged, (Some("Ada".to_owned()), true));

    let updated = ctx
        .insert_into(upsert_users::table)
        .values((
            upsert_users::id.set(bind(4_i64)),
            upsert_users::email.set(bind("ben@example.com")),
            upsert_users::display_name.set(bind(Some("Benedict"))),
            upsert_users::active.set(bind(true)),
        ))
        .on_conflict((upsert_users::email,))
        .do_update(|excluded| {
            (
                upsert_users::display_name.set(excluded.field(upsert_users::display_name)),
                upsert_users::active.set(excluded.field(upsert_users::active)),
            )
        })
        .returning((
            upsert_users::id,
            upsert_users::display_name,
            upsert_users::active,
        ))
        .fetch_one::<(i64, Option<String>, bool)>(&client)
        .await?;
    assert_eq!(updated, (2, Some("Benedict".to_owned()), true));

    let tx_result = transaction(&mut client, |tx| {
        Box::pin(async move {
            ctx.insert_into(upsert_users::table)
                .values((
                    upsert_users::id.set(bind(5_i64)),
                    upsert_users::email.set(bind("rolled-back@example.com")),
                    upsert_users::display_name.set(bind(Some("Rollback"))),
                    upsert_users::active.set(bind(true)),
                ))
                .execute(tx)
                .await?;

            ctx.insert_into(upsert_users::table)
                .values((
                    upsert_users::id.set(bind(1_i64)),
                    upsert_users::email.set(bind("duplicate-id@example.com")),
                    upsert_users::display_name.set(bind(None::<String>)),
                    upsert_users::active.set(bind(true)),
                ))
                .execute(tx)
                .await?;

            Ok(())
        })
    })
    .await;
    assert!(tx_result.is_err());

    let rolled_back_count = ctx
        .select(count_star())
        .from(upsert_users::table)
        .where_(upsert_users::email.eq(bind("rolled-back@example.com")))
        .fetch_one::<i64>(&client)
        .await?;
    assert_eq!(rolled_back_count, 0);

    client
        .batch_execute("drop table if exists public.fuwa_test_upsert_users;")
        .await?;

    Ok(())
}

#[tokio::test]
async fn complex_schema_queries_with_real_data_when_database_url_is_set() -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL integration test: FUWA_TEST_DATABASE_URL is not set");
        return Ok(());
    };

    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("PostgreSQL connection task failed: {err}");
        }
    });

    client
        .batch_execute(
            r#"
            drop schema if exists fuwa_it_complex cascade;
            create schema fuwa_it_complex;

            create table fuwa_it_complex.accounts (
                id bigint primary key,
                email text not null unique,
                display_name text,
                active boolean not null,
                signup_rank integer not null,
                account_balance numeric not null
            );

            create table fuwa_it_complex.posts (
                id bigint primary key,
                account_id bigint not null references fuwa_it_complex.accounts(id),
                title text not null,
                published boolean not null,
                score integer not null,
                body text
            );

            create table fuwa_it_complex.comments (
                id bigint primary key,
                post_id bigint not null references fuwa_it_complex.posts(id),
                body text not null
            );

            create table fuwa_it_complex.recent_buffers (
                "userId" text primary key,
                "recentImages" text[] not null
            );

            insert into fuwa_it_complex.accounts
                (id, email, display_name, active, signup_rank, account_balance)
            values
                (1, 'ada@example.com', 'Ada', true, 10, 10.25),
                (2, 'ben@example.com', null, false, 20, 20.50),
                (3, 'cy@example.com', null, true, 30, 30.25),
                (4, 'dana@example.com', null, true, 40, 40.00);

            insert into fuwa_it_complex.posts
                (id, account_id, title, published, score, body)
            values
                (10, 1, 'Rust DSLs', true, 42, 'typed query builders'),
                (11, 1, 'Draft notes', false, 3, null),
                (20, 2, 'Inactive account post', true, 12, 'should be filtered'),
                (30, 3, 'Fuwa roadmap', true, 77, null);

            insert into fuwa_it_complex.comments (id, post_id, body)
            values
                (100, 10, 'first'),
                (101, 10, 'second'),
                (110, 11, 'draft-only'),
                (300, 30, 'roadmap comment');

            insert into fuwa_it_complex.recent_buffers ("userId", "recentImages")
            values ('ada', array['img-a.jpg', 'img-b.jpg']);
            "#,
        )
        .await?;

    let ctx = Context::new();

    let published_for_active_accounts = ctx
        .select((accounts::email, posts::title, posts::score))
        .from(accounts::table)
        .join(posts::table.on(posts::account_id.eq(accounts::id)))
        .where_(
            accounts::active
                .eq(bind(true))
                .and(posts::published.eq(bind(true))),
        )
        .order_by((accounts::id.asc(), posts::id.asc()))
        .fetch_all::<(String, String, i32)>(&client)
        .await?;

    assert_eq!(
        published_for_active_accounts,
        vec![
            ("ada@example.com".to_owned(), "Rust DSLs".to_owned(), 42),
            ("cy@example.com".to_owned(), "Fuwa roadmap".to_owned(), 77),
        ]
    );

    let active_accounts_with_optional_published_posts = ctx
        .select((accounts::email, nullable(posts::title)))
        .from(accounts::table)
        .left_join(
            posts::table.on(posts::account_id
                .eq(accounts::id)
                .and(posts::published.eq(bind(true)))),
        )
        .where_(accounts::active.eq(bind(true)))
        .order_by(accounts::id.asc())
        .fetch_all::<(String, Option<String>)>(&client)
        .await?;

    assert_eq!(
        active_accounts_with_optional_published_posts,
        vec![
            ("ada@example.com".to_owned(), Some("Rust DSLs".to_owned())),
            ("cy@example.com".to_owned(), Some("Fuwa roadmap".to_owned())),
            ("dana@example.com".to_owned(), None),
        ]
    );

    let accounts_without_display_name = ctx
        .select((accounts::id, accounts::display_name))
        .from(accounts::table)
        .where_(accounts::display_name.is_null())
        .order_by(accounts::id.asc())
        .fetch_all::<(i64, Option<String>)>(&client)
        .await?;

    assert_eq!(
        accounts_without_display_name,
        vec![(2, None), (3, None), (4, None)]
    );

    let high_rank_accounts = ctx
        .select(accounts::email)
        .from(accounts::table)
        .where_(accounts::signup_rank.gte(bind(30_i32)))
        .order_by(accounts::signup_rank.desc())
        .fetch_all::<String>(&client)
        .await?;

    assert_eq!(
        high_rank_accounts,
        vec!["dana@example.com".to_owned(), "cy@example.com".to_owned()]
    );

    let ada_balance = ctx
        .select(accounts::account_balance)
        .from(accounts::table)
        .where_(accounts::email.eq(bind("ada@example.com")))
        .fetch_one::<Decimal>(&client)
        .await?;

    assert_eq!(ada_balance, Decimal::new(1025, 2));

    let active_balance_total = ctx
        .select(sum(accounts::account_balance))
        .from(accounts::table)
        .where_(accounts::active.eq(bind(true)))
        .fetch_one::<Option<Decimal>>(&client)
        .await?;

    assert_eq!(active_balance_total, Some(Decimal::new(8050, 2)));

    let filtered_accounts = ctx
        .select(accounts::email)
        .from(accounts::table)
        .where_(
            accounts::id
                .in_([bind(1_i64), bind(2_i64), bind(3_i64), bind(4_i64)])
                .and(accounts::signup_rank.between(bind(10_i32), bind(40_i32)))
                .and(accounts::email.not_in([bind("ben@example.com")]))
                .and(accounts::signup_rank.not_between(bind(35_i32), bind(45_i32))),
        )
        .order_by(accounts::id.asc())
        .fetch_all::<String>(&client)
        .await?;

    assert_eq!(
        filtered_accounts,
        vec!["ada@example.com".to_owned(), "cy@example.com".to_owned()]
    );

    let adjusted_scores = ctx
        .select((posts::id, posts::score.expr() + bind(8_i32)))
        .from(posts::table)
        .where_(posts::id.in_([bind(10_i64), bind(30_i64)]))
        .order_by(posts::id.asc())
        .fetch_all::<(i64, i32)>(&client)
        .await?;

    assert_eq!(adjusted_scores, vec![(10, 50), (30, 85)]);

    let account_labels = ctx
        .select((
            accounts::id,
            concat(
                coalesce((accounts::display_name, accounts::email)),
                bind(" account"),
            ),
            nullif(accounts::display_name, bind("Ada")),
            case_when()
                .when(accounts::active.eq(bind(true)), bind("active"))
                .else_(bind("inactive")),
        ))
        .from(accounts::table)
        .where_(accounts::id.in_([bind(1_i64), bind(2_i64)]))
        .order_by(accounts::id.asc())
        .fetch_all::<(i64, String, Option<String>, String)>(&client)
        .await?;

    assert_eq!(
        account_labels,
        vec![
            (1, "Ada account".to_owned(), None, "active".to_owned()),
            (
                2,
                "ben@example.com account".to_owned(),
                None,
                "inactive".to_owned()
            ),
        ]
    );

    let inserted_balance = ctx
        .insert_into(accounts::table)
        .values((
            accounts::id.set(bind(5_i64)),
            accounts::email.set(bind("eve@example.com")),
            accounts::display_name.set(bind(None::<String>)),
            accounts::active.set(bind(true)),
            accounts::signup_rank.set(bind(50_i32)),
            accounts::account_balance.set(bind(Decimal::new(5050, 2))),
        ))
        .returning(accounts::account_balance)
        .fetch_one::<Decimal>(&client)
        .await?;

    assert_eq!(inserted_balance, Decimal::new(5050, 2));

    let posts_without_body = ctx
        .select((posts::id, posts::body))
        .from(posts::table)
        .where_(posts::body.is_null())
        .order_by(posts::id.asc())
        .fetch_all::<(i64, Option<String>)>(&client)
        .await?;

    assert_eq!(posts_without_body, vec![(11, None), (30, None)]);

    let published_post_count = ctx
        .select(count_star())
        .from(posts::table)
        .where_(posts::published.eq(bind(true)))
        .fetch_one::<i64>(&client)
        .await?;

    assert_eq!(published_post_count, 3);

    let post_counts_by_account = ctx
        .select((posts::account_id, count_star()))
        .from(posts::table)
        .group_by(posts::account_id)
        .having(count_star().gt(bind(1_i64)))
        .order_by(posts::account_id.asc())
        .fetch_all::<(i64, i64)>(&client)
        .await?;

    assert_eq!(post_counts_by_account, vec![(1, 2)]);

    let inserted_comment = ctx
        .insert_into(comments::table)
        .values((
            comments::id.set(bind(301_i64)),
            comments::post_id.set(bind(30_i64)),
            comments::body.set(bind("late addition")),
        ))
        .returning((comments::id, comments::body))
        .fetch_one::<(i64, String)>(&client)
        .await?;

    assert_eq!(inserted_comment, (301, "late addition".to_owned()));

    let updated_post = ctx
        .update(posts::table)
        .set(posts::score.set(bind(100_i32)))
        .where_(posts::id.eq(bind(10_i64)))
        .returning((posts::id, posts::score))
        .fetch_one::<(i64, i32)>(&client)
        .await?;

    assert_eq!(updated_post, (10, 100));

    let deleted_draft_comments = ctx
        .delete_from(comments::table)
        .where_(comments::post_id.eq(bind(11_i64)))
        .execute(&client)
        .await?;

    assert_eq!(deleted_draft_comments, 1);

    let remaining_comment_count = ctx
        .select(count_star())
        .from(comments::table)
        .fetch_one::<i64>(&client)
        .await?;

    assert_eq!(remaining_comment_count, 4);

    let recent_images = raw(r#"select "recentImages"
           from fuwa_it_complex.recent_buffers
           where "userId" = $1"#)
    .bind("ada")
    .fetch_one::<Vec<String>>(&client)
    .await?;

    assert_eq!(
        recent_images,
        vec!["img-a.jpg".to_owned(), "img-b.jpg".to_owned()]
    );

    let selected_emails = raw(r#"select email
           from fuwa_it_complex.accounts
           where email = any($1)
           order by email"#)
    .bind(vec![
        "cy@example.com".to_owned(),
        "ada@example.com".to_owned(),
    ])
    .fetch_all::<String>(&client)
    .await?;

    assert_eq!(
        selected_emails,
        vec!["ada@example.com".to_owned(), "cy@example.com".to_owned()]
    );

    client
        .batch_execute("drop schema if exists fuwa_it_complex cascade;")
        .await?;

    Ok(())
}
