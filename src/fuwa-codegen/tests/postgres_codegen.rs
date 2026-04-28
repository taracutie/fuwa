use fuwa_codegen::{
    ensure_read_only_connection, generate_schema_module, introspect_schema_read_only,
    introspect_schemas_read_only, read_only_connection_config, TableFilter,
};
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn introspects_schema_with_related_tables_when_database_url_is_set() -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!("skipping codegen integration test: FUWA_TEST_DATABASE_URL is not set");
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
            drop schema if exists fuwa_codegen_it cascade;
            create schema fuwa_codegen_it;
            create type fuwa_codegen_it.preference_kind as enum ('NEVER', 'MORE', 'LESS');

            create table fuwa_codegen_it.users (
                id bigint primary key,
                email text not null unique,
                display_name text,
                active boolean not null default true,
                profile jsonb not null default '{}'::jsonb,
                balance numeric not null default 0,
                balance_history numeric[] not null default '{}'::numeric[]
            );

            create table fuwa_codegen_it.posts (
                id bigint primary key,
                user_id bigint not null references fuwa_codegen_it.users(id),
                title varchar(100) not null,
                published_at timestamp,
                created_on date not null default current_date
            );

            create table fuwa_codegen_it."RecentImagePair" (
                "userId" text not null,
                "image1" text not null,
                "image2" text not null,
                "shownAt" timestamptz not null
            );

            create table fuwa_codegen_it."SwipeRecentBuffer" (
                "userId" text primary key,
                "recentImages" text[] not null
            );

            create table fuwa_codegen_it."UserImagePreference" (
                "userId" text not null,
                filename text not null,
                type fuwa_codegen_it.preference_kind not null
            );

            insert into fuwa_codegen_it.users
                (id, email, display_name, active, profile, balance, balance_history)
            values
                (1, 'ada@example.com', 'Ada', true, '{"role":"admin"}'::jsonb, 10.25, array[10.25, 11.50]::numeric[]),
                (2, 'ben@example.com', null, false, '{"role":"writer"}'::jsonb, 20.50, array[20.50]::numeric[]);

            insert into fuwa_codegen_it.posts
                (id, user_id, title, published_at, created_on)
            values
                (10, 1, 'Rust DSLs', timestamp '2026-04-28 10:15:00', date '2026-04-28'),
                (20, 2, 'Draft', null, date '2026-04-27');

            insert into fuwa_codegen_it."SwipeRecentBuffer" ("userId", "recentImages")
            values ('ada', array['img-a.jpg', 'img-b.jpg']);

            insert into fuwa_codegen_it."UserImagePreference" ("userId", filename, type)
            values ('ada', 'img-a.jpg', 'MORE');
            "#,
        )
        .await?;

    let read_only_config = read_only_connection_config(&database_url)?;
    let (read_only_client, read_only_connection) = read_only_config.connect(NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = read_only_connection.await {
            eprintln!("PostgreSQL read-only connection task failed: {err}");
        }
    });

    ensure_read_only_connection(&read_only_client).await?;

    let write_error = read_only_client
        .batch_execute("create table fuwa_codegen_it.must_not_be_created (id bigint)")
        .await
        .expect_err("codegen read-only connection must reject writes");
    assert!(
        write_error
            .as_db_error()
            .is_some_and(|err| *err.code() == SqlState::READ_ONLY_SQL_TRANSACTION),
        "unexpected write rejection error: {write_error}"
    );

    let schema = introspect_schema_read_only(&read_only_client, "fuwa_codegen_it").await?;
    assert_eq!(schema.tables.len(), 5);

    let users = schema
        .tables
        .iter()
        .find(|table| table.name == "users")
        .expect("users table should be introspected");
    assert_eq!(users.schema, "fuwa_codegen_it");
    assert_eq!(users.columns.len(), 7);

    let id = users
        .columns
        .iter()
        .find(|column| column.name == "id")
        .expect("id column should be introspected");
    assert_eq!(id.pg_type, "int8");
    assert_eq!(id.rust_type.path(), "i64");
    assert!(!id.nullable);
    assert!(id.primary_key);

    let display_name = users
        .columns
        .iter()
        .find(|column| column.name == "display_name")
        .expect("display_name column should be introspected");
    assert_eq!(display_name.rust_type.path(), "String");
    assert!(display_name.nullable);

    let profile = users
        .columns
        .iter()
        .find(|column| column.name == "profile")
        .expect("profile column should be introspected");
    assert_eq!(profile.pg_type, "jsonb");
    assert_eq!(profile.rust_type.path(), "fuwa::types::Value");
    assert!(profile.default_expression.is_some());

    let balance = users
        .columns
        .iter()
        .find(|column| column.name == "balance")
        .expect("balance column should be introspected");
    assert_eq!(balance.pg_type, "numeric");
    assert_eq!(balance.rust_type.path(), "fuwa::types::Decimal");

    let balance_history = users
        .columns
        .iter()
        .find(|column| column.name == "balance_history")
        .expect("balance_history column should be introspected");
    assert_eq!(balance_history.pg_type, "_numeric");
    assert_eq!(
        balance_history.rust_type.path(),
        "Vec<fuwa::types::Decimal>"
    );

    let posts = schema
        .tables
        .iter()
        .find(|table| table.name == "posts")
        .expect("posts table should be introspected");
    let published_at = posts
        .columns
        .iter()
        .find(|column| column.name == "published_at")
        .expect("published_at column should be introspected");
    assert_eq!(published_at.pg_type, "timestamp");
    assert_eq!(published_at.rust_type.path(), "fuwa::types::NaiveDateTime");
    assert!(published_at.nullable);

    let recent_pair = schema
        .tables
        .iter()
        .find(|table| table.name == "RecentImagePair")
        .expect("RecentImagePair table should be introspected");
    let shown_at = recent_pair
        .columns
        .iter()
        .find(|column| column.name == "shownAt")
        .expect("shownAt column should be introspected");
    assert_eq!(
        shown_at.rust_type.path(),
        "fuwa::types::DateTime<fuwa::types::Utc>"
    );

    let recent_buffer = schema
        .tables
        .iter()
        .find(|table| table.name == "SwipeRecentBuffer")
        .expect("SwipeRecentBuffer table should be introspected");
    let recent_images = recent_buffer
        .columns
        .iter()
        .find(|column| column.name == "recentImages")
        .expect("recentImages column should be introspected");
    assert_eq!(recent_images.pg_type, "_text");
    assert_eq!(recent_images.rust_type.path(), "Vec<String>");

    let preferences = schema
        .tables
        .iter()
        .find(|table| table.name == "UserImagePreference")
        .expect("UserImagePreference table should be introspected");
    let preference_type = preferences
        .columns
        .iter()
        .find(|column| column.name == "type")
        .expect("type column should be introspected");
    assert_eq!(preference_type.pg_type_kind, "e");
    assert_eq!(preference_type.rust_type.path(), "String");

    let generated = generate_schema_module(&schema)?;
    assert!(generated.contains("pub mod users"));
    assert!(generated.contains("pub mod posts"));
    assert!(generated.contains("pub mod recent_image_pair"));
    assert!(generated.contains("pub mod swipe_recent_buffer"));
    assert!(generated.contains("pub mod user_image_preference"));
    assert!(generated.contains("pub const profile: Field<fuwa::types::Value, NotNull>"));
    assert!(generated.contains("pub const balance: Field<fuwa::types::Decimal, NotNull>"));
    assert!(
        generated.contains("pub const balance_history: Field<Vec<fuwa::types::Decimal>, NotNull>")
    );
    assert!(generated.contains("pub balance: fuwa::types::Decimal"));
    assert!(generated.contains("pub balance_history: Vec<fuwa::types::Decimal>"));
    assert!(generated.contains("pub published_at: Option<fuwa::types::NaiveDateTime>"));
    assert!(generated.contains("pub const user_id: Field<String, NotNull>"));
    assert!(generated
        .contains("pub const shown_at: Field<fuwa::types::DateTime<fuwa::types::Utc>, NotNull>"));
    assert!(generated.contains("pub const recent_images: Field<Vec<String>, NotNull>"));
    assert!(generated.contains("pub const r#type: Field<String, NotNull>"));
    assert!(generated.contains("pub struct All"));
    assert!(generated.contains("fn into_select_items(self) -> Vec<fuwa::SelectItem>"));

    let filtered = introspect_schemas_read_only(
        &read_only_client,
        ["fuwa_codegen_it"],
        [
            TableFilter::parse("users")?,
            TableFilter::parse("fuwa_codegen_it.SwipeRecentBuffer")?,
        ],
    )
    .await?;
    let filtered_names: Vec<_> = filtered
        .tables
        .iter()
        .map(|table| table.name.as_str())
        .collect();
    assert_eq!(filtered_names, vec!["SwipeRecentBuffer", "users"]);

    client
        .batch_execute("drop schema if exists fuwa_codegen_it cascade;")
        .await?;

    Ok(())
}
