extern crate fuwa_postgres as fuwa;

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::poll_fn;
use fuwa_core::prelude::*;
use fuwa_derive::FromRow;
use fuwa_postgres::{create_pool_with_options, Dsl, Pool, PoolOptions, StreamExt};
use rust_decimal::Decimal;
use serde_json::json;
use tokio_postgres::{AsyncMessage, NoTls};

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

fn test_pool(
    database_url: &str,
    max_size: usize,
) -> std::result::Result<Pool, Box<dyn std::error::Error>> {
    Ok(create_pool_with_options(
        database_url,
        PoolOptions { max_size },
    )?)
}

#[test]
fn pool_options_reject_zero_max_size() {
    let err =
        match create_pool_with_options("host=localhost user=postgres", PoolOptions { max_size: 0 })
        {
            Ok(_) => panic!("zero-sized pools should be rejected"),
            Err(err) => err,
        };

    assert!(err
        .to_string()
        .contains("max_size must be greater than zero"));
}

#[tokio::test]
async fn enum_string_field_selects_with_text_cast_when_database_url_is_set() -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!(
            "skipping enum string projection integration test: FUWA_TEST_DATABASE_URL is not set"
        );
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
            drop schema if exists fuwa_enum_decode cascade;
            create schema fuwa_enum_decode;
            create type fuwa_enum_decode.preference_kind as enum ('MORE', 'LESS');
            create table fuwa_enum_decode.preferences (
                id bigint primary key,
                kind fuwa_enum_decode.preference_kind not null
            );
            insert into fuwa_enum_decode.preferences (id, kind) values (1, 'MORE');
            "#,
        )
        .await?;

    #[allow(non_upper_case_globals)]
    mod preferences {
        use fuwa_core::prelude::*;

        pub const table: Table = Table::new("fuwa_enum_decode", "preferences");
        pub const id: Field<i64, NotNull> = Field::new(table, "id");
        pub const kind: Field<String, NotNull> =
            Field::new_with_pg_type_and_select_cast(table, "kind", "preference_kind", "text");
    }

    let dsl = Dsl::using(&client);
    let kind = dsl
        .select(preferences::kind)
        .from(preferences::table)
        .where_(preferences::id.eq(bind(1_i64)))
        .fetch_one()
        .await?;

    assert_eq!(kind, "MORE");

    client
        .batch_execute("drop schema if exists fuwa_enum_decode cascade;")
        .await?;

    Ok(())
}

#[tokio::test]
async fn notify_none_sends_empty_payload_when_database_url_is_set() -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!("skipping NOTIFY integration test: FUWA_TEST_DATABASE_URL is not set");
        return Ok(());
    };

    let channel = "fuwa_test_notify_empty_payload";
    let (listener, mut listener_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let listen = listener.batch_execute("unlisten *; listen fuwa_test_notify_empty_payload;");
    tokio::pin!(listen);

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                result = &mut listen => break result.map_err(Box::<dyn std::error::Error>::from),
                message = poll_fn(|cx| listener_connection.poll_message(cx)) => {
                    match message {
                        Some(Ok(_)) => {}
                        Some(Err(err)) => break Err(Box::<dyn std::error::Error>::from(err)),
                        None => {
                            break Err(Box::<dyn std::error::Error>::from(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "listener connection closed",
                            )))
                        }
                    }
                }
            }
        }
    })
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out preparing notification listener",
        )
    })??;

    let (sender, sender_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = sender_connection.await {
            eprintln!("PostgreSQL notification sender connection task failed: {err}");
        }
    });

    Dsl::using(&sender).notify(channel, None).await?;

    let notification = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match poll_fn(|cx| listener_connection.poll_message(cx)).await {
                Some(Ok(AsyncMessage::Notification(notification)))
                    if notification.channel() == channel =>
                {
                    break Ok(notification)
                }
                Some(Ok(_)) => {}
                Some(Err(err)) => break Err(Box::<dyn std::error::Error>::from(err)),
                None => {
                    break Err(Box::<dyn std::error::Error>::from(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "listener connection closed",
                    )))
                }
            }
        }
    })
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out waiting for notification",
        )
    })??;

    assert_eq!(notification.payload(), "");
    Ok(())
}

#[derive(Debug, PartialEq, Eq, FromRow)]
struct DerivedAccount {
    id: i64,
    email: String,
    display_name: Option<String>,
}

#[allow(non_upper_case_globals)]
mod users {
    use fuwa_core::prelude::*;

    pub const table: Table = Table::new("public", "fuwa_test_users");
    pub const id: Field<i64, NotNull> = Field::new(table, "id");
    pub const email: Field<String, NotNull> = Field::new(table, "email");
    pub const active: Field<bool, NotNull> = Field::new(table, "active");
}

#[allow(non_upper_case_globals)]
mod pool_users {
    use fuwa_core::prelude::*;

    pub const table: Table = Table::new("public", "fuwa_test_pool_users");
    pub const id: Field<i64, NotNull> = Field::new(table, "id");
    pub const email: Field<String, NotNull> = Field::new(table, "email");
    pub const active: Field<bool, NotNull> = Field::new(table, "active");
}

#[allow(non_upper_case_globals)]
mod acquire_users {
    use fuwa_core::prelude::*;

    pub const table: Table = Table::new("public", "fuwa_test_acquire_users");
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

    let dsl = Dsl::using(&client);

    let sixteen_column_row = dsl
        .raw(
            r#"
            select
                1::bigint, 2::bigint, 3::bigint, 4::bigint,
                5::bigint, 6::bigint, 7::bigint, 8::bigint,
                9::bigint, 10::bigint, 11::bigint, 12::bigint,
                13::bigint, 14::bigint, 15::bigint, 16::bigint
            "#,
        )
        .fetch_one_as::<(
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
        )>()
        .await?;
    let (c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16) =
        sixteen_column_row;
    assert_eq!(
        [c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16,],
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    );

    let inserted_id = dsl
        .insert_into(users::table)
        .values((
            users::email.set(bind("a@example.com")),
            users::active.set(bind(true)),
        ))
        .returning(users::id)
        .fetch_one()
        .await?;

    let rows = dsl
        .select((users::id, users::email))
        .from(users::table)
        .where_(users::id.eq(bind(inserted_id)))
        .fetch_all()
        .await?;
    assert_eq!(rows, vec![(inserted_id, "a@example.com".to_owned())]);

    let missing = dsl
        .select(users::id)
        .from(users::table)
        .where_(users::email.eq(bind("missing@example.com")))
        .fetch_optional()
        .await?;
    assert_eq!(missing, None);

    let updated = dsl
        .update(users::table)
        .set(users::email.set(bind("new@example.com")))
        .where_(users::id.eq(bind(inserted_id)))
        .returning(users::email)
        .fetch_one()
        .await?;
    assert_eq!(updated, "new@example.com");

    let deleted = dsl
        .delete_from(users::table)
        .where_(users::id.eq(bind(inserted_id)))
        .execute()
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

    let dsl = Dsl::using(&client);

    let inserted = dsl
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
        .execute()
        .await?;
    assert_eq!(inserted, 2);

    let rows = dsl
        .select((upsert_users::id, upsert_users::email))
        .from(upsert_users::table)
        .order_by(upsert_users::id.asc())
        .fetch_all()
        .await?;
    assert_eq!(
        rows,
        vec![
            (1, "ada@example.com".to_owned()),
            (2, "ben@example.com".to_owned()),
        ]
    );

    let ignored = dsl
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
        .fetch_optional()
        .await?;
    assert_eq!(ignored, None);

    let unchanged = dsl
        .select((upsert_users::display_name, upsert_users::active))
        .from(upsert_users::table)
        .where_(upsert_users::email.eq(bind("ada@example.com")))
        .fetch_one()
        .await?;
    assert_eq!(unchanged, (Some("Ada".to_owned()), true));

    let updated = dsl
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
        .fetch_one()
        .await?;
    assert_eq!(updated, (2, Some("Benedict".to_owned()), true));
    drop(dsl);

    let mut dsl = Dsl::using(&mut client);
    let committed_id = dsl
        .transaction(async |tx| {
            tx.insert_into(upsert_users::table)
                .values((
                    upsert_users::id.set(bind(5_i64)),
                    upsert_users::email.set(bind("committed@example.com")),
                    upsert_users::display_name.set(bind(Some("Committed"))),
                    upsert_users::active.set(bind(true)),
                ))
                .returning(upsert_users::id)
                .fetch_one()
                .await
        })
        .await?;
    assert_eq!(committed_id, 5);
    drop(dsl);

    let dsl = Dsl::using(&client);
    let committed_count = dsl
        .select(count_star())
        .from(upsert_users::table)
        .where_(upsert_users::email.eq(bind("committed@example.com")))
        .fetch_one()
        .await?;
    assert_eq!(committed_count, 1);
    drop(dsl);

    let mut dsl = Dsl::using(&mut client);
    let tx_result = dsl
        .transaction(async |tx| {
            tx.insert_into(upsert_users::table)
                .values((
                    upsert_users::id.set(bind(6_i64)),
                    upsert_users::email.set(bind("rolled-back@example.com")),
                    upsert_users::display_name.set(bind(Some("Rollback"))),
                    upsert_users::active.set(bind(true)),
                ))
                .execute()
                .await?;

            tx.insert_into(upsert_users::table)
                .values((
                    upsert_users::id.set(bind(1_i64)),
                    upsert_users::email.set(bind("duplicate-id@example.com")),
                    upsert_users::display_name.set(bind(None::<String>)),
                    upsert_users::active.set(bind(true)),
                ))
                .execute()
                .await?;

            Ok(())
        })
        .await;
    assert!(tx_result.is_err());
    drop(dsl);

    let dsl = Dsl::using(&client);
    let rolled_back_count = dsl
        .select(count_star())
        .from(upsert_users::table)
        .where_(upsert_users::email.eq(bind("rolled-back@example.com")))
        .fetch_one()
        .await?;
    assert_eq!(rolled_back_count, 0);

    client
        .batch_execute("drop table if exists public.fuwa_test_upsert_users;")
        .await?;

    Ok(())
}

#[tokio::test]
async fn derive_from_row_decodes_custom_struct_by_column_name_when_database_url_is_set(
) -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!("skipping FromRow derive integration test: FUWA_TEST_DATABASE_URL is not set");
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
            drop table if exists public.fuwa_test_derive_accounts;
            create table public.fuwa_test_derive_accounts (
                id bigint primary key,
                email text not null,
                display_name text
            );

            insert into public.fuwa_test_derive_accounts (id, email, display_name)
            values (1, 'derive@example.com', 'Derived');
            "#,
        )
        .await?;

    let dsl = Dsl::using(&client);
    let account = dsl
        .raw(
            r#"
        select display_name, email, id
        from public.fuwa_test_derive_accounts
        where id = $1
        "#,
        )
        .bind(1_i64)
        .fetch_one_as::<DerivedAccount>()
        .await?;

    assert_eq!(
        account,
        DerivedAccount {
            id: 1,
            email: "derive@example.com".to_owned(),
            display_name: Some("Derived".to_owned()),
        }
    );

    client
        .batch_execute("drop table if exists public.fuwa_test_derive_accounts;")
        .await?;

    Ok(())
}

#[tokio::test]
async fn fetch_stream_and_chunked_use_postgres_portals_when_database_url_is_set() -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!(
            "skipping PostgreSQL streaming integration test: FUWA_TEST_DATABASE_URL is not set"
        );
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
            drop table if exists public.fuwa_test_stream_rows;
            create table public.fuwa_test_stream_rows (
                id bigint primary key,
                label text not null
            );

            insert into public.fuwa_test_stream_rows (id, label)
            select value, 'stream-' || value::text
            from generate_series(1, 10000) as value;
            "#,
        )
        .await?;

    let transaction = client.transaction().await?;
    let dsl = Dsl::using(&transaction);
    let mut rows = dsl
        .raw(
            r#"
        select id, label
        from public.fuwa_test_stream_rows
        order by id
        "#,
        )
        .fetch_stream_as::<(i64, String)>()
        .await?;

    let mut count = 0_i64;
    while let Some(row) = rows.next().await {
        let (id, label) = row?;
        count += 1;

        assert_eq!(id, count);
        assert_eq!(label, format!("stream-{count}"));
    }
    assert_eq!(count, 10_000);
    drop(rows);
    drop(dsl);
    transaction.commit().await?;

    let transaction = client.transaction().await?;
    let dsl = Dsl::using(&transaction);
    let mut chunks = dsl
        .raw(
            r#"
        select id, label
        from public.fuwa_test_stream_rows
        order by id
        "#,
        )
        .fetch_chunked_as::<(i64, String)>(777)
        .await?;

    let mut total = 0_i64;
    let mut chunk_count = 0_usize;
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        chunk_count += 1;

        let chunk_len = chunk.len();

        if total + (chunk_len as i64) < 10_000 {
            assert_eq!(chunk.len(), 777);
        } else {
            assert_eq!(chunk.len(), 676);
        }

        for (offset, (id, label)) in chunk.into_iter().enumerate() {
            let expected = total + offset as i64 + 1;
            assert_eq!(id, expected);
            assert_eq!(label, format!("stream-{expected}"));
        }

        total += chunk_len as i64;
    }
    assert_eq!(total, 10_000);
    assert_eq!(chunk_count, 13);
    drop(chunks);
    drop(dsl);
    transaction.commit().await?;

    client
        .batch_execute("drop table if exists public.fuwa_test_stream_rows;")
        .await?;

    Ok(())
}

#[tokio::test]
async fn mutable_transaction_executor_runs_queries_when_database_url_is_set() -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!("skipping mutable transaction executor integration test: FUWA_TEST_DATABASE_URL is not set");
        return Ok(());
    };

    let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("PostgreSQL connection task failed: {err}");
        }
    });

    let mut transaction = client.transaction().await?;
    let mut dsl = Dsl::using(&mut transaction);

    let value = dsl.raw("select 42::bigint").fetch_one_as::<i64>().await?;
    assert_eq!(value, 42);

    let nested_value = dsl
        .transaction(async |tx| tx.raw("select 43::bigint").fetch_one_as::<i64>().await)
        .await?;
    assert_eq!(nested_value, 43);

    drop(dsl);
    transaction.rollback().await?;

    Ok(())
}

#[tokio::test]
async fn pool_dsl_context_acquires_on_execution_and_can_be_shared_when_database_url_is_set(
) -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!("skipping pool executor integration test: FUWA_TEST_DATABASE_URL is not set");
        return Ok(());
    };

    let pool = test_pool(&database_url, 4)?;
    let dsl = Dsl::using(pool.clone());
    let shared = Arc::new(dsl.clone());

    let mut handles = Vec::new();
    for expected in 0_i64..100 {
        let dsl = Arc::clone(&shared);
        handles.push(tokio::spawn(async move {
            dsl.raw("select $1::bigint")
                .bind(expected)
                .fetch_one_as::<i64>()
                .await
        }));
    }

    for (expected, handle) in (0_i64..100).zip(handles) {
        let actual = handle.await??;
        assert_eq!(actual, expected);
    }

    let status = pool.status();
    assert_eq!(status.max_size, 4);
    assert!(status.size <= status.max_size);

    let (first_pid, second_pid) = shared
        .with_connection(async |dsl| {
            let first_pid = dsl
                .raw("select pg_backend_pid()")
                .fetch_one_as::<i32>()
                .await?;
            let second_pid = dsl
                .raw("select pg_backend_pid()")
                .fetch_one_as::<i32>()
                .await?;
            Ok((first_pid, second_pid))
        })
        .await?;
    assert_eq!(first_pid, second_pid);

    // pool-level streaming is now a compile-time error via the
    // `TransactionalExecutor` bound on `fetch_stream` / `fetch_chunked`,
    // so there's nothing to assert at runtime here.

    Ok(())
}

#[tokio::test]
async fn pool_transaction_callback_uses_dsl_context_when_database_url_is_set() -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!("skipping pool transaction integration test: FUWA_TEST_DATABASE_URL is not set");
        return Ok(());
    };

    let pool = test_pool(&database_url, 4)?;
    {
        let client = pool.get().await?;
        client
            .batch_execute(
                r#"
                drop table if exists public.fuwa_test_pool_users;
                create table public.fuwa_test_pool_users (
                    id bigint primary key,
                    email text not null unique,
                    active boolean not null
                );
                "#,
            )
            .await?;
    }

    let dsl = Dsl::using(pool.clone());
    let committed_id = dsl
        .transaction(async |dsl| {
            dsl.insert_into(pool_users::table)
                .values((
                    pool_users::id.set(bind(1_i64)),
                    pool_users::email.set(bind("pool-committed@example.com")),
                    pool_users::active.set(bind(true)),
                ))
                .returning(pool_users::id)
                .fetch_one()
                .await
        })
        .await?;
    assert_eq!(committed_id, 1);

    let committed_count = dsl
        .select(count_star())
        .from(pool_users::table)
        .where_(pool_users::email.eq(bind("pool-committed@example.com")))
        .fetch_one()
        .await?;
    assert_eq!(committed_count, 1);

    let transaction_result = dsl
        .transaction(async |dsl| {
            dsl.insert_into(pool_users::table)
                .values((
                    pool_users::id.set(bind(2_i64)),
                    pool_users::email.set(bind("pool-rolled-back@example.com")),
                    pool_users::active.set(bind(true)),
                ))
                .execute()
                .await?;

            dsl.insert_into(pool_users::table)
                .values((
                    pool_users::id.set(bind(1_i64)),
                    pool_users::email.set(bind("pool-duplicate@example.com")),
                    pool_users::active.set(bind(true)),
                ))
                .execute()
                .await?;

            Ok(())
        })
        .await;
    assert!(transaction_result.is_err());

    let rolled_back_count = dsl
        .select(count_star())
        .from(pool_users::table)
        .where_(pool_users::email.eq(bind("pool-rolled-back@example.com")))
        .fetch_one()
        .await?;
    assert_eq!(rolled_back_count, 0);

    {
        let client = pool.get().await?;
        client
            .batch_execute("drop table if exists public.fuwa_test_pool_users;")
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn pool_acquire_begin_commit_round_trips_when_database_url_is_set() -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!(
            "skipping pool acquire+begin integration test: FUWA_TEST_DATABASE_URL is not set"
        );
        return Ok(());
    };

    let pool = test_pool(&database_url, 4)?;
    {
        let client = pool.get().await?;
        client
            .batch_execute(
                r#"
                drop table if exists public.fuwa_test_acquire_users;
                create table public.fuwa_test_acquire_users (
                    id bigint primary key,
                    email text not null unique,
                    active boolean not null
                );
                "#,
            )
            .await?;
    }

    let dsl = Dsl::using(pool.clone());
    let mut conn = dsl.acquire().await?;

    // Commit path.
    let tx = conn.begin().await?;
    let inserted: i64 = tx
        .dsl()
        .insert_into(acquire_users::table)
        .values((
            acquire_users::id.set(bind(1_i64)),
            acquire_users::email.set(bind("commit@example.com")),
            acquire_users::active.set(bind(true)),
        ))
        .returning(acquire_users::id)
        .fetch_one()
        .await?;
    assert_eq!(inserted, 1);
    tx.commit().await?;

    let committed_count: i64 = dsl
        .select(count_star())
        .from(acquire_users::table)
        .where_(acquire_users::email.eq(bind("commit@example.com")))
        .fetch_one()
        .await?;
    assert_eq!(committed_count, 1);

    // Explicit rollback path.
    let tx = conn.begin().await?;
    tx.dsl()
        .insert_into(acquire_users::table)
        .values((
            acquire_users::id.set(bind(2_i64)),
            acquire_users::email.set(bind("rollback@example.com")),
            acquire_users::active.set(bind(true)),
        ))
        .execute()
        .await?;
    tx.rollback().await?;

    let rolled_back_count: i64 = dsl
        .select(count_star())
        .from(acquire_users::table)
        .where_(acquire_users::email.eq(bind("rollback@example.com")))
        .fetch_one()
        .await?;
    assert_eq!(rolled_back_count, 0);

    // Drop = rollback path.
    {
        let tx = conn.begin().await?;
        tx.dsl()
            .insert_into(acquire_users::table)
            .values((
                acquire_users::id.set(bind(3_i64)),
                acquire_users::email.set(bind("dropped@example.com")),
                acquire_users::active.set(bind(true)),
            ))
            .execute()
            .await?;
        // drop without commit — tokio-postgres rollbacks on drop
    }

    // Re-acquire to ensure the previous drop-rollback was actually flushed
    // before we read.
    drop(conn);
    let dropped_count: i64 = dsl
        .select(count_star())
        .from(acquire_users::table)
        .where_(acquire_users::email.eq(bind("dropped@example.com")))
        .fetch_one()
        .await?;
    assert_eq!(dropped_count, 0);

    {
        let client = pool.get().await?;
        client
            .batch_execute("drop table if exists public.fuwa_test_acquire_users;")
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn priority_features_round_trip_when_database_url_is_set() -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!("skipping priority feature integration test: FUWA_TEST_DATABASE_URL is not set");
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
            drop schema if exists fuwa_priority cascade;
            create schema fuwa_priority;
            create table fuwa_priority.events (
                id bigint primary key,
                account_id bigint not null,
                amount integer not null,
                created_at timestamptz not null default now()
            );
            create table fuwa_priority.archive (
                id bigint primary key,
                account_id bigint not null,
                amount integer not null
            );
            create table fuwa_priority.jobs (
                id bigint primary key,
                status text not null,
                created_at timestamptz not null default now()
            );
            create table fuwa_priority.bulk (
                id bigint primary key,
                label text not null,
                active boolean not null
            );
            create table fuwa_priority.bulk_json (
                id bigint primary key,
                doc_json json not null,
                doc_jsonb jsonb not null,
                optional_doc json
            );
            create table fuwa_priority.bulk_arrays (
                id bigint primary key,
                labels varchar(12)[] not null,
                codes character(1)[] not null
            );
            create table fuwa_priority.uniq (
                email text primary key,
                rank integer not null
            );
            insert into fuwa_priority.events (id, account_id, amount) values
                (1, 100, 10), (2, 100, 20), (3, 100, 30),
                (4, 200, 40), (5, 200, 50);
            insert into fuwa_priority.jobs (id, status) values
                (1, 'queued'), (2, 'queued'), (3, 'done');
            "#,
        )
        .await?;

    #[allow(non_upper_case_globals)]
    mod events {
        use fuwa_core::prelude::*;
        pub const table: Table = Table::new("fuwa_priority", "events");
        pub const id: Field<i64, NotNull> = Field::new(table, "id");
        pub const account_id: Field<i64, NotNull> = Field::new(table, "account_id");
        pub const amount: Field<i32, NotNull> = Field::new(table, "amount");
    }
    #[allow(non_upper_case_globals)]
    mod archive {
        use fuwa_core::prelude::*;
        pub const table: Table = Table::new("fuwa_priority", "archive");
        pub const id: Field<i64, NotNull> = Field::new(table, "id");
        pub const account_id: Field<i64, NotNull> = Field::new(table, "account_id");
        pub const amount: Field<i32, NotNull> = Field::new(table, "amount");
    }
    #[allow(non_upper_case_globals)]
    mod jobs {
        use fuwa_core::prelude::*;
        pub const table: Table = Table::new("fuwa_priority", "jobs");
        pub const id: Field<i64, NotNull> = Field::new(table, "id");
        pub const status: Field<String, NotNull> = Field::new(table, "status");
    }
    #[allow(non_upper_case_globals)]
    mod bulk {
        use fuwa_core::prelude::*;
        pub const table: Table = Table::new("fuwa_priority", "bulk");
        pub const id: Field<i64, NotNull> = Field::new(table, "id");
        pub const label: Field<String, NotNull> = Field::new(table, "label");
        pub const active: Field<bool, NotNull> = Field::new(table, "active");
    }
    #[allow(non_upper_case_globals)]
    mod bulk_json {
        use fuwa_core::prelude::*;
        pub const table: Table = Table::new("fuwa_priority", "bulk_json");
        pub const id: Field<i64, NotNull> = Field::new(table, "id");
        pub const doc_json: Field<serde_json::Value, NotNull> =
            Field::new_with_pg_type(table, "doc_json", "json");
        pub const doc_jsonb: Field<serde_json::Value, NotNull> =
            Field::new_with_pg_type(table, "doc_jsonb", "jsonb");
        pub const optional_doc: Field<serde_json::Value, Nullable> =
            Field::new_with_pg_type(table, "optional_doc", "json");
    }
    #[allow(non_upper_case_globals)]
    mod bulk_arrays {
        use fuwa_core::prelude::*;
        pub const table: Table = Table::new("fuwa_priority", "bulk_arrays");
        pub const id: Field<i64, NotNull> = Field::new(table, "id");
        pub const labels: Field<Vec<String>, NotNull> =
            Field::new_with_pg_type(table, "labels", "_varchar");
        pub const codes: Field<Vec<String>, NotNull> =
            Field::new_with_pg_type(table, "codes", "_bpchar");
    }
    #[allow(non_upper_case_globals)]
    mod uniq {
        use fuwa_core::prelude::*;
        pub const table: Table = Table::new("fuwa_priority", "uniq");
        pub const email: Field<String, NotNull> = Field::new(table, "email");
        pub const rank: Field<i32, NotNull> = Field::new(table, "rank");
    }

    // window function: top-1-event-per-account using an inline filter
    // (avoid the AliasedSubquery + window-output-column path which hits a
    // postgres planner quirk we don't model yet).
    let dsl = Dsl::using(&client);

    let max_per_account = dsl
        .select((events::account_id, max(events::amount)))
        .from(events::table)
        .group_by(events::account_id)
        .order_by(events::account_id.asc())
        .fetch_all()
        .await?;
    assert_eq!(max_per_account, vec![(100, Some(30)), (200, Some(50))]);

    // direct window function in a SELECT (no alias subquery)
    let event_ranks = dsl
        .select((
            events::id,
            events::account_id,
            row_number()
                .over(partition_by(events::account_id).order_by(events::amount.desc()))
                .as_("rn"),
        ))
        .from(events::table)
        .order_by((events::account_id.asc(), events::amount.desc()))
        .fetch_all()
        .await?;
    assert_eq!(event_ranks.len(), 5);
    assert_eq!(event_ranks[0], (3, 100, 1));
    assert_eq!(event_ranks[3], (5, 200, 1));

    // set operation: union of two filtered queries
    let q1 = dsl
        .select(events::id)
        .from(events::table)
        .where_(events::account_id.eq(bind(100_i64)));
    let q2 = dsl
        .select(events::id)
        .from(events::table)
        .where_(events::account_id.eq(bind(200_i64)));
    let mut all_ids = q1.union(q2).order_by(events::id.asc()).fetch_all().await?;
    all_ids.sort();
    assert_eq!(all_ids, vec![1, 2, 3, 4, 5]);

    // for update + skip locked: lock-and-claim pattern
    let dsl_arc = std::sync::Arc::new(Dsl::using(pool_for(&database_url, 4).await?));
    let claim = {
        let dsl = std::sync::Arc::clone(&dsl_arc);
        async move {
            dsl.transaction(async |tx| {
                let claimed = tx
                    .select(jobs::id)
                    .from(jobs::table)
                    .where_(jobs::status.eq(bind("queued")))
                    .order_by(jobs::id.asc())
                    .limit(1)
                    .for_update()
                    .skip_locked()
                    .fetch_optional()
                    .await?;
                if let Some(id) = claimed {
                    tx.update(jobs::table)
                        .set(jobs::status.set(bind("running")))
                        .where_(jobs::id.eq(bind(id)))
                        .execute()
                        .await?;
                }
                Ok::<_, fuwa::Error>(claimed)
            })
            .await
        }
    };
    let first = claim.await?;
    assert_eq!(first, Some(1));

    // insert ... select
    let dsl = Dsl::using(&client);
    let archived = dsl
        .insert_into(archive::table)
        .columns((archive::id, archive::account_id, archive::amount))
        .from_select(
            dsl.select((events::id, events::account_id, events::amount))
                .from(events::table)
                .where_(events::amount.ge(bind(30_i32))),
        )
        .execute()
        .await?;
    assert_eq!(archived, 3);

    // update ... from
    let bumped = dsl
        .update(archive::table)
        .set(archive::amount.set(events::amount.expr() + bind(1_i32)))
        .from(events::table)
        .where_(archive::id.eq(events::id).and(archive::id.eq(bind(3_i64))))
        .returning((archive::id, archive::amount))
        .fetch_one()
        .await?;
    assert_eq!(bumped, (3, 31));

    // delete ... using
    let mut deleted = dsl
        .delete_from(archive::table)
        .using(events::table)
        .where_(
            archive::id
                .eq(events::id)
                .and(events::account_id.eq(bind(200_i64))),
        )
        .returning(archive::id)
        .fetch_all()
        .await?;
    deleted.sort_unstable();
    assert_eq!(deleted, vec![4, 5]);

    // copy_in_binary
    let copied = dsl
        .copy_in_binary(
            bulk::table,
            (bulk::id, bulk::label, bulk::active),
            async |writer| {
                for i in 1_i64..=100 {
                    writer.send((i, format!("row-{i}"), i % 2 == 0)).await?;
                }
                Ok(())
            },
        )
        .await?;
    assert_eq!(copied, 100);
    let bulk_count = dsl
        .select(count_star())
        .from(bulk::table)
        .fetch_one()
        .await?;
    assert_eq!(bulk_count, 100);
    let copied_json = dsl
        .copy_in_binary(
            bulk_json::table,
            (
                bulk_json::id,
                bulk_json::doc_json,
                bulk_json::doc_jsonb,
                bulk_json::optional_doc,
            ),
            async |writer| {
                writer
                    .send((
                        1_i64,
                        json!({"format": "json"}),
                        json!({"format": "jsonb"}),
                        None::<serde_json::Value>,
                    ))
                    .await?;
                writer
                    .send((
                        2_i64,
                        json!({"n": 2}),
                        json!({"n": 2, "indexed": true}),
                        Some(json!({"present": true})),
                    ))
                    .await?;
                Ok(())
            },
        )
        .await?;
    assert_eq!(copied_json, 2);
    let bulk_json_count = dsl
        .select(count_star())
        .from(bulk_json::table)
        .fetch_one()
        .await?;
    assert_eq!(bulk_json_count, 2);
    let copied_arrays = dsl
        .copy_in_binary(
            bulk_arrays::table,
            (bulk_arrays::id, bulk_arrays::labels, bulk_arrays::codes),
            async |writer| {
                writer
                    .send((
                        1_i64,
                        vec!["alpha".to_owned(), "beta".to_owned()],
                        vec!["A".to_owned(), "B".to_owned()],
                    ))
                    .await?;
                writer
                    .send((2_i64, vec!["gamma".to_owned()], vec!["C".to_owned()]))
                    .await?;
                Ok(())
            },
        )
        .await?;
    assert_eq!(copied_arrays, 2);
    let bulk_arrays_count = dsl
        .select(count_star())
        .from(bulk_arrays::table)
        .fetch_one()
        .await?;
    assert_eq!(bulk_arrays_count, 2);
    let copy_dup_err = dsl
        .copy_in_binary(
            bulk::table,
            (bulk::id, bulk::label, bulk::active),
            async |writer| {
                for i in 0..256 {
                    writer
                        .send((1_i64, format!("duplicate-{i:03}"), true))
                        .await?;
                }
                Ok(())
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(copy_dup_err, fuwa::Error::UniqueViolation { .. }),
        "expected UniqueViolation from COPY, got: {copy_dup_err:?}"
    );

    // error taxonomy: unique violation maps to typed variant
    dsl.insert_into(uniq::table)
        .values((uniq::email.set(bind("a")), uniq::rank.set(bind(1_i32))))
        .execute()
        .await?;
    let dup_err = dsl
        .insert_into(uniq::table)
        .values((uniq::email.set(bind("a")), uniq::rank.set(bind(2_i32))))
        .execute()
        .await
        .unwrap_err();
    assert!(
        matches!(dup_err, fuwa::Error::UniqueViolation { .. }),
        "expected UniqueViolation, got: {dup_err:?}"
    );

    // dynamic composition: Condition::all over a filter list
    let mut filters: Vec<Condition> = Vec::new();
    filters.push(events::account_id.eq(bind(100_i64)));
    filters.push(events::amount.gt(bind(15_i32)));
    let count = dsl
        .select(count_star())
        .from(events::table)
        .filter(Condition::all(filters))
        .fetch_one()
        .await?;
    assert_eq!(count, 2);

    client
        .batch_execute("drop schema if exists fuwa_priority cascade;")
        .await?;

    Ok(())
}

async fn pool_for(
    database_url: &str,
    max_size: usize,
) -> std::result::Result<Pool, Box<dyn std::error::Error>> {
    Ok(create_pool_with_options(
        database_url,
        PoolOptions { max_size },
    )?)
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

    let dsl = Dsl::using(&client);

    let active_accounts = Table::unqualified("active_accounts");
    let active_account_id = active_accounts.field_of(accounts::id);
    let active_account_email = active_accounts.field_of(accounts::email);

    let active_accounts_cte = dsl
        .select((accounts::id, accounts::email))
        .from(accounts::table)
        .where_(accounts::active.eq(bind(true)));
    let active_account_posts = dsl
        .with("active_accounts", active_accounts_cte)
        .select((active_account_id, active_account_email, posts::title))
        .from(active_accounts)
        .join(posts::table.on(posts::account_id.eq(active_account_id)))
        .where_(posts::published.eq(bind(true)))
        .order_by((active_account_id.asc(), posts::id.asc()))
        .render()?;

    assert_eq!(
        active_account_posts.sql(),
        concat!(
            r#"with "active_accounts" as (select "accounts"."id", "accounts"."email" "#,
            r#"from "fuwa_it_complex"."accounts" where ("accounts"."active" = $1)) "#,
            r#"select "active_accounts"."id", "active_accounts"."email", "posts"."title" "#,
            r#"from "active_accounts" join "fuwa_it_complex"."posts" "#,
            r#"on ("posts"."account_id" = "active_accounts"."id") "#,
            r#"where ("posts"."published" = $2) "#,
            r#"order by "active_accounts"."id" asc, "posts"."id" asc"#
        )
    );

    let active_accounts_cte = dsl
        .select((accounts::id, accounts::email))
        .from(accounts::table)
        .where_(accounts::active.eq(bind(true)));
    let active_account_posts = dsl
        .with("active_accounts", active_accounts_cte)
        .select((active_account_id, active_account_email, posts::title))
        .from(active_accounts)
        .join(posts::table.on(posts::account_id.eq(active_account_id)))
        .where_(posts::published.eq(bind(true)))
        .order_by((active_account_id.asc(), posts::id.asc()))
        .fetch_all()
        .await?;

    assert_eq!(
        active_account_posts,
        vec![
            (1, "ada@example.com".to_owned(), "Rust DSLs".to_owned()),
            (3, "cy@example.com".to_owned(), "Fuwa roadmap".to_owned()),
        ]
    );

    let published_for_active_accounts = dsl
        .select((accounts::email, posts::title, posts::score))
        .from(accounts::table)
        .join(posts::table.on(posts::account_id.eq(accounts::id)))
        .where_(
            accounts::active
                .eq(bind(true))
                .and(posts::published.eq(bind(true))),
        )
        .order_by((accounts::id.asc(), posts::id.asc()))
        .fetch_all()
        .await?;

    assert_eq!(
        published_for_active_accounts,
        vec![
            ("ada@example.com".to_owned(), "Rust DSLs".to_owned(), 42),
            ("cy@example.com".to_owned(), "Fuwa roadmap".to_owned(), 77),
        ]
    );

    let active_accounts_with_optional_published_posts = dsl
        .select((accounts::email, nullable(posts::title)))
        .from(accounts::table)
        .left_join(
            posts::table.on(posts::account_id
                .eq(accounts::id)
                .and(posts::published.eq(bind(true)))),
        )
        .where_(accounts::active.eq(bind(true)))
        .order_by(accounts::id.asc())
        .fetch_all()
        .await?;

    assert_eq!(
        active_accounts_with_optional_published_posts,
        vec![
            ("ada@example.com".to_owned(), Some("Rust DSLs".to_owned())),
            ("cy@example.com".to_owned(), Some("Fuwa roadmap".to_owned())),
            ("dana@example.com".to_owned(), None),
        ]
    );

    let accounts_without_display_name = dsl
        .select((accounts::id, accounts::display_name))
        .from(accounts::table)
        .where_(accounts::display_name.is_null())
        .order_by(accounts::id.asc())
        .fetch_all()
        .await?;

    assert_eq!(
        accounts_without_display_name,
        vec![(2, None), (3, None), (4, None)]
    );

    let high_rank_accounts = dsl
        .select(accounts::email)
        .from(accounts::table)
        .where_(accounts::signup_rank.ge(bind(30_i32)))
        .order_by(accounts::signup_rank.desc())
        .fetch_all()
        .await?;

    assert_eq!(
        high_rank_accounts,
        vec!["dana@example.com".to_owned(), "cy@example.com".to_owned()]
    );

    let ada_balance = dsl
        .select(accounts::account_balance)
        .from(accounts::table)
        .where_(accounts::email.eq(bind("ada@example.com")))
        .fetch_one()
        .await?;

    assert_eq!(ada_balance, Decimal::new(1025, 2));

    let active_balance_total = dsl
        .select(sum(accounts::account_balance))
        .from(accounts::table)
        .where_(accounts::active.eq(bind(true)))
        .fetch_one()
        .await?;

    assert_eq!(active_balance_total, Some(Decimal::new(8050, 2)));

    let filtered_accounts = dsl
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
        .fetch_all()
        .await?;

    assert_eq!(
        filtered_accounts,
        vec!["ada@example.com".to_owned(), "cy@example.com".to_owned()]
    );

    let adjusted_scores = dsl
        .select((posts::id, posts::score.expr() + bind(8_i32)))
        .from(posts::table)
        .where_(posts::id.in_([bind(10_i64), bind(30_i64)]))
        .order_by(posts::id.asc())
        .fetch_all()
        .await?;

    assert_eq!(adjusted_scores, vec![(10, 50), (30, 85)]);

    let account_labels = dsl
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
        .fetch_all()
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

    let inserted_balance = dsl
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
        .fetch_one()
        .await?;

    assert_eq!(inserted_balance, Decimal::new(5050, 2));

    let posts_without_body = dsl
        .select((posts::id, posts::body))
        .from(posts::table)
        .where_(posts::body.is_null())
        .order_by(posts::id.asc())
        .fetch_all()
        .await?;

    assert_eq!(posts_without_body, vec![(11, None), (30, None)]);

    let published_post_count = dsl
        .select(count_star())
        .from(posts::table)
        .where_(posts::published.eq(bind(true)))
        .fetch_one()
        .await?;

    assert_eq!(published_post_count, 3);

    let post_counts_by_account = dsl
        .select((posts::account_id, count_star()))
        .from(posts::table)
        .group_by(posts::account_id)
        .having(count_star().gt(bind(1_i64)))
        .order_by(posts::account_id.asc())
        .fetch_all()
        .await?;

    assert_eq!(post_counts_by_account, vec![(1, 2)]);

    let inserted_comment = dsl
        .insert_into(comments::table)
        .values((
            comments::id.set(bind(301_i64)),
            comments::post_id.set(bind(30_i64)),
            comments::body.set(bind("late addition")),
        ))
        .returning((comments::id, comments::body))
        .fetch_one()
        .await?;

    assert_eq!(inserted_comment, (301, "late addition".to_owned()));

    let updated_post = dsl
        .update(posts::table)
        .set(posts::score.set(bind(100_i32)))
        .where_(posts::id.eq(bind(10_i64)))
        .returning((posts::id, posts::score))
        .fetch_one()
        .await?;

    assert_eq!(updated_post, (10, 100));

    let deleted_draft_comments = dsl
        .delete_from(comments::table)
        .where_(comments::post_id.eq(bind(11_i64)))
        .execute()
        .await?;

    assert_eq!(deleted_draft_comments, 1);

    let remaining_comment_count = dsl
        .select(count_star())
        .from(comments::table)
        .fetch_one()
        .await?;

    assert_eq!(remaining_comment_count, 4);

    let recent_images = dsl
        .raw(
            r#"select "recentImages"
           from fuwa_it_complex.recent_buffers
           where "userId" = $1"#,
        )
        .bind("ada")
        .fetch_one_as::<Vec<String>>()
        .await?;

    assert_eq!(
        recent_images,
        vec!["img-a.jpg".to_owned(), "img-b.jpg".to_owned()]
    );

    let selected_emails = dsl
        .raw(
            r#"select email
           from fuwa_it_complex.accounts
           where email = any($1)
           order by email"#,
        )
        .bind(vec![
            "cy@example.com".to_owned(),
            "ada@example.com".to_owned(),
        ])
        .fetch_all_as::<String>()
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
