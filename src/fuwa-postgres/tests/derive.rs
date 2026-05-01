extern crate fuwa_postgres as fuwa;

use fuwa_derive::FromRow;
use fuwa_postgres::Dsl;
use tokio_postgres::NoTls;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

#[derive(Debug, PartialEq, Eq, FromRow)]
struct PlainAccount {
    id: i64,
    email: String,
}

#[derive(Debug, PartialEq, Eq, FromRow)]
struct RenamedAccount {
    #[fuwa(rename = "id")]
    account_id: i64,
    #[fuwa(rename = "email")]
    contact: String,
}

#[derive(Debug, PartialEq, Eq, FromRow)]
#[fuwa(rename_all = "camelCase")]
struct CamelAccount {
    user_id: i64,
    primary_email: String,
}

#[derive(Debug, PartialEq, Eq, FromRow)]
struct SkippedAccount {
    id: i64,
    email: String,
    #[fuwa(skip)]
    cached: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, FromRow)]
struct DefaultedAccount {
    id: i64,
    email: String,
    #[fuwa(default)]
    label: String,
}

#[derive(Debug, PartialEq, Eq, FromRow)]
struct AuditFields {
    id: i64,
    email: String,
}

#[derive(Debug, PartialEq, Eq, FromRow)]
struct FlattenedAccount {
    #[fuwa(flatten)]
    audit: AuditFields,
    active: bool,
}

#[tokio::test]
async fn fromrow_attrs_round_trip_when_database_url_is_set() -> TestResult {
    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {
        eprintln!("skipping derive integration test: FUWA_TEST_DATABASE_URL is not set");
        return Ok(());
    };

    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("PostgreSQL connection task failed: {err}");
        }
    });

    let dsl = Dsl::using(&client);

    let plain = dsl
        .raw("select 1::bigint as id, 'tara@example.com'::text as email")
        .fetch_one_as::<PlainAccount>()
        .await?;
    assert_eq!(
        plain,
        PlainAccount {
            id: 1,
            email: "tara@example.com".into()
        }
    );

    let renamed = dsl
        .raw("select 7::bigint as id, 'ada@example.com'::text as email")
        .fetch_one_as::<RenamedAccount>()
        .await?;
    assert_eq!(
        renamed,
        RenamedAccount {
            account_id: 7,
            contact: "ada@example.com".into()
        }
    );

    let camel = dsl
        .raw(r#"select 42::bigint as "userId", 'ben@example.com'::text as "primaryEmail""#)
        .fetch_one_as::<CamelAccount>()
        .await?;
    assert_eq!(
        camel,
        CamelAccount {
            user_id: 42,
            primary_email: "ben@example.com".into()
        }
    );

    let skipped = dsl
        .raw("select 9::bigint as id, 'cleo@example.com'::text as email")
        .fetch_one_as::<SkippedAccount>()
        .await?;
    assert_eq!(
        skipped,
        SkippedAccount {
            id: 9,
            email: "cleo@example.com".into(),
            cached: Vec::new()
        }
    );

    let defaulted_present = dsl
        .raw("select 11::bigint as id, 'd@example.com'::text as email, 'vip'::text as label")
        .fetch_one_as::<DefaultedAccount>()
        .await?;
    assert_eq!(
        defaulted_present,
        DefaultedAccount {
            id: 11,
            email: "d@example.com".into(),
            label: "vip".into()
        }
    );

    let defaulted_missing = dsl
        .raw("select 12::bigint as id, 'e@example.com'::text as email")
        .fetch_one_as::<DefaultedAccount>()
        .await?;
    assert_eq!(
        defaulted_missing,
        DefaultedAccount {
            id: 12,
            email: "e@example.com".into(),
            label: String::new()
        }
    );

    let defaulted_null = dsl
        .raw("select 13::bigint as id, 'g@example.com'::text as email, null::text as label")
        .fetch_one_as::<DefaultedAccount>()
        .await;
    let err = defaulted_null.expect_err("present-but-NULL label should error, not default");
    assert!(
        err.to_string().contains("failed to decode column label"),
        "unexpected error: {err}"
    );

    let flattened = dsl
        .raw("select 21::bigint as id, 'f@example.com'::text as email, true::boolean as active")
        .fetch_one_as::<FlattenedAccount>()
        .await?;
    assert_eq!(
        flattened,
        FlattenedAccount {
            audit: AuditFields {
                id: 21,
                email: "f@example.com".into()
            },
            active: true
        }
    );

    Ok(())
}
