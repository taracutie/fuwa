use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use fuwa_codegen::{generate, CodegenSource, GenerateOptions, SchemaSnapshot};

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

#[test]
fn snapshot_round_trips_into_generate() -> TestResult {
    let dir = temp_dir("snapshot");
    fs::create_dir_all(&dir)?;
    let prisma_path = dir.join("schema.prisma");
    let snapshot_path = dir.join("fuwa.schema.json");

    fs::write(
        &prisma_path,
        r#"
        datasource db {
          provider = "postgresql"
        }

        model User {
          id    BigInt @id
          email String @unique

          @@map("users")
        }
        "#,
    )?;

    let snapshot = SchemaSnapshot::from_prisma(&prisma_path)?;
    snapshot.write_to(&snapshot_path)?;
    let generated = generate(GenerateOptions::new(CodegenSource::Snapshot(snapshot_path)))?;

    assert!(generated.contains("pub mod users"));
    assert!(generated.contains("pub const id: Field<i64, NotNull>"));
    assert!(generated.contains("pub const email: Field<String, NotNull>"));
    Ok(())
}

#[test]
fn prisma_schema_generates_compile_checked_module_and_expected_sql() -> TestResult {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crate should live under workspace src")
        .to_owned();
    let dir = temp_dir("prisma");
    let src_dir = dir.join("src");
    fs::create_dir_all(&src_dir)?;
    let prisma_path = dir.join("schema.prisma");

    fs::write(
        &prisma_path,
        r#"
        datasource db {
          provider = "postgresql"
          schemas  = ["public", "fuwa_prisma_it"]
        }

        enum Role {
          USER
          ADMIN @map("admin")

          @@map("user_role")
          @@schema("fuwa_prisma_it")
        }

        enum Table {
          SELF
          ACTIVE @map("active")

          @@map("user_table")
          @@schema("fuwa_prisma_it")
        }

        enum Box {
          SMALL

          @@map("user_box")
          @@schema("fuwa_prisma_it")
        }

        model User {
          id        String   @id @default(uuid()) @db.Uuid
          email     String   @unique
          firstName String   @map("first_name")
          role      Role?    @default(USER)
          state     Table?   @default(SELF)
          container Box?     @map("container")
          createdAt DateTime @default(now()) @map("created_at") @db.Timestamptz
          posts     Post[]

          @@map("users")
          @@schema("fuwa_prisma_it")
        }

        model Post {
          id       BigInt  @id @default(autoincrement())
          author   User    @relation(fields: [authorId], references: [id])
          authorId String  @map("author_id") @db.Uuid
          title    String
          score    Decimal @default(0) @db.Decimal(12, 2)
          meta     Json?
          bytes    Bytes?

          @@map("posts")
        }
        "#,
    )?;

    let generated = generate(GenerateOptions::new(CodegenSource::Prisma(prisma_path)))?;
    assert!(generated.contains("pub enum Role"));
    assert!(generated.contains("Admin => \"admin\""));
    assert!(generated.contains("pub enum Table"));
    assert!(generated.contains("_Self => \"SELF\""));
    assert!(generated.contains("pub enum Box"));
    assert!(generated.contains("std::boxed::Box<dyn std::error::Error + Sync + Send>"));
    assert!(!generated.contains("use super::*"));
    assert!(generated.contains("type Stored = Role"));
    assert!(generated.contains("impl<'a> fuwa::postgres::types::FromSql<'a> for Role"));
    assert!(generated.contains("impl fuwa::postgres::types::ToSql for Role"));
    assert!(
        generated.contains("pub const table: Table = Table::new(\"fuwa_prisma_it\", \"users\")")
    );
    assert!(generated.contains("pub const first_name: Field<String, NotNull>"));
    assert!(generated.contains("pub const role: Field<super::Role, Nullable>"));
    assert!(!generated.contains(r#"new_with_pg_type_and_select_cast(table, "role""#));
    assert!(generated.contains("pub role: Option<super::Role>"));
    assert!(generated.contains("pub const state: Field<super::Table, Nullable>"));
    assert!(generated.contains("pub state: Option<super::Table>"));
    assert!(generated.contains("pub const container: Field<super::Box, Nullable>"));
    assert!(generated.contains("pub container: Option<super::Box>"));
    assert!(generated.contains("pub const author_id: Field<fuwa::types::Uuid, NotNull>"));
    assert!(generated.contains("pub const score: Field<fuwa::types::Decimal, NotNull>"));
    assert!(generated.contains("pub const meta: Field<fuwa::types::Value, Nullable>"));

    fs::write(src_dir.join("schema.rs"), &generated)?;
    fs::write(
        dir.join("Cargo.toml"),
        format!(
            r#"
            [package]
            name = "fuwa_prisma_codegen_compile"
            version = "0.0.0"
            edition = "2021"

            [dependencies]
            fuwa = {{ path = "{}" }}
            tokio = {{ version = "1", features = ["macros", "rt-multi-thread"] }}
            tokio-postgres = {{ version = "0.7" }}
            "#,
            root.join("src/fuwa").display()
        ),
    )?;

    fs::write(
        src_dir.join("lib.rs"),
        format!(
            r##"
            pub mod schema {{
                include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/schema.rs"));
            }}

            #[cfg(test)]
            mod tests {{
                use fuwa::prelude::*;
                use super::schema::{{posts, users, Role}};

                #[test]
                fn nullable_generated_enum_has_from_row_impl() {{
                    fn assert_from_row<T: fuwa::FromRow>() {{}}

                    assert_from_row::<Option<Role>>();
                }}

                #[test]
                fn renders_prisma_expected_identifiers() {{
                    let query = fuwa::core::Context::new()
                        .select((users::id, users::role, posts::author_id))
                        .from(users::table)
                        .join(posts::table.on(posts::author_id.eq(users::id)))
                        .where_(users::role.eq(bind(Role::Admin)))
                        .render()
                        .unwrap();

                    assert_eq!(
                        query.sql(),
                        r#"select "users"."id", "users"."role", "posts"."author_id" from "fuwa_prisma_it"."users" join "public"."posts" on ("posts"."author_id" = "users"."id") where ("users"."role" = $1)"#
                    );
                    assert_eq!(query.binds().len(), 1);
                }}

                #[test]
                fn generated_enum_codecs_accept_real_postgres_enum_types() {{
                    use fuwa::postgres::types::{{FromSql, Kind, ToSql, Type}};

                    let ty = Type::new(
                        "user_role".to_owned(),
                        1,
                        Kind::Enum(vec!["USER".to_owned(), "admin".to_owned()]),
                        "fuwa_prisma_it".to_owned(),
                    );

                    assert!(<Role as FromSql>::accepts(&ty));
                    assert!(<Role as ToSql>::accepts(&ty));
                    assert_eq!(<Role as FromSql>::from_sql(&ty, b"admin").unwrap(), Role::Admin);
                }}

                #[tokio::test]
                async fn generated_enum_round_trips_with_real_postgres_enum_columns() -> std::result::Result<(), Box<dyn std::error::Error>> {{
                    let Ok(database_url) = std::env::var("FUWA_TEST_DATABASE_URL") else {{
                        eprintln!("skipping generated Prisma enum integration test: FUWA_TEST_DATABASE_URL is not set");
                        return Ok(());
                    }};

                    let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls).await?;
                    tokio::spawn(async move {{
                        if let Err(err) = connection.await {{
                            eprintln!("PostgreSQL connection task failed: {{err}}");
                        }}
                    }});

                    client
                        .batch_execute(
                            r#"
                            drop schema if exists fuwa_prisma_it cascade;
                            create schema fuwa_prisma_it;
                            create type fuwa_prisma_it.user_role as enum ('USER', 'admin');
                            create type fuwa_prisma_it.user_table as enum ('SELF', 'active');
                            create type fuwa_prisma_it.user_box as enum ('SMALL');
                            create table fuwa_prisma_it.users (
                                id uuid primary key default '00000000-0000-0000-0000-000000000001'::uuid,
                                email text not null unique,
                                first_name text not null,
                                role fuwa_prisma_it.user_role default 'USER',
                                state fuwa_prisma_it.user_table default 'SELF',
                                container fuwa_prisma_it.user_box,
                                created_at timestamptz not null default now()
                            );
                            "#,
                        )
                        .await?;

                    let dsl = Dsl::using(&client);
                    let inserted_role = dsl
                        .insert_into(users::table)
                        .values((
                            users::email.set(bind("enum@example.com")),
                            users::first_name.set(bind("Enum")),
                            users::role.set(bind(Role::Admin)),
                        ))
                        .returning(users::role)
                        .fetch_one()
                        .await?;

                    assert_eq!(inserted_role, Some(Role::Admin));

                    let record = dsl
                        .select(users::all())
                        .from(users::table)
                        .where_(users::role.eq(bind(Role::Admin)))
                        .fetch_one()
                        .await?;

                    assert_eq!(record.role, Some(Role::Admin));
                    assert_eq!(record.state, Some(super::schema::Table::_Self));
                    assert_eq!(record.container, None);

                    client
                        .batch_execute("drop schema if exists fuwa_prisma_it cascade;")
                        .await?;

                    Ok(())
                }}
            }}
            "##
        ),
    )?;

    let output = Command::new("cargo")
        .arg("test")
        .arg("--manifest-path")
        .arg(dir.join("Cargo.toml"))
        .arg("--offline")
        .arg("--quiet")
        .output()?;
    assert!(
        output.status.success(),
        "generated Prisma module failed to compile/test\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    Ok(())
}

fn temp_dir(kind: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "fuwa-codegen-{kind}-{}-{nonce}",
        std::process::id()
    ))
}
