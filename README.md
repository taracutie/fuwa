<p align="center">
  <img src="fuwafuwa.png" width="400" />
</p>

<h1 align="center">fuwafuwa!</h1>

<p align="center">
  <em>comfy comfy postgres~ ♡ 🌼</em>
</p>

---

`fuwa`'s just my cute crate to interact with postgres in a safe + performant
(hopefully) manner.

## quickstart

### 1. add dependencies


```toml
# Cargo.toml
[dependencies]
fuwa = { git = "https://github.com/taracutie/fuwa.git" }
```

### 2. create tables

`fuwa-codegen` reads your existing postgres tables. for example:

```sql
create table public.users (
    id bigserial primary key,
    email text not null,
    active boolean not null default true,
    created_at timestamptz not null default now()
);

create table public.posts (
    id bigserial primary key,
    user_id bigint not null references public.users(id),
    title text not null,
    published boolean not null default false
);
```

### 3. install and run codegen

install the cli from the repo:

```sh
cargo install --git https://github.com/taracutie/fuwa.git fuwa-codegen --locked
```

generate a rust schema module:

```sh
fuwa-codegen \
  --database-url "$DATABASE_URL" \
  --schema public \
  --table users \
  --table posts \
  --out src/schema.rs
```

`--schema` and `--table` can be repeated or comma-separated. tables can be
unqualified (`users`) or schema-qualified (`admin.audit_log`). if you skip
`--table`, it just generates all supported tables in the selected schemas.

generated modules come with `Table` constants, typed `Field<T, Nullability>`
constants, a `Record` struct, a `FromRow` impl, + an `all()` selection helper:

```rust
pub mod users {
    use fuwa::prelude::*;

    pub const table: Table = Table::new("public", "users");
    pub const id: Field<i64, NotNull> = Field::new(table, "id");
    pub const email: Field<String, NotNull> = Field::new(table, "email");
    pub const active: Field<bool, NotNull> = Field::new(table, "active");

    #[derive(Debug, Clone)]
    pub struct Record {
        pub id: i64,
        pub email: String,
        pub active: bool,
        // plus the rest of the table columns
    }

    pub fn all() -> All {
        All
    }
}
```

### 4. import the generated schema

drop the generated module into your app:

```rust
mod schema;

use fuwa::prelude::*;
use schema::{posts, users};
```

### 5. build and execute queries

`Context` is the query builder entry point. values go thru `bind(...)` ~
they're never just stuffed into the SQL text.

```rust
let ctx = Context::new();

let rows = ctx
    .select((users::id, users::email, posts::title))
    .from(users::table)
    .join(posts::table.on(posts::user_id.eq(users::id)))
    .where_(users::active.eq(bind(true)))
    .order_by(users::created_at.desc())
    .limit(20)
    .fetch_all::<(i64, String, String)>(&client)
    .await?;

let users = ctx
    .select(users::all())
    .from(users::table)
    .where_(users::email.ilike(bind("%@example.com")))
    .fetch_all::<users::Record>(&client)
    .await?;
```

### full example

this assumes `src/schema.rs` was generated from the `users` + `posts` tables
up above.

```rust
mod schema;

use fuwa::prelude::*;
use schema::{posts, users};
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;

    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("postgres connection error: {err}");
        }
    });

    let ctx = Context::new();

    let user_id = ctx
        .insert_into(users::table)
        .values((
            users::email.set(bind("tara@example.com")),
            users::active.set(bind(true)),
        ))
        .returning(users::id)
        .fetch_one::<i64>(&client)
        .await?;

    let post_id = ctx
        .insert_into(posts::table)
        .values((
            posts::user_id.set(bind(user_id)),
            posts::title.set(bind("hello from fuwa")),
            posts::published.set(bind(true)),
        ))
        .returning(posts::id)
        .fetch_one::<i64>(&client)
        .await?;

    let joined = ctx
        .select((users::id, users::email, posts::title))
        .from(users::table)
        .join(posts::table.on(posts::user_id.eq(users::id)))
        .where_(users::active.eq(bind(true)).and(posts::published.eq(bind(true))))
        .order_by((users::id.asc(), posts::id.desc()))
        .limit(20)
        .fetch_all::<(i64, String, String)>(&client)
        .await?;

    let matching_users = ctx
        .select(users::all())
        .from(users::table)
        .where_(users::email.ilike(bind("%@example.com")))
        .fetch_all::<users::Record>(&client)
        .await?;

    let renamed_email = ctx
        .update(users::table)
        .set(users::email.set(bind("new@example.com")))
        .where_(users::id.eq(bind(user_id)))
        .returning(users::email)
        .fetch_one::<String>(&client)
        .await?;

    let deleted_posts = ctx
        .delete_from(posts::table)
        .where_(posts::id.eq(bind(post_id)))
        .execute(&client)
        .await?;

    println!("{joined:?}");
    println!("{matching_users:?}");
    println!("renamed to {renamed_email}, deleted {deleted_posts} post(s)");

    Ok(())
}
```

## raw SQL escape hatch

for SQL the typed DSL doesnt cover yet, you can drop into raw SQL with separate bind values:

```rust
use fuwa::prelude::*;

async fn raw_example(client: &tokio_postgres::Client) -> fuwa::Result<()> {
    let rows = raw(r#"select filename from "ImageMetadata" where filename = any($1)"#)
        .bind(vec!["a.jpg".to_owned(), "b.jpg".to_owned()])
        .fetch_all::<String>(client)
        .await?;

    println!("{rows:?}");
    Ok(())
}
```

## local postgres tests

```sh
docker compose up -d postgres
FUWA_TEST_DATABASE_URL=postgres://fuwa:fuwa@localhost:15432/fuwa cargo test -p fuwa-postgres --test postgres
```

## notes

- `fuwa-codegen` connects with `default_transaction_read_only=on` and pokes
  around inside an explicit `BEGIN READ ONLY` transaction (so it cant mess up
  your db on accident).