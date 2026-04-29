<p align="center">
  <img src="fuwafuwa.png" width="400" />
</p>

<h1 align="center">fuwafuwa!</h1>

<p align="center">
  <em>comfy comfy postgres~ ♡ 🌼</em>
</p>

---

`fuwa`'s just my cute crate to interact with postgres in a safe + performant manner.

## examples

with the `fuwa` DSL, queries read almost the same as the SQL they compile to ~
typed columns, typed bindings, no string concatenation.

### simple select

```sql
select id, email
from users
where active = true
order by created_at desc
limit 20
```

```rust
ctx.select((users::id, users::email))
   .from(users::table)
   .where_(users::active.eq(bind(true)))
   .order_by(users::created_at.desc())
   .limit(20)
```

### join with multiple conditions

```sql
select accounts.email, posts.title, posts.score
from accounts
join posts on posts.account_id = accounts.id
where accounts.active = true
  and posts.published = true
order by accounts.id asc, posts.id asc
```

```rust
ctx.select((accounts::email, posts::title, posts::score))
   .from(accounts::table)
   .join(posts::table.on(posts::account_id.eq(accounts::id)))
   .where_(
       accounts::active.eq(bind(true))
           .and(posts::published.eq(bind(true))),
   )
   .order_by((accounts::id.asc(), posts::id.asc()))
```

### aggregation, group by, having

```sql
select account_id, count(*)
from posts
group by account_id
having count(*) > 1
order by account_id asc
```

```rust
ctx.select((posts::account_id, count_star()))
   .from(posts::table)
   .group_by(posts::account_id)
   .having(count_star().gt(bind(1_i64)))
   .order_by(posts::account_id.asc())
```

### case / coalesce / concat

```sql
select
    id,
    coalesce(display_name, email) || ' account',
    case when active = true then 'active' else 'inactive' end
from accounts
where id in (1, 2)
order by id asc
```

```rust
ctx.select((
       accounts::id,
       concat(
           coalesce((accounts::display_name, accounts::email)),
           bind(" account"),
       ),
       case_when()
           .when(accounts::active.eq(bind(true)), bind("active"))
           .else_(bind("inactive")),
   ))
   .from(accounts::table)
   .where_(accounts::id.in_([bind(1_i64), bind(2_i64)]))
   .order_by(accounts::id.asc())
```

### insert ... returning

```sql
insert into users (email, active)
values ('tara@example.com', true)
returning id
```

```rust
ctx.insert_into(users::table)
   .values((
       users::email.set(bind("tara@example.com")),
       users::active.set(bind(true)),
   ))
   .returning(users::id)
```

### upsert (`on conflict do update`)

```sql
insert into users (id, email, display_name, active)
values (4, 'ben@example.com', 'Benedict', true)
on conflict (email) do update
   set display_name = excluded.display_name,
       active       = excluded.active
returning id, display_name, active
```

```rust
ctx.insert_into(users::table)
   .values((
       users::id.set(bind(4_i64)),
       users::email.set(bind("ben@example.com")),
       users::display_name.set(bind(Some("Benedict"))),
       users::active.set(bind(true)),
   ))
   .on_conflict((users::email,))
   .do_update(|excluded| (
       users::display_name.set(excluded.field(users::display_name)),
       users::active.set(excluded.field(users::active)),
   ))
   .returning((users::id, users::display_name, users::active))
```

every column reference + bound value is type-checked at compile time, so a
typo'd column name or a `bind(true)` against a `text` field is a compile error,
not a runtime surprise.

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
you can also generate from an offline snapshot or a prisma schema:

```sh
fuwa-codegen --snapshot fuwa.schema.json --out src/schema.rs
fuwa-codegen --prisma prisma/schema.prisma --out src/schema.rs
```

to create a snapshot from a live database:

```sh
fuwa-codegen snapshot \
  --database-url "$DATABASE_URL" \
  --schema public \
  --out fuwa.schema.json
```

for build scripts, call the library entry point and fall back to a checked-in
snapshot when `FUWA_OFFLINE=1` or the database is unavailable:

```rust
// build.rs
use std::{env, fs, path::PathBuf};

use fuwa_codegen::{generate, CodegenSource, GenerateOptions, SchemaSnapshot};

fn main() {
    println!("cargo:rerun-if-env-changed=DATABASE_URL");
    println!("cargo:rerun-if-env-changed=FUWA_OFFLINE");
    println!("cargo:rerun-if-changed=fuwa.schema.json");

    let out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("schema.rs");
    let snapshot = PathBuf::from("fuwa.schema.json");

    let generated = if env::var_os("FUWA_OFFLINE").is_some() {
        generate(GenerateOptions::new(CodegenSource::Snapshot(snapshot))).unwrap()
    } else {
        let database_url = env::var("DATABASE_URL").unwrap();
        generate(GenerateOptions::new(CodegenSource::Database {
            database_url,
            schemas: vec!["public".to_owned()],
            tables: Vec::new(),
        }))
        .or_else(|_| {
            SchemaSnapshot::from_snapshot("fuwa.schema.json")
                .and_then(|snapshot| generate(GenerateOptions::new(CodegenSource::SnapshotValue(snapshot))))
        })
        .unwrap()
    };

    fs::write(out, generated).unwrap();
}
```

add `fuwa-codegen` under `[build-dependencies]` when you use this pattern.
include the generated output from your crate:

```rust
pub mod schema {
    include!(concat!(env!("OUT_DIR"), "/schema.rs"));
}
```

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
- `fuwa-codegen` can read live postgres, `fuwa.schema.json` snapshots, or
  postgresql `schema.prisma` files.
