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

the rust snippets below assume a generated schema module is imported and an
executor has been wrapped once:

```rust
use fuwa::prelude::*;

let dsl = Dsl::connect(database_url)?;
```

### simple select

```sql
select id, email
from users
where active = true
order by created_at desc
limit 20
```

```rust
dsl.select((users::id, users::email))
   .from(users::table)
   .where_(users::active.eq(true))
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
dsl.select((accounts::email, posts::title, posts::score))
   .from(accounts::table)
   .join(posts::table.on(posts::account_id.eq(accounts::id)))
   .where_(
       accounts::active.eq(true)
           .and(posts::published.eq(true)),
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
dsl.select((posts::account_id, count_star()))
   .from(posts::table)
   .group_by(posts::account_id)
   .having(count_star().gt(1_i64))
   .order_by(posts::account_id.asc())
```

### with / common table expressions

```sql
with active_accounts as (
    select id, email
    from accounts
    where active = true
)
select id, email
from active_accounts
where id > 10
```

```rust
let active_accounts = Table::unqualified("active_accounts");
let (active_id, active_email) =
    active_accounts.fields_of((accounts::id, accounts::email));

dsl.with(
       "active_accounts",
       dsl.select((accounts::id, accounts::email))
          .from(accounts::table)
          .where_(accounts::active.eq(true)),
   )
   .select((active_id, active_email))
   .from(active_accounts)
   .where_(active_id.gt(10_i64))
```

### from / join subqueries

```sql
select recent.id, recent.email
from (
    select id, email
    from accounts
    where active = true
) as recent
where recent.id > 10
```

```rust
let recent = dsl
    .select((accounts::id, accounts::email))
    .from(accounts::table)
    .where_(accounts::active.eq(true))
    .alias("recent");
let (recent_id, recent_email) = recent.fields_of((accounts::id, accounts::email));

dsl.select((recent_id, recent_email))
   .from(recent)
   .where_(recent_id.gt(10_i64))
```

for computed or renamed columns, `field::<T, N>("name")` is still there.
`field_of` keeps the source field's nullability exactly ~ if a subquery uses a
left join and the selected value can become null, mark that projection nullable
yourself with `field::<T, Nullable>("name")`.

the same aliased subquery source works in joins:

```sql
select accounts.email, post_counts.account_id
from accounts
join (
    select account_id, count(*)
    from posts
    group by account_id
    having count(*) > 1
) as post_counts on post_counts.account_id = accounts.id
where accounts.active = true
```

```rust
let post_counts = dsl
    .select((posts::account_id, count_star()))
    .from(posts::table)
    .group_by(posts::account_id)
    .having(count_star().gt(1_i64))
    .alias("post_counts");
let post_counts_account_id = post_counts.field_of(posts::account_id);

dsl.select((accounts::email, post_counts_account_id))
   .from(accounts::table)
   .join(post_counts.on(post_counts_account_id.eq(accounts::id)))
   .where_(accounts::active.eq(true))
```

### in / not in subqueries

```sql
select id, email
from accounts
where id in (
    select account_id
    from posts
    where published = true
)
```

```rust
let published_authors = dsl
    .select(posts::account_id)
    .from(posts::table)
    .where_(posts::published.eq(true));

dsl.select((accounts::id, accounts::email))
   .from(accounts::table)
   .where_(accounts::id.in_(published_authors))
```

`in_(...)` and `not_in(...)` accept arrays, `Vec`s, and single-column
`SelectQuery`s whose selected SQL type matches the left-hand expression.
collect custom iterators into a `Vec` before passing them in.

### jsonb + postgres arrays

```sql
select id, profile ->> 'role'
from accounts
where profile @> '{"active": true}'::jsonb
  and profile ? 'role'
  and tags && array['beta', 'internal']
  and email = any(array['tara@example.com', 'ada@example.com'])
```

```rust
dsl.select((accounts::id, accounts::profile.expr().json_get_text("role")))
   .from(accounts::table)
   .where_(
       accounts::profile
           .expr()
           .contains(serde_json::json!({ "active": true }))
           .and(accounts::profile.expr().has_key("role"))
           .and(accounts::tags.expr().overlaps(vec!["beta", "internal"]))
           .and(
               accounts::email
                   .expr()
                   .eq_any(vec!["tara@example.com", "ada@example.com"]),
           ),
   )
```

jsonb operators are available on `Expr<serde_json::Value, _>`, and array
operators are available on `Expr<Vec<T>, _>`, so generated field constants use
`.expr()` before postgres-native helpers like `json_get_text`, `contains`,
`overlaps`, `concat`, `eq_any`, and `eq_all`.

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
dsl.select((
       accounts::id,
       concat(
           coalesce((accounts::display_name, accounts::email)),
           bind(" account"),
       ),
       case_when()
           .when(accounts::active.eq(true), bind("active"))
           .else_(bind("inactive")),
   ))
   .from(accounts::table)
   .where_(accounts::id.in_([1_i64, 2_i64]))
   .order_by(accounts::id.asc())
```

### insert ... returning

```sql
insert into users (email, active)
values ('tara@example.com', true)
returning id
```

```rust
dsl.insert_into(users::table)
   .values((
       users::email.set("tara@example.com"),
       users::active.set(true),
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
dsl.insert_into(users::table)
   .values((
       users::id.set(4_i64),
       users::email.set("ben@example.com"),
       users::display_name.set(Some("Benedict")),
       users::active.set(true),
   ))
   .on_conflict((users::email,))
   .do_update(|excluded| (
       users::display_name.set(excluded.field(users::display_name)),
       users::active.set(excluded.field(users::active)),
   ))
   .returning((users::id, users::display_name, users::active))
```

every column reference + bound value is type-checked at compile time, so a
typo'd column name or an `.eq(true)` against a `text` field is a compile error,
not a runtime surprise.

## quickstart

### 1. add dependencies


```toml
# Cargo.toml
[dependencies]
fuwa = { git = "https://github.com/taracutie/fuwa.git" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
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

### 5. connect and query

`Dsl::connect(...)` builds fuwa's default postgres pool and returns a
`DslContext<Pool>`. queries acquire a postgres connection when `.fetch_*()` or
`.execute()` runs. plain values passed to comparisons and assignments become
bind parameters automatically, so they're never just stuffed into the SQL text.
`bind(...)` still works when you want to be explicit.

this example assumes `src/schema.rs` was generated from the `users` + `posts`
tables up above.

```rust
mod schema;

use fuwa::prelude::*;
use schema::users;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dsl = Dsl::connect(std::env::var("DATABASE_URL")?)?;

    let users = dsl
        .select(users::all())
        .from(users::table)
        .where_(users::active.eq(true))
        .fetch_all::<users::Record>()
        .await?;

    Ok(())
}
```

use `PoolOptions` when you want to tune the default pool:

```rust
use fuwa::prelude::*;

let dsl = Dsl::connect_with_options(
    std::env::var("DATABASE_URL")?,
    PoolOptions { max_size: 32 },
)?;
```

### sharing the dsl across handlers

`DslContext<Pool>` is cheap to clone, so app state can own one shared query
entry point:

```rust
use axum::extract::State;
use fuwa::prelude::*;

#[derive(Clone)]
struct AppState {
    dsl: DslContext<Pool>,
}

async fn handler(State(state): State<AppState>) -> fuwa::Result<String> {
    let emails = state
        .dsl
        .select(users::email)
        .from(users::table)
        .where_(users::active.eq(true))
        .fetch_all::<String>()
        .await?;

    Ok(format!("{emails:?}"))
}
```

## postgres transactions

`DslContext::transaction(...)` opens a postgres transaction, passes a
transaction-scoped DSL context to the callback, commits on success, and rolls
back when the callback returns an error:

```rust
use fuwa::prelude::*;

async fn transaction_example(
    dsl: DslContext<Pool>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    dsl.transaction(|dsl| {
        Box::pin(async move {
            dsl.insert_into(users::table)
                .values((
                    users::email.set("queued@example.com"),
                    users::active.set(true),
                ))
                .execute()
                .await?;

            Ok(())
        })
    })
    .await?;

    Ok(())
}
```

## postgres streaming

`fetch_stream` and `fetch_chunked` use PostgreSQL portals for server-side
streaming. Portals only live for the duration of a transaction, so bind a DSL
context to an open transaction and keep it alive until the stream is exhausted
or dropped:

```rust
use fuwa::prelude::*;

async fn stream_example(
    dsl: DslContext<Pool>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    dsl.transaction(|dsl| {
        Box::pin(async move {
            let mut chunks = dsl
                .raw("select id, email from users order by id")
                .fetch_chunked::<(i64, String)>(500)
                .await?;

            while let Some(chunk) = chunks.next().await {
                for (id, email) in chunk? {
                    println!("{id}: {email}");
                }
            }

            Ok(())
        })
    })
    .await?;

    Ok(())
}
```

## raw SQL escape hatch

for SQL the typed DSL doesnt cover yet, you can drop into raw SQL with separate bind values:

```rust
use fuwa::prelude::*;

async fn raw_example(dsl: &DslContext<Pool>) -> fuwa::Result<()> {
    let rows = dsl
        .raw(r#"select email from users where email ~* $1"#)
        .bind(r"@example\.com$")
        .fetch_all::<String>()
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
