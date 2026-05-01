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

### two equivalent forms

`fuwa` exposes both a JOOQ-style free-function form and a method-on-`dsl` form.
they produce byte-identical SQL ~ pick whichever reads nicer for the call site.

```rust
// free-function form: query construction is decoupled from the executor.
// great for helpers that build queries in one place and run them in another.
let q = select((users::id, users::email))
    .from(users::table)
    .where_(users::active.eq(true));
let rows = dsl.fetch_all(q).await?;

// method-on-dsl form: ergonomic for one-shot reads.
let rows = dsl
    .select((users::id, users::email))
    .from(users::table)
    .where_(users::active.eq(true))
    .fetch_all()
    .await?;
```

`select`, `insert_into`, `update`, `delete_from`, and `with` are all available
as free functions in `fuwa::prelude`. the runners on `DslContext` are
`fetch_all`, `fetch_one`, `fetch_optional`, and `execute`; each has a
`_as::<T>()` override when you want to decode rows into a hand-rolled type
instead of the projection's inferred row type.

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
   .where_((
       accounts::active.eq(true),
       posts::published.eq(true),
   ))
   .order_by((accounts::id.asc(), posts::id.asc()))
```

a tuple in `where_` / `having` / `on` AND-folds its elements, so the snippet
above is equivalent to `.where_(a.and(b))`. `Condition::all([..])` and
`Condition::any([..])` work the same way for `Vec`-shaped predicates.

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
// the free-function form is handy for subqueries: no dsl handle needed
// to build the inner SELECT, only to run the outer one.
let published_authors = select(posts::account_id)
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
dsl.select((accounts::id, accounts::profile.json_get_text("role")))
   .from(accounts::table)
   .where_(
       accounts::profile
           .contains(serde_json::json!({ "active": true }))
           .and(accounts::profile.has_key("role"))
           .and(accounts::tags.overlaps(vec!["beta", "internal"]))
           .and(accounts::email.eq_any(vec!["tara@example.com", "ada@example.com"])),
   )
```

jsonb operators (`json_get`, `json_get_text`, `json_path`, `json_path_text`,
`contains`, `has_key`) work directly on jsonb fields and `Expr<serde_json::Value, _>`.
array operators (`contains`, `overlaps`, `concat`, `eq_any`, `eq_all`) work
directly on array fields and `Expr<Vec<T>, _>`. drop into `.expr()` only when
you need to compose with custom expressions.

### window functions

```sql
select
    id,
    row_number() over (partition by account_id order by created_at desc) as rn,
    sum(amount) over (
        partition by account_id
        order by created_at asc
        rows between unbounded preceding and current row
    ) as running_total
from transactions
```

```rust
dsl.select((
       transactions::id,
       row_number()
           .over(
               partition_by(transactions::account_id)
                   .order_by(transactions::created_at.desc()),
           )
           .as_("rn"),
       sum(transactions::amount)
           .over(
               partition_by(transactions::account_id)
                   .order_by(transactions::created_at.asc())
                   .rows_between(unbounded_preceding(), current_row()),
           )
           .as_("running_total"),
   ))
   .from(transactions::table)
```

`row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first_value`, `last_value`,
and `ntile` are pure window functions (require `.over(...)`). every aggregate
also has `.over(...)` for window-shaped sums / averages / etc. frames support
`rows_between`, `range_between`, `groups_between`.

### set operators (union, except, intersect)

```sql
(select id from accounts where active = true)
union all
(select id from accounts where signup_rank > 100)
order by id asc
limit 50
```

```rust
let active = dsl.select(accounts::id)
                .from(accounts::table)
                .where_(accounts::active.eq(true));
let high = dsl.select(accounts::id)
              .from(accounts::table)
              .where_(accounts::signup_rank.gt(100));

active.union_all(high).order_by(accounts::id.asc()).limit(50)
```

`union`, `union_all`, `except`, `except_all`, `intersect`, `intersect_all` all
work this way. column shape on both sides has to match at the type level.

### more joins (right / full / cross / lateral)

```rust
// inner / left / right / full
dsl.select(...).right_join(other::table.on(...))
dsl.select(...).full_join(other::table.on(...))

// cross join (no on clause)
dsl.select(...).cross_join(other::table)

// lateral correlations for "latest N per group"-style queries
let latest_post = dsl
    .select(posts::title)
    .from(posts::table)
    .where_(posts::account_id.eq(accounts::id))
    .order_by(posts::created_at.desc())
    .limit(1)
    .alias("latest");
dsl.select((accounts::id, latest_post.field_of(posts::title)))
   .from(accounts::table)
   .cross_join_lateral(latest_post)
```

`join_lateral` and `left_join_lateral` are also available for lateral joins
that need an `on` condition.

### row locking (for update / skip locked / nowait)

```sql
select id from jobs
where status = 'queued'
order by created_at asc
limit 1
for update skip locked
```

```rust
dsl.select(jobs::id)
   .from(jobs::table)
   .where_(jobs::status.eq("queued"))
   .order_by(jobs::created_at.asc())
   .limit(1)
   .for_update()
   .skip_locked()
```

`for_update`, `for_no_key_update`, `for_share`, `for_key_share` cover all four
strengths. `.of(table)` narrows the lock scope; `.skip_locked()` and
`.no_wait()` choose the contention behaviour.

### exists / cast / extract / date_trunc / now / greatest / least / length / lower / upper / trim / abs / round / ceil / floor

every common postgres function is now a typed helper. nullability propagates
correctly:

```rust
use fuwa::prelude::*;

dsl.select((
       accounts::id,
       cast::<i32, String, _>(accounts::signup_rank).as_("rank_str"),
       extract::<_, _>("year", accounts::created_at).as_("year"),
       date_trunc("day", accounts::created_at).as_("day_bucket"),
       greatest((accounts::signup_rank, bind(0_i32))).as_("at_least_zero"),
       length(accounts::email).as_("email_len"),
       lower(accounts::email).as_("email_lc"),
   ))
   .from(accounts::table)
   .where_(exists(
       dsl.select(posts::id)
          .from(posts::table)
          .where_(posts::account_id.eq(accounts::id)),
   ))
```

every selectable expression also has `.as_("alias")` to attach a SQL column
alias.

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

the column-by-column form spells every assignment out:

```rust
dsl.insert_into(users::table)
   .values((
       users::email.set("tara@example.com"),
       users::active.set(true),
   ))
   .returning(users::id)
```

for full-row inserts, codegen already emits `impl Assignments for users::Record`
on every table, so `dsl.insert_into(table).value(record)` works:

```rust
dsl.insert_into(users::table)
   .value(users::Record {
       id: 1,
       email: "tara@example.com".to_owned(),
       active: true,
       // ...
   })
   .returning(users::id)
```

for partial inserts (e.g. when the primary key is `bigserial` and you don't
want to set `id`), define a narrower struct and `#[derive(Insertable)]` ~ the
attribute points at the schema module that owns the field constants:

```rust
use fuwa::Insertable;

#[derive(Insertable)]
#[fuwa(table = users)]
struct NewUser {
    email: String,
    active: bool,
}

dsl.insert_into(users::table)
   .value(NewUser { email: "tara@example.com".into(), active: true })
   .returning(users::id)
```

`#[derive(Patch)]` is the matching shape for partial UPDATEs ~ it wraps each
field in `Option<T>` and only emits assignments for `Some(_)` values:

```rust
use fuwa::Patch;

#[derive(Patch, Default)]
#[fuwa(table = users)]
struct UserPatch {
    email: Option<String>,
    active: Option<bool>,
}

dsl.update(users::table)
   .set(UserPatch { email: Some("new@example.com".into()), ..Default::default() })
   .where_(users::id.eq(7_i64))
```

### insert ... select / update ... from / delete ... using

```sql
insert into archive (id, payload)
select id, payload from events where created_at < now() - interval '30 days';
```

```rust
dsl.insert_into(archive::table)
   .columns((archive::id, archive::payload))
   .from_select(
       dsl.select((events::id, events::payload))
          .from(events::table)
          .where_(events::created_at.lt(bind(cutoff))),
   )
```

```rust
// update t set x = other.y from other where other.id = t.id
dsl.update(users::table)
   .set(users::email.set(bind("backfilled@example.com")))
   .from(posts::table)
   .where_(posts::user_id.eq(users::id))

// delete from t using other where ...
dsl.delete_from(users::table)
   .using(posts::table)
   .where_(posts::user_id.eq(users::id).and(posts::id.gt(bind(10_i64))))
```

### bulk inserts via binary COPY

for high-volume inserts, `dsl.copy_in_binary(...)` runs `COPY ... FROM STDIN
BINARY` against the executor and gives you a typed writer:

```rust
dsl.copy_in_binary(
    users::table,
    (users::email, users::active),
    async |writer| {
        writer.send(("a@example.com".to_owned(), true)).await?;
        writer.send(("b@example.com".to_owned(), true)).await?;
        Ok(())
    },
).await?;
```

works for tuples up to 8 columns. row tuple types track each column's
nullability (`Field<T, NotNull>` -> `T`, `Field<T, Nullable>` -> `Option<T>`).

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

for build scripts, the one-liner `fuwa_codegen::build::run()` reads
`DATABASE_URL`, falls back to `fuwa.schema.json` if the database is unreachable,
emits the right `cargo:rerun-if-*` directives, and writes `schema.rs` into
`$OUT_DIR`:

```rust
// build.rs
fn main() {
    fuwa_codegen::build::run().unwrap();
}
```

set `FUWA_OFFLINE=1` to force snapshot use ~ handy in CI without postgres. for
non-default options use `Builder` directly:

```rust
// build.rs
fn main() {
    fuwa_codegen::build::Builder::new()
        .schemas(["public", "auth"])
        .tables(["users", "posts"])
        .snapshot_path("fuwa.schema.json")
        .out_filename("schema.rs")
        .run()
        .unwrap();
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
constants, a `Record` struct, `FromRow` + `Assignments` impls, + an `all()`
selection helper:

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

    impl fuwa::FromRow for Record { /* read every column by index */ }
    impl fuwa::Assignments for Record { /* set every column from self.<col> */ }

    pub fn all() -> All { All }
}
```

the auto-emitted `Assignments` covers every column on the table, so
`dsl.insert_into(users::table).value(record)` works as a one-liner full-row
insert. for tables with `bigserial` PKs (where you don't want to send `id`),
hand-derive a narrower `#[derive(Insertable)]` struct as shown earlier.

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
        .fetch_all()
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
        .fetch_all()
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

async fn transaction_example(dsl: DslContext<Pool>) -> fuwa::Result<()> {
    dsl.transaction(async |dsl| {
        dsl.insert_into(users::table)
            .values((
                users::email.set("queued@example.com"),
                users::active.set(true),
            ))
            .execute()
            .await?;

        Ok(())
    })
    .await
}
```

## postgres streaming

`fetch_stream` and `fetch_chunked` use PostgreSQL portals for server-side
streaming. portals only live for the duration of a transaction, so the methods
are bounded on the `TransactionalExecutor` trait ~ calling them on a `Pool` or
bare `Client` is a **compile error**, not a runtime surprise. bind a DSL
context to an open transaction and keep it alive until the stream is exhausted
or dropped:

```rust
mod schema;

use fuwa::prelude::*;
use schema::users;

async fn stream_example(dsl: DslContext<Pool>) -> fuwa::Result<()> {
    dsl.transaction(async |dsl| {
        let mut chunks = dsl
            .select((users::id, users::email))
            .from(users::table)
            .order_by(users::id.asc())
            .fetch_chunked(500)
            .await?;

        while let Some(chunk) = chunks.next().await {
            for (id, email) in chunk? {
                println!("{id}: {email}");
            }
        }

        Ok(())
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
        .fetch_all_as::<String>()
        .await?;

    println!("{rows:?}");
    Ok(())
}
```

### compile-time-checked raw SQL

for raw SQL that should still get a sanity check, the `query!` macro
validates every `table.column` reference against `fuwa.schema.json` at compile
time (no live DB hit needed):

```rust
use fuwa::prelude::*;

async fn checked_raw(dsl: &DslContext<Pool>) -> fuwa::Result<Vec<String>> {
    let sql: &str = fuwa::query!(
        "select users.email from users where users.id = $1"
    );
    dsl.raw(sql).bind(7_i64).fetch_all_as::<String>().await
}
```

a typo'd column (e.g. `users.emial`) becomes a `cargo check` error pointing
at the macro call. set `FUWA_QUERY_SNAPSHOT` to override the snapshot path
(defaults to `$CARGO_MANIFEST_DIR/fuwa.schema.json`).

## error taxonomy

`fuwa::Error` exposes typed variants for the common postgres SQLSTATE codes
so callers can match on them directly without parsing strings:

```rust
use fuwa::Error;

match dsl.insert_into(...).execute().await {
    Err(Error::UniqueViolation { constraint, .. }) => { /* idempotent retry */ }
    Err(Error::SerializationFailure(_)) | Err(Error::DeadlockDetected(_)) => {
        // retry the transaction
    }
    Err(other) => return Err(other.into()),
    Ok(rows) => { /* ok */ }
}
```

variants: `UniqueViolation`, `ForeignKeyViolation`, `CheckViolation`,
`NotNullViolation`, `SerializationFailure`, `DeadlockDetected`. anything that
doesn't map falls through to `Error::Postgres { sqlstate, message }`.

## tracing

every `execute` / `fetch_*` emits a `debug` `tracing` span carrying the
rendered SQL, bind count, and rows affected. enable a subscriber to see them.
the `tracing` feature is on by default; turn it off with
`fuwa-postgres = { ..., default-features = false }` if you need a leaner
build.

## dynamic query composition

`Condition::all([..])` and `Condition::any([..])` fold an iterator of
predicates with `AND` / `OR`. an empty iterator returns `TRUE` / `FALSE` so
you can build filter sets without special-casing the empty case:

```rust
use fuwa::prelude::*;

let mut filters: Vec<Condition> = Vec::new();
if let Some(active) = active_filter {
    filters.push(users::active.eq(active));
}
if let Some(rank) = rank_filter {
    filters.push(users::signup_rank.gt(rank));
}
dsl.select(users::id)
   .from(users::table)
   .where_(Condition::all(filters))
```

`.filter(...)` is also accepted as a diesel-style alias for `.where_(...)`.

for accumulating filters one-at-a-time across a function body, `and_where` /
`and_having` and the `push_*` helpers keep the query type stable through a
mutation loop:

```rust
let mut q = select((users::id, users::email)).from(users::table);

if let Some(active) = active_filter {
    q = q.and_where(users::active.eq(active));
}
if let Some(rank) = rank_filter {
    q = q.and_where(users::signup_rank.gt(rank));
}
for sort in requested_sorts {
    q = q.push_order_by(sort);
}
let rows = dsl.fetch_all(q).await?;
```

`push_select_item`, `push_order_by`, `push_join`, and `push_group_by` accept
the bare AST nodes (`SelectItem`, `OrderExpr`, `Join`, `ExprNode`) when you
need to vary the projection / sort / joins by request input. they don't change
the query's record type, so dynamic columns need an explicit `fetch_all_as::<T>()`
to choose how rows decode.

### inspecting rendered SQL

every query type has a `.render()` (consuming) and a `.render_ref(&self)`
(borrow) that returns a `RenderedQuery`. `RenderedQuery` impls `Display`, so
`println!("{q}", q = q.render_ref()?)` and `dbg!(q.render_ref()?)` print the
SQL with `$N` placeholders without consuming the builder.

```rust
let q = select((users::id, users::email))
    .from(users::table)
    .where_(users::active.eq(true));

println!("{}", q.render_ref()?); // select "users"."id", "users"."email" from ...
let rows = dsl.fetch_all(q).await?;
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
