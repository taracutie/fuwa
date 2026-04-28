<p align="center">
  <img src="fuwafuwa.png" width="400" />
</p>

<h1 align="center">fuwafuwa!</h1>

<p align="center">
  <em>comfy comfy postgres~ ♡ 🌼</em>
</p>

---

's just my cute crate to interact with postgres in a safe + performant (hopefully) manner

## quickstart

```rust
use fuwa::prelude::*;

#[allow(non_upper_case_globals)]
mod users {
    use fuwa::prelude::*;

    pub const table: Table = Table::new("public", "users");
    pub const id: Field<i64, NotNull> = Field::new(table, "id");
    pub const email: Field<String, NotNull> = Field::new(table, "email");
    pub const active: Field<bool, NotNull> = Field::new(table, "active");
}

# async fn example(client: &tokio_postgres::Client) -> fuwa::Result<()> {
let ctx = Context::new();

let rows = ctx
    .select((users::id, users::email))
    .from(users::table)
    .where_(users::active.eq(bind(true)))
    .order_by(users::id.desc())
    .limit(20)
    .fetch_all::<(i64, String)>(client)
    .await?;
# let _ = rows;
# Ok(())
# }
```

for stuff the typed DSL doesn't cover yet, there's raw SQL + separate bind values:

```rust
use fuwa::prelude::*;

# async fn example(client: &tokio_postgres::Client) -> fuwa::Result<()> {
let rows = raw(r#"select filename from "ImageMetadata" where filename = any($1)"#)
    .bind(vec!["a.jpg".to_owned(), "b.jpg".to_owned()])
    .fetch_all::<String>(client)
    .await?;
# let _ = rows;
# Ok(())
# }
```

## codegen

generate a schema module from postgres:

```sh
fuwa-codegen \
  --database-url "$DATABASE_URL" \
  --schema public \
  --out src/schema.rs
```

or limit it to specific schemas / tables:

```sh
fuwa-codegen \
  --database-url "$DATABASE_URL" \
  --schema public \
  --table users \
  --table posts \
  --out src/schema.rs
```

`--schema` and `--table` can repeat or be comma-separated. tables can be
unqualified (`users`) or schema-qualified (`admin.audit_log`).

you get `Table` and `Field<T, Nullability>` values, plus a `Record` type and `all()` selection helper.

## local postgres tests

```sh
docker compose up -d postgres
FUWA_TEST_DATABASE_URL=postgres://fuwa:fuwa@localhost:15432/fuwa cargo test -p fuwa-postgres --test postgres
```

## safety

- values are always rendered as postgres bind placeholders (`$1`, `$2`, ...).
- bind values are stored separately and passed straight to `tokio-postgres`.
- raw SQL escape hatches still collect values through `.bind(...)` ~ don't interpolate values into raw SQL text.
- identifiers are quoted with double quotes and embedded quotes are escaped.
- generated schema code should be your primary source of table + field identifiers.
- `fuwa-codegen` connects with `default_transaction_read_only=on` and introspects inside an explicit `BEGIN READ ONLY` transaction.