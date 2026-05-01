//! Compile-time-checked SQL query macros for `fuwa`.
//!
//! These macros validate that every `table.column` reference in a SQL string
//! exists in `fuwa.schema.json` (the snapshot produced by `fuwa-codegen`),
//! catching typos and renamed columns at `cargo check` time without ever
//! touching a live database.

use std::path::{Path, PathBuf};

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, LitStr};

use fuwa_codegen::{DatabaseSchema, SchemaSnapshot};

/// Validate a SQL string against the schema snapshot at compile time and
/// return it as a `&'static str` for use with `dsl.raw(...)`.
///
/// Resolves the snapshot path from `FUWA_QUERY_SNAPSHOT` if set, otherwise
/// from `$CARGO_MANIFEST_DIR/fuwa.schema.json`.
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let lit = parse_macro_input!(input as LitStr);
    let sql = lit.value();

    let loaded = match load_snapshot() {
        Ok(s) => s,
        Err(e) => return syn::Error::new(lit.span(), e).into_compile_error().into(),
    };

    if let Err(e) = validate_qualified_refs(&sql, &loaded.snapshot.schema) {
        return syn::Error::new(lit.span(), e).into_compile_error().into();
    }

    let snapshot_path = match include_path_literal(&loaded.path) {
        Ok(path) => path,
        Err(e) => return syn::Error::new(lit.span(), e).into_compile_error().into(),
    };

    quote!({
        const _: ::core::option::Option<&'static str> = option_env!("FUWA_QUERY_SNAPSHOT");
        const _: &'static str = include_str!(#snapshot_path);
        #lit
    })
    .into()
}

struct LoadedSnapshot {
    path: PathBuf,
    snapshot: SchemaSnapshot,
}

fn load_snapshot() -> Result<LoadedSnapshot, String> {
    let path: PathBuf = match std::env::var_os("FUWA_QUERY_SNAPSHOT") {
        Some(p) => PathBuf::from(p),
        None => {
            let manifest = std::env::var_os("CARGO_MANIFEST_DIR").ok_or_else(|| {
                "fuwa::query!: CARGO_MANIFEST_DIR is not set; cannot resolve schema snapshot path"
                    .to_owned()
            })?;
            let mut path = PathBuf::from(manifest);
            path.push("fuwa.schema.json");
            path
        }
    };

    let snapshot = SchemaSnapshot::from_snapshot(&path).map_err(|e| {
        format!(
            "fuwa::query!: failed to load schema snapshot from {}: {e}\n\
             Generate one with `fuwa-codegen --snapshot` or set FUWA_QUERY_SNAPSHOT.",
            path.display()
        )
    })?;
    let path = path.canonicalize().map_err(|e| {
        format!(
            "fuwa::query!: failed to resolve schema snapshot path {}: {e}",
            path.display()
        )
    })?;

    Ok(LoadedSnapshot { path, snapshot })
}

fn include_path_literal(path: &Path) -> Result<String, String> {
    path.to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("fuwa::query!: schema snapshot path is not valid UTF-8: {path:?}"))
}

fn validate_qualified_refs(sql: &str, schema: &DatabaseSchema) -> Result<(), String> {
    let aliases = extract_table_aliases(sql);

    for reference in extract_qualified_ref_spans(sql) {
        let reference_to_validate =
            match resolve_alias_reference(&reference.reference, reference.start, &aliases) {
                AliasResolution::Resolved(resolved) => resolved,
                AliasResolution::Derived => continue,
                AliasResolution::Unresolved => reference.reference.clone(),
            };

        if known_column(schema, &reference_to_validate) {
            continue;
        }

        if reference_to_validate.schema.is_none()
            && known_schema_table(
                schema,
                &reference_to_validate.table,
                &reference_to_validate.column,
            )
        {
            continue;
        }

        return Err(format!(
            "fuwa::query!: unknown column `{}` ~ no matching table+column in fuwa.schema.json",
            reference.reference.display()
        ));
    }
    Ok(())
}

fn resolve_alias_reference(
    reference: &QualifiedRef,
    position: usize,
    aliases: &[TableAlias],
) -> AliasResolution {
    if reference.schema.is_some() {
        return AliasResolution::Unresolved;
    }

    let Some(alias) = aliases
        .iter()
        .enumerate()
        .filter(|(_, alias)| alias.alias == reference.table && alias.scope.contains(position))
        .max_by_key(|(index, alias)| (alias.scope.depth, *index))
        .map(|(_, alias)| alias)
    else {
        return AliasResolution::Unresolved;
    };

    match &alias.target {
        AliasTarget::Table { schema, table } => AliasResolution::Resolved(QualifiedRef {
            schema: schema.clone(),
            table: table.clone(),
            column: reference.column.clone(),
        }),
        AliasTarget::Derived => AliasResolution::Derived,
    }
}

fn known_column(schema: &DatabaseSchema, reference: &QualifiedRef) -> bool {
    schema.tables.iter().any(|table| {
        reference
            .schema
            .as_deref()
            .map(|schema_name| table.schema == schema_name)
            .unwrap_or(true)
            && table.name == reference.table
            && table
                .columns
                .iter()
                .any(|column| column.name == reference.column)
    })
}

fn known_schema_table(schema: &DatabaseSchema, schema_name: &str, table_name: &str) -> bool {
    schema
        .tables
        .iter()
        .any(|table| table.schema == schema_name && table.name == table_name)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableAlias {
    alias: String,
    target: AliasTarget,
    scope: SqlScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SqlScope {
    start: usize,
    end: usize,
    depth: usize,
}

impl SqlScope {
    fn contains(self, position: usize) -> bool {
        self.start <= position && position < self.end
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AliasTarget {
    Table {
        schema: Option<String>,
        table: String,
    },
    Derived,
}

enum AliasResolution {
    Resolved(QualifiedRef),
    Derived,
    Unresolved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QualifiedRef {
    schema: Option<String>,
    table: String,
    column: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QualifiedRefSpan {
    reference: QualifiedRef,
    start: usize,
}

impl QualifiedRef {
    fn display(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.{}.{}", self.table, self.column),
            None => format!("{}.{}", self.table, self.column),
        }
    }
}

/// Extract `table.column` and `schema.table.column` references from a SQL
/// string, skipping content inside string literals, line comments (`-- ...`),
/// and block comments (`/* ... */`).
#[cfg(test)]
fn extract_qualified_refs(sql: &str) -> Vec<QualifiedRef> {
    extract_qualified_ref_spans(sql)
        .into_iter()
        .map(|span| span.reference)
        .collect()
}

fn extract_qualified_ref_spans(sql: &str) -> Vec<QualifiedRefSpan> {
    let mut refs = Vec::new();
    let cast_type_spans = extract_cast_type_spans(sql);
    let bytes = sql.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let c = bytes[i];

        // Single-quoted string literal: skip with '' escape.
        if c == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }

        // Dollar-quoted string literal: skip until the matching delimiter.
        if c == b'$' {
            if let Some(end) = skip_dollar_quoted_string(bytes, i) {
                i = end;
                continue;
            }
        }

        // Line comment.
        if c == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        // Block comment.
        if c == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }

        // Identifier (possibly double-quoted).
        if c == b'"' || is_ident_start(c) {
            let start = i;
            let after_cast_operator = is_after_cast_operator(bytes, i);
            let (id, end) = read_identifier(bytes, i);
            let mut parts = vec![id];
            let mut end = end;

            loop {
                // Skip whitespace between identifier and dot.
                let mut peek = end;
                while peek < bytes.len() && bytes[peek].is_ascii_whitespace() {
                    peek += 1;
                }
                if peek >= bytes.len() || bytes[peek] != b'.' {
                    break;
                }

                let mut after_dot = peek + 1;
                while after_dot < bytes.len() && bytes[after_dot].is_ascii_whitespace() {
                    after_dot += 1;
                }
                if after_dot >= bytes.len()
                    || (bytes[after_dot] != b'"' && !is_ident_start(bytes[after_dot]))
                {
                    break;
                }

                let (id, id_end) = read_identifier(bytes, after_dot);
                parts.push(id);
                end = id_end;
            }

            if after_cast_operator
                || cast_type_spans.iter().any(|span| span.contains(start))
                || (parts.len() >= 2 && next_non_whitespace(bytes, end) == Some(b'('))
            {
                i = end;
                continue;
            }

            match parts.as_slice() {
                [table, column] => refs.push(QualifiedRefSpan {
                    reference: QualifiedRef {
                        schema: None,
                        table: table.clone(),
                        column: column.clone(),
                    },
                    start,
                }),
                [schema, table, column] => refs.push(QualifiedRefSpan {
                    reference: QualifiedRef {
                        schema: Some(schema.clone()),
                        table: table.clone(),
                        column: column.clone(),
                    },
                    start,
                }),
                _ => {}
            }
            i = end;
            continue;
        }

        i += 1;
    }

    refs
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ByteSpan {
    start: usize,
    end: usize,
}

impl ByteSpan {
    fn contains(self, position: usize) -> bool {
        self.start <= position && position < self.end
    }
}

fn extract_cast_type_spans(sql: &str) -> Vec<ByteSpan> {
    let tokens = tokenize_sql(sql);
    let mut spans = Vec::new();

    for i in 0..tokens.len().saturating_sub(1) {
        if !tokens[i].is_keyword("cast") || !tokens[i + 1].is_lparen() {
            continue;
        }

        let lparen = i + 1;
        let Some(rparen) = matching_rparen_index(&tokens, lparen) else {
            continue;
        };
        let cast_arg_depth = tokens[lparen].depth + 1;
        let Some(as_index) = (lparen + 1..rparen)
            .find(|&index| tokens[index].depth == cast_arg_depth && tokens[index].is_keyword("as"))
        else {
            continue;
        };

        let start = tokens
            .get(as_index + 1)
            .map(|token| token.start)
            .unwrap_or(tokens[as_index].end);
        spans.push(ByteSpan {
            start,
            end: tokens[rparen].start,
        });
    }

    spans
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SqlTokenKind {
    Ident { text: String, quoted: bool },
    Dot,
    Comma,
    LParen,
    RParen,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlToken {
    kind: SqlTokenKind,
    start: usize,
    end: usize,
    depth: usize,
}

impl SqlToken {
    fn ident(&self) -> Option<&str> {
        match &self.kind {
            SqlTokenKind::Ident { text, .. } => Some(text),
            _ => None,
        }
    }

    fn is_keyword(&self, keyword: &str) -> bool {
        match &self.kind {
            SqlTokenKind::Ident { text, quoted } => !quoted && text.eq_ignore_ascii_case(keyword),
            _ => false,
        }
    }

    fn is_dot(&self) -> bool {
        matches!(&self.kind, SqlTokenKind::Dot)
    }

    fn is_comma(&self) -> bool {
        matches!(&self.kind, SqlTokenKind::Comma)
    }

    fn is_lparen(&self) -> bool {
        matches!(&self.kind, SqlTokenKind::LParen)
    }

    fn is_rparen(&self) -> bool {
        matches!(&self.kind, SqlTokenKind::RParen)
    }
}

fn extract_table_aliases(sql: &str) -> Vec<TableAlias> {
    let tokens = tokenize_sql(sql);
    let cte_names = extract_cte_names(&tokens);
    let sql_len = sql.len();
    let mut aliases = cte_names
        .iter()
        .cloned()
        .map(|alias| TableAlias {
            alias,
            target: AliasTarget::Derived,
            scope: SqlScope {
                start: 0,
                end: sql_len,
                depth: 0,
            },
        })
        .collect::<Vec<_>>();

    let mut i = 0;
    while i < tokens.len() {
        if tokens[i].is_keyword("update") {
            i = parse_update_target(&tokens, i + 1, &mut aliases, sql_len);
            continue;
        }

        if tokens[i].is_keyword("insert") {
            i = parse_insert_target(&tokens, i + 1, &mut aliases, sql_len);
            continue;
        }

        if tokens[i].is_keyword("delete") {
            i = parse_delete_target(&tokens, i + 1, &mut aliases, sql_len);
            continue;
        }

        if tokens[i].is_keyword("from") {
            i = parse_from_sources(&tokens, i + 1, &cte_names, &mut aliases, sql_len);
            continue;
        }

        if tokens[i].is_keyword("using") {
            i = parse_from_sources(&tokens, i + 1, &cte_names, &mut aliases, sql_len);
            continue;
        }

        if tokens[i].is_keyword("join") {
            i = parse_table_source(&tokens, i + 1, &cte_names, &mut aliases, sql_len);
            continue;
        }

        i += 1;
    }

    aliases
}

fn tokenize_sql(sql: &str) -> Vec<SqlToken> {
    let mut tokens = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut depth = 0_usize;

    while i < bytes.len() {
        let c = bytes[i];

        if c == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }

        if c == b'$' {
            if let Some(end) = skip_dollar_quoted_string(bytes, i) {
                i = end;
                continue;
            }
        }

        if c == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        if c == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }

        if c == b'"' || is_ident_start(c) {
            let (text, quoted, end) = read_identifier_with_quote(bytes, i);
            tokens.push(SqlToken {
                kind: SqlTokenKind::Ident { text, quoted },
                start: i,
                end,
                depth,
            });
            i = end;
            continue;
        }

        match c {
            b'.' => tokens.push(SqlToken {
                kind: SqlTokenKind::Dot,
                start: i,
                end: i + 1,
                depth,
            }),
            b',' => tokens.push(SqlToken {
                kind: SqlTokenKind::Comma,
                start: i,
                end: i + 1,
                depth,
            }),
            b'(' => {
                tokens.push(SqlToken {
                    kind: SqlTokenKind::LParen,
                    start: i,
                    end: i + 1,
                    depth,
                });
                depth += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                tokens.push(SqlToken {
                    kind: SqlTokenKind::RParen,
                    start: i,
                    end: i + 1,
                    depth,
                });
            }
            _ => {}
        }
        i += 1;
    }

    tokens
}

fn extract_cte_names(tokens: &[SqlToken]) -> Vec<String> {
    let mut names = Vec::new();
    let mut i = 0;

    while i < tokens.len() {
        if !tokens[i].is_keyword("with") {
            i += 1;
            continue;
        }

        i += 1;
        if i < tokens.len() && tokens[i].is_keyword("recursive") {
            i += 1;
        }

        loop {
            let Some(name) = tokens.get(i).and_then(SqlToken::ident) else {
                break;
            };
            names.push(name.to_owned());
            i += 1;

            if tokens.get(i).is_some_and(SqlToken::is_lparen) {
                i = skip_balanced(tokens, i);
            }

            if !tokens.get(i).is_some_and(|token| token.is_keyword("as")) {
                break;
            }
            i += 1;

            while tokens
                .get(i)
                .is_some_and(|token| token.is_keyword("materialized") || token.is_keyword("not"))
            {
                i += 1;
            }

            if tokens.get(i).is_some_and(SqlToken::is_lparen) {
                i = skip_balanced(tokens, i);
            }

            if tokens.get(i).is_some_and(SqlToken::is_comma) {
                i += 1;
                continue;
            }

            break;
        }
    }

    names
}

struct ParsedAlias {
    name: String,
    token_index: usize,
}

fn parse_update_target(
    tokens: &[SqlToken],
    i: usize,
    aliases: &mut Vec<TableAlias>,
    sql_len: usize,
) -> usize {
    let Some((target, next, _target_scope)) = parse_dml_table_target(tokens, i, sql_len) else {
        return i.saturating_add(1);
    };

    let (alias, next) = parse_optional_alias(tokens, next);
    if let Some(alias) = alias {
        aliases.push(TableAlias {
            alias: alias.name,
            target,
            scope: scope_for_token(tokens, alias.token_index, sql_len),
        });
    }
    next
}

fn parse_insert_target(
    tokens: &[SqlToken],
    mut i: usize,
    aliases: &mut Vec<TableAlias>,
    sql_len: usize,
) -> usize {
    if !tokens.get(i).is_some_and(|token| token.is_keyword("into")) {
        return i.saturating_add(1);
    }
    i += 1;

    let Some((target, next, target_scope)) = parse_dml_table_target(tokens, i, sql_len) else {
        return i.saturating_add(1);
    };

    let (alias, next) = parse_optional_alias(tokens, next);
    if let Some(alias) = alias {
        aliases.push(TableAlias {
            alias: alias.name,
            target: target.clone(),
            scope: scope_for_token(tokens, alias.token_index, sql_len),
        });
    }
    aliases.push(TableAlias {
        alias: "excluded".to_owned(),
        target,
        scope: target_scope,
    });
    next
}

fn parse_delete_target(
    tokens: &[SqlToken],
    mut i: usize,
    aliases: &mut Vec<TableAlias>,
    sql_len: usize,
) -> usize {
    if !tokens.get(i).is_some_and(|token| token.is_keyword("from")) {
        return i.saturating_add(1);
    }
    i += 1;

    let Some((target, next, _target_scope)) = parse_dml_table_target(tokens, i, sql_len) else {
        return i.saturating_add(1);
    };

    let (alias, next) = parse_optional_alias(tokens, next);
    if let Some(alias) = alias {
        aliases.push(TableAlias {
            alias: alias.name,
            target,
            scope: scope_for_token(tokens, alias.token_index, sql_len),
        });
    }
    next
}

fn parse_dml_table_target(
    tokens: &[SqlToken],
    mut i: usize,
    sql_len: usize,
) -> Option<(AliasTarget, usize, SqlScope)> {
    if tokens.get(i).is_some_and(|token| token.is_keyword("only")) {
        i += 1;
    }

    let mut parts = Vec::new();
    let scope = scope_for_token(tokens, i, sql_len);
    let first_token = tokens.get(i)?;
    if is_dml_target_stop(first_token) {
        return None;
    }
    let first = first_token.ident()?;
    parts.push(first.to_owned());
    i += 1;

    while tokens.get(i).is_some_and(SqlToken::is_dot) {
        let Some(part) = tokens.get(i + 1).and_then(SqlToken::ident) else {
            break;
        };
        parts.push(part.to_owned());
        i += 2;
    }

    let table = parts.pop().unwrap_or_default();
    let schema = parts.pop();
    Some((AliasTarget::Table { schema, table }, i, scope))
}

fn is_dml_target_stop(token: &SqlToken) -> bool {
    [
        "do",
        "from",
        "on",
        "returning",
        "select",
        "set",
        "using",
        "values",
        "where",
    ]
    .iter()
    .any(|keyword| token.is_keyword(keyword))
}

fn parse_from_sources(
    tokens: &[SqlToken],
    mut i: usize,
    cte_names: &[String],
    aliases: &mut Vec<TableAlias>,
    sql_len: usize,
) -> usize {
    loop {
        i = parse_table_source(tokens, i, cte_names, aliases, sql_len);
        if tokens.get(i).is_some_and(SqlToken::is_comma) {
            i += 1;
            continue;
        }
        return i;
    }
}

fn parse_table_source(
    tokens: &[SqlToken],
    mut i: usize,
    cte_names: &[String],
    aliases: &mut Vec<TableAlias>,
    sql_len: usize,
) -> usize {
    if tokens
        .get(i)
        .is_some_and(|token| token.is_keyword("lateral"))
    {
        i += 1;
    }

    if tokens.get(i).is_some_and(SqlToken::is_lparen) {
        i = skip_balanced(tokens, i);
        let (alias, next) = parse_optional_alias(tokens, i);
        if let Some(alias) = alias {
            aliases.push(TableAlias {
                alias: alias.name,
                target: AliasTarget::Derived,
                scope: scope_for_token(tokens, alias.token_index, sql_len),
            });
        }
        return skip_column_alias_list(tokens, next);
    }

    let mut parts = Vec::new();
    let source_token_index = i;
    let Some(first) = tokens.get(i).and_then(SqlToken::ident) else {
        return i.saturating_add(1);
    };
    parts.push(first.to_owned());
    i += 1;

    while tokens.get(i).is_some_and(SqlToken::is_dot) {
        let Some(part) = tokens.get(i + 1).and_then(SqlToken::ident) else {
            break;
        };
        parts.push(part.to_owned());
        i += 2;
    }

    if tokens.get(i).is_some_and(SqlToken::is_lparen) {
        i = skip_balanced(tokens, i);
        let (alias, next) = parse_optional_alias(tokens, i);
        if let Some(alias) = alias {
            aliases.push(TableAlias {
                alias: alias.name,
                target: AliasTarget::Derived,
                scope: scope_for_token(tokens, alias.token_index, sql_len),
            });
        }
        return skip_column_alias_list(tokens, next);
    }

    let target = if parts.len() == 1 && cte_names.iter().any(|name| name == &parts[0]) {
        AliasTarget::Derived
    } else {
        let table = parts.pop().unwrap_or_default();
        let schema = parts.pop();
        AliasTarget::Table { schema, table }
    };
    let implicit_alias = match &target {
        AliasTarget::Table { table, .. } => Some(table.clone()),
        AliasTarget::Derived => None,
    };

    let (alias, next) = parse_optional_alias(tokens, i);
    if let Some(alias) = alias {
        aliases.push(TableAlias {
            alias: alias.name,
            target,
            scope: scope_for_token(tokens, alias.token_index, sql_len),
        });
    } else if let Some(implicit_alias) = implicit_alias {
        aliases.push(TableAlias {
            alias: implicit_alias,
            target,
            scope: scope_for_token(tokens, source_token_index, sql_len),
        });
    }
    skip_column_alias_list(tokens, next)
}

fn parse_optional_alias(tokens: &[SqlToken], mut i: usize) -> (Option<ParsedAlias>, usize) {
    if tokens.get(i).is_some_and(|token| token.is_keyword("as")) {
        i += 1;
        let Some(alias) = tokens.get(i).and_then(SqlToken::ident) else {
            return (None, i);
        };
        return (
            Some(ParsedAlias {
                name: alias.to_owned(),
                token_index: i,
            }),
            i + 1,
        );
    }

    let Some(alias) = tokens.get(i).and_then(SqlToken::ident) else {
        return (None, i);
    };

    if is_alias_stop(tokens.get(i).unwrap()) {
        return (None, i);
    }

    (
        Some(ParsedAlias {
            name: alias.to_owned(),
            token_index: i,
        }),
        i + 1,
    )
}

fn is_alias_stop(token: &SqlToken) -> bool {
    [
        "on",
        "using",
        "where",
        "join",
        "inner",
        "left",
        "right",
        "full",
        "cross",
        "natural",
        "conflict",
        "default",
        "group",
        "order",
        "having",
        "limit",
        "offset",
        "fetch",
        "union",
        "except",
        "intersect",
        "returning",
        "set",
        "overriding",
        "values",
        "window",
        "for",
    ]
    .iter()
    .any(|keyword| token.is_keyword(keyword))
}

fn skip_column_alias_list(tokens: &[SqlToken], i: usize) -> usize {
    if tokens.get(i).is_some_and(SqlToken::is_lparen) {
        skip_balanced(tokens, i)
    } else {
        i
    }
}

fn scope_for_token(tokens: &[SqlToken], token_index: usize, sql_len: usize) -> SqlScope {
    let Some(token) = tokens.get(token_index) else {
        return SqlScope {
            start: 0,
            end: sql_len,
            depth: 0,
        };
    };
    let depth = token.depth;

    let (start, end) = if depth == 0 {
        (0, sql_len)
    } else {
        let start = (0..=token_index)
            .rev()
            .find(|&i| tokens[i].is_lparen() && tokens[i].depth + 1 == depth)
            .map(|i| tokens[i].end)
            .unwrap_or(0);
        let end = (token_index + 1..tokens.len())
            .find(|&i| tokens[i].is_rparen() && tokens[i].depth + 1 == depth)
            .map(|i| tokens[i].start)
            .unwrap_or(sql_len);
        (start, end)
    };

    let start = (0..token_index)
        .rev()
        .take_while(|&i| tokens[i].end >= start)
        .find(|&i| tokens[i].depth == depth && is_set_operation_boundary(&tokens[i]))
        .map(|i| tokens[i].end)
        .unwrap_or(start);
    let end = (token_index + 1..tokens.len())
        .take_while(|&i| tokens[i].start <= end)
        .find(|&i| tokens[i].depth == depth && is_set_operation_boundary(&tokens[i]))
        .map(|i| tokens[i].start)
        .unwrap_or(end);

    SqlScope { start, end, depth }
}

fn is_set_operation_boundary(token: &SqlToken) -> bool {
    token.is_keyword("union") || token.is_keyword("except") || token.is_keyword("intersect")
}

fn skip_balanced(tokens: &[SqlToken], mut i: usize) -> usize {
    let mut depth = 0_usize;
    while i < tokens.len() {
        match &tokens[i].kind {
            SqlTokenKind::LParen => depth += 1,
            SqlTokenKind::RParen => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return i + 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    i
}

fn matching_rparen_index(tokens: &[SqlToken], mut i: usize) -> Option<usize> {
    if !tokens.get(i).is_some_and(SqlToken::is_lparen) {
        return None;
    }

    let mut depth = 0_usize;
    while i < tokens.len() {
        match &tokens[i].kind {
            SqlTokenKind::LParen => depth += 1,
            SqlTokenKind::RParen => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }

    None
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_continue(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

fn skip_dollar_quoted_string(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'$') {
        return None;
    }

    let mut tag_end = start + 1;
    while tag_end < bytes.len() && bytes[tag_end] != b'$' {
        if !bytes[tag_end].is_ascii_alphanumeric() && bytes[tag_end] != b'_' {
            return None;
        }
        tag_end += 1;
    }

    if tag_end >= bytes.len() {
        return None;
    }

    let delimiter = &bytes[start..=tag_end];
    let mut i = tag_end + 1;
    while i + delimiter.len() <= bytes.len() {
        if &bytes[i..i + delimiter.len()] == delimiter {
            return Some(i + delimiter.len());
        }
        i += 1;
    }

    Some(bytes.len())
}

fn next_non_whitespace(bytes: &[u8], mut i: usize) -> Option<u8> {
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    bytes.get(i).copied()
}

fn is_after_cast_operator(bytes: &[u8], start: usize) -> bool {
    let mut i = start;
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    i >= 2 && bytes[i - 2] == b':' && bytes[i - 1] == b':'
}

fn read_identifier_with_quote(bytes: &[u8], start: usize) -> (String, bool, usize) {
    if bytes[start] == b'"' {
        let mut i = start + 1;
        let mut id = String::new();
        while i < bytes.len() {
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    id.push('"');
                    i += 2;
                } else {
                    i += 1;
                    break;
                }
            } else {
                id.push(bytes[i] as char);
                i += 1;
            }
        }
        (id, true, i)
    } else {
        let mut i = start;
        while i < bytes.len() && is_ident_continue(bytes[i]) {
            i += 1;
        }
        let id = std::str::from_utf8(&bytes[start..i])
            .unwrap_or("")
            .to_ascii_lowercase();
        (id, false, i)
    }
}

fn read_identifier(bytes: &[u8], start: usize) -> (String, usize) {
    let (id, _, end) = read_identifier_with_quote(bytes, start);
    (id, end)
}

#[cfg(test)]
mod tests {
    use super::{extract_qualified_refs, validate_qualified_refs, QualifiedRef};
    use fuwa_codegen::{ColumnDef, DatabaseSchema, RustType, TableDef};

    fn table_column(table: &str, column: &str) -> QualifiedRef {
        QualifiedRef {
            schema: None,
            table: table.into(),
            column: column.into(),
        }
    }

    fn schema_table_column(schema: &str, table: &str, column: &str) -> QualifiedRef {
        QualifiedRef {
            schema: Some(schema.into()),
            table: table.into(),
            column: column.into(),
        }
    }

    fn test_schema() -> DatabaseSchema {
        DatabaseSchema {
            tables: vec![
                TableDef {
                    schema: "public".into(),
                    name: "users".into(),
                    columns: vec![column("id"), column("email"), column("active")],
                    primary_key: Vec::new(),
                    uniques: Vec::new(),
                },
                TableDef {
                    schema: "public".into(),
                    name: "posts".into(),
                    columns: vec![column("id"), column("user_id"), column("title")],
                    primary_key: Vec::new(),
                    uniques: Vec::new(),
                },
            ],
            enums: Vec::new(),
        }
    }

    fn column(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            ordinal_position: 1,
            pg_type: "int8".into(),
            pg_type_kind: "base".into(),
            rust_type: RustType::new("i64"),
            nullable: false,
            default_expression: None,
            primary_key: false,
            unique: false,
            relation: None,
        }
    }

    #[test]
    fn picks_up_basic_qualified_refs() {
        let refs = extract_qualified_refs(
            r#"select users.id, users.email from users where users.active = true"#,
        );
        assert_eq!(
            refs,
            vec![
                table_column("users", "id"),
                table_column("users", "email"),
                table_column("users", "active"),
            ]
        );
    }

    #[test]
    fn picks_up_schema_qualified_refs() {
        let refs = extract_qualified_refs(
            r#"select public.users.id, "public" . "users" . "email" from users"#,
        );
        assert_eq!(
            refs,
            vec![
                schema_table_column("public", "users", "id"),
                schema_table_column("public", "users", "email"),
            ]
        );
    }

    #[test]
    fn skips_string_literals_and_comments() {
        let refs = extract_qualified_refs(
            r#"-- foo.bar in a line comment
               /* baz.qux in a block comment */
               select users.id from users where users.email = 'evil.fake@x'"#,
        );
        assert_eq!(
            refs,
            vec![table_column("users", "id"), table_column("users", "email"),]
        );
    }

    #[test]
    fn skips_dollar_quoted_string_literals() {
        let refs = extract_qualified_refs(
            r#"select users.id, $$ users.missing $$, $tag$ public.users.missing $tag$
               from users"#,
        );
        assert_eq!(refs, vec![table_column("users", "id")]);
    }

    #[test]
    fn skips_schema_qualified_calls_and_cast_types() {
        let refs = extract_qualified_refs(
            r#"select pg_catalog.lower(users.email), $1::public.my_type from users"#,
        );
        assert_eq!(refs, vec![table_column("users", "email")]);
    }

    #[test]
    fn skips_schema_qualified_cast_types_in_cast_expressions() {
        let refs = extract_qualified_refs(
            r#"select cast($1 as public.my_type), cast(users.id as public.my_type) from users"#,
        );
        assert_eq!(refs, vec![table_column("users", "id")]);

        validate_qualified_refs(
            r#"select cast($1 as public.my_type), cast(users.id as public.my_type) from users"#,
            &test_schema(),
        )
        .unwrap();
    }

    #[test]
    fn handles_double_quoted_identifiers() {
        let refs = extract_qualified_refs(r#"select "Users"."id" from "Users""#);
        assert_eq!(refs, vec![table_column("Users", "id")]);
    }

    #[test]
    fn folds_unquoted_identifiers_to_lowercase() {
        let refs = extract_qualified_refs(r#"select Users.ID from Users where Users.ACTIVE"#);
        assert_eq!(
            refs,
            vec![table_column("users", "id"), table_column("users", "active")]
        );

        validate_qualified_refs(r#"select Users.ID from Users"#, &test_schema()).unwrap();
    }

    #[test]
    fn keeps_quoted_identifiers_case_sensitive() {
        let err = validate_qualified_refs(r#"select "Users"."id" from "Users""#, &test_schema())
            .unwrap_err();
        assert!(err.contains("Users.id"));
    }

    #[test]
    fn skips_decimal_literals() {
        let refs = extract_qualified_refs(r#"select 1.5, users.id from users"#);
        assert_eq!(refs, vec![table_column("users", "id")]);
    }

    #[test]
    fn validates_schema_qualified_columns_and_tables() {
        let schema = test_schema();

        validate_qualified_refs(
            r#"select public.users.id from public.users where public.users.active = true"#,
            &schema,
        )
        .unwrap();
    }

    #[test]
    fn resolves_table_aliases_before_validating_columns() {
        let schema = test_schema();

        validate_qualified_refs(
            r#"select u.id, u.email from users u where u.active = true"#,
            &schema,
        )
        .unwrap();

        validate_qualified_refs(
            r#"select u.id from public.users as u where u.active = true"#,
            &schema,
        )
        .unwrap();
    }

    #[test]
    fn resolves_unaliased_schema_qualified_sources_as_implicit_aliases() {
        let mut schema = test_schema();
        schema.tables.push(TableDef {
            schema: "auth".into(),
            name: "users".into(),
            columns: vec![column("id")],
            primary_key: Vec::new(),
            uniques: Vec::new(),
        });

        validate_qualified_refs(r#"select users.id from auth.users"#, &schema).unwrap();

        let err =
            validate_qualified_refs(r#"select users.email from auth.users"#, &schema).unwrap_err();
        assert!(err.contains("users.email"));
    }

    #[test]
    fn resolves_nested_aliases_in_their_sql_scope() {
        let schema = test_schema();

        validate_qualified_refs(
            r#"select u.email from users u
               where exists (
                   select 1 from posts u
                   where u.user_id = users.id
               )"#,
            &schema,
        )
        .unwrap();
    }

    #[test]
    fn resolves_reused_aliases_in_sibling_subqueries_independently() {
        let schema = test_schema();

        validate_qualified_refs(
            r#"select
                   exists (select 1 from users u where u.email = $1),
                   exists (select 1 from posts u where u.title = $2)"#,
            &schema,
        )
        .unwrap();
    }

    #[test]
    fn resolves_reused_aliases_across_set_operation_operands_independently() {
        let schema = test_schema();

        validate_qualified_refs(
            r#"select u.email from users u
               union all
               select u.title from posts u"#,
            &schema,
        )
        .unwrap();
    }

    #[test]
    fn rejects_unknown_columns_through_table_aliases() {
        let schema = test_schema();

        let err = validate_qualified_refs(r#"select u.missing from users u"#, &schema).unwrap_err();
        assert!(err.contains("u.missing"));
    }

    #[test]
    fn resolves_dml_target_aliases_before_validating_columns() {
        let schema = test_schema();

        validate_qualified_refs(
            r#"update users as u set email = $1 where u.id = $2 returning u.email"#,
            &schema,
        )
        .unwrap();

        validate_qualified_refs(
            r#"insert into users as u (id, email, active) values ($1, $2, $3)
               on conflict (email) do update set email = excluded.email
               where u.active = true"#,
            &schema,
        )
        .unwrap();

        validate_qualified_refs(
            r#"delete from users as u where u.active = false returning u.id"#,
            &schema,
        )
        .unwrap();

        validate_qualified_refs(
            r#"delete from users as u using users as other
               where other.id = u.id returning u.email"#,
            &schema,
        )
        .unwrap();
    }

    #[test]
    fn rejects_unknown_columns_through_dml_aliases() {
        let schema = test_schema();

        let err = validate_qualified_refs(
            r#"update users as u set email = $1 where u.missing = $2"#,
            &schema,
        )
        .unwrap_err();
        assert!(err.contains("u.missing"));

        let err = validate_qualified_refs(
            r#"insert into users (id, email, active) values ($1, $2, $3)
               on conflict (email) do update set email = excluded.missing"#,
            &schema,
        )
        .unwrap_err();
        assert!(err.contains("excluded.missing"));
    }

    #[test]
    fn skips_derived_and_cte_alias_refs() {
        let schema = test_schema();

        validate_qualified_refs(
            r#"select u.id from (select users.id from users) u"#,
            &schema,
        )
        .unwrap();

        validate_qualified_refs(
            r#"with active_users as (select users.id from users) select active_users.id from active_users"#,
            &schema,
        )
        .unwrap();
    }

    #[test]
    fn validation_ignores_qualified_calls_casts_and_dollar_quoted_literals() {
        let schema = test_schema();

        validate_qualified_refs(
            r#"select pg_catalog.lower(users.email), $1::public.my_type, $$ users.missing $$
               from users where users.active = true"#,
            &schema,
        )
        .unwrap();
    }

    #[test]
    fn rejects_unknown_schema_qualified_columns() {
        let schema = test_schema();

        let err =
            validate_qualified_refs(r#"select public.users.missing from public.users"#, &schema)
                .unwrap_err();
        assert!(err.contains("public.users.missing"));

        let err = validate_qualified_refs(r#"select auth.users.id from public.users"#, &schema)
            .unwrap_err();
        assert!(err.contains("auth.users.id"));
    }
}
