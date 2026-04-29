use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use fuwa_core::{Error, Result};

use crate::{
    rust_type_ident, rust_variant_ident, ColumnDef, DatabaseSchema, EnumDef, EnumVariantDef,
    RelationDef, RustType, TableDef,
};

pub(crate) fn schema_from_prisma_file(path: &Path) -> Result<DatabaseSchema> {
    let source = std::fs::read_to_string(path)?;
    parse_prisma_schema(&source)
}

pub(crate) fn schema_from_prisma_file_with_default_schema(
    path: &Path,
    cli_default_schema: Option<&str>,
) -> Result<DatabaseSchema> {
    let source = std::fs::read_to_string(path)?;
    parse_prisma_schema_with_default_schema(&source, cli_default_schema)
}

pub(crate) fn parse_prisma_schema(source: &str) -> Result<DatabaseSchema> {
    parse_prisma_schema_with_default_schema(source, None)
}

pub(crate) fn parse_prisma_schema_with_default_schema(
    source: &str,
    cli_default_schema: Option<&str>,
) -> Result<DatabaseSchema> {
    let blocks = extract_blocks(source)?;
    let datasource = validate_postgres_datasource(&blocks)?;
    let default_schema = datasource
        .default_schema
        .as_deref()
        .or(cli_default_schema)
        .unwrap_or("public");

    let mut enums = Vec::new();
    let mut enum_names = BTreeSet::new();
    let mut enum_db_names = BTreeMap::new();

    for block in blocks.iter().filter(|block| block.kind == "enum") {
        let enum_def = parse_enum_block(block, default_schema)?;
        enum_names.insert(block.name.clone());
        enum_db_names.insert(
            block.name.clone(),
            (enum_def.schema.clone(), enum_def.name.clone()),
        );
        enums.push(enum_def);
    }

    let model_names: BTreeSet<String> = blocks
        .iter()
        .filter(|block| block.kind == "model")
        .map(|block| block.name.clone())
        .collect();
    let mut model_field_columns = BTreeMap::new();
    for block in blocks
        .iter()
        .filter(|block| block.kind == "model" && !model_is_ignored(block))
    {
        model_field_columns.insert(
            block.name.clone(),
            prisma_field_column_map(block, &model_names)?,
        );
    }

    let mut tables = Vec::new();
    for block in blocks
        .iter()
        .filter(|block| block.kind == "model" && !model_is_ignored(block))
    {
        tables.push(parse_model_block(
            block,
            &datasource.name,
            default_schema,
            &model_names,
            &model_field_columns,
            &enum_names,
            &enum_db_names,
        )?);
    }

    Ok(DatabaseSchema { tables, enums })
}

#[derive(Debug)]
struct Block {
    kind: String,
    name: String,
    body: Vec<String>,
}

#[derive(Debug)]
struct DatasourceConfig {
    name: String,
    default_schema: Option<String>,
}

fn extract_blocks(source: &str) -> Result<Vec<Block>> {
    let source = strip_comments(source);
    let source = normalize_block_braces(&source);
    let lines: Vec<String> = source.lines().map(str::to_owned).collect();
    let mut blocks = Vec::new();
    let mut index = 0;

    while index < lines.len() {
        let line = lines[index].trim();
        let Some((kind, name)) = block_header(line) else {
            index += 1;
            continue;
        };

        let mut body = Vec::new();
        let mut depth = brace_delta(line);
        index += 1;

        while index < lines.len() && depth > 0 {
            let line = &lines[index];
            depth += brace_delta(line);
            if depth > 0 {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    body.push(trimmed.to_owned());
                }
            }
            index += 1;
        }

        if depth != 0 {
            return Err(Error::codegen(format!(
                "unterminated Prisma {kind} block {name}"
            )));
        }

        let body = coalesce_body_lines(&kind, &name, body)?;
        blocks.push(Block { kind, name, body });
    }

    Ok(blocks)
}

fn normalize_block_braces(source: &str) -> String {
    let mut in_string = false;
    let mut escaped = false;
    let mut out = String::with_capacity(source.len());

    for ch in source.chars() {
        if escaped {
            out.push(ch);
            escaped = false;
            continue;
        }

        if in_string {
            out.push(ch);
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => {
                in_string = true;
                out.push(ch);
            }
            '{' => {
                out.push(ch);
                out.push('\n');
            }
            '}' => {
                if !out.ends_with('\n') {
                    out.push('\n');
                }
                out.push(ch);
                out.push('\n');
            }
            _ => out.push(ch),
        }
    }

    out
}

#[derive(Default)]
struct LogicalLineState {
    parens: i32,
    brackets: i32,
    in_string: bool,
    escaped: bool,
}

impl LogicalLineState {
    fn update(&mut self, line: &str) {
        for ch in line.chars() {
            if self.escaped {
                self.escaped = false;
                continue;
            }

            match ch {
                '\\' if self.in_string => self.escaped = true,
                '"' => self.in_string = !self.in_string,
                '(' if !self.in_string => self.parens += 1,
                ')' if !self.in_string => self.parens -= 1,
                '[' if !self.in_string => self.brackets += 1,
                ']' if !self.in_string => self.brackets -= 1,
                _ => {}
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.parens == 0 && self.brackets == 0 && !self.in_string
    }
}

fn coalesce_body_lines(kind: &str, name: &str, lines: Vec<String>) -> Result<Vec<String>> {
    let mut logical_lines = Vec::new();
    let mut current = String::new();
    let mut state = LogicalLineState::default();

    for line in lines {
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(line.trim());
        state.update(&line);

        if state.is_complete() {
            logical_lines.push(std::mem::take(&mut current));
            state = LogicalLineState::default();
        }
    }

    if !current.is_empty() {
        return Err(Error::codegen(format!(
            "unterminated multiline Prisma declaration in {kind} block {name}: {current:?}"
        )));
    }

    Ok(logical_lines)
}

fn block_header(line: &str) -> Option<(String, String)> {
    let open = find_unquoted_char(line, '{')?;
    let mut parts = line[..open].split_whitespace();
    let kind = parts.next()?;
    if !matches!(kind, "datasource" | "generator" | "model" | "enum") {
        return None;
    }
    let name = parts.next()?;
    Some((kind.to_owned(), name.to_owned()))
}

fn find_unquoted_char(line: &str, target: char) -> Option<usize> {
    let mut in_string = false;
    let mut escaped = false;

    for (index, ch) in line.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }

        match ch {
            '\\' if in_string => escaped = true,
            '"' => in_string = !in_string,
            ch if ch == target && !in_string => return Some(index),
            _ => {}
        }
    }

    None
}

fn brace_delta(line: &str) -> i32 {
    let mut in_string = false;
    let mut escaped = false;
    let mut delta = 0;

    for ch in line.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_string => escaped = true,
            '"' => in_string = !in_string,
            '{' if !in_string => delta += 1,
            '}' if !in_string => delta -= 1,
            _ => {}
        }
    }

    delta
}

fn strip_comments(source: &str) -> String {
    let mut in_string = false;
    let mut escaped = false;
    let mut out = String::new();
    let mut chars = source.chars().peekable();

    while let Some(ch) = chars.next() {
        if escaped {
            out.push(ch);
            escaped = false;
            continue;
        }

        if in_string {
            out.push(ch);
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => {
                in_string = true;
                out.push(ch);
            }
            '/' if chars.peek().is_some_and(|next| *next == '/') => {
                chars.next();
                for comment_ch in chars.by_ref() {
                    if comment_ch == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            '/' if chars.peek().is_some_and(|next| *next == '*') => {
                chars.next();
                out.push(' ');
                let mut previous_star = false;
                for comment_ch in chars.by_ref() {
                    if comment_ch == '\n' {
                        out.push('\n');
                    }
                    if previous_star && comment_ch == '/' {
                        break;
                    }
                    previous_star = comment_ch == '*';
                }
            }
            _ => out.push(ch),
        }
    }

    out
}

fn validate_postgres_datasource(blocks: &[Block]) -> Result<DatasourceConfig> {
    let Some(datasource) = blocks.iter().find(|block| block.kind == "datasource") else {
        return Ok(DatasourceConfig {
            name: "db".to_owned(),
            default_schema: None,
        });
    };

    let provider = datasource
        .body
        .iter()
        .find_map(|line| assignment_string(line, "provider"));
    match provider.as_deref() {
        Some("postgresql") | None => Ok(DatasourceConfig {
            name: datasource.name.clone(),
            default_schema: datasource_default_schema(datasource),
        }),
        Some(other) => Err(Error::codegen(format!(
            "fuwa-codegen Prisma source only supports provider = \"postgresql\", got {other:?}"
        ))),
    }
}

fn datasource_default_schema(datasource: &Block) -> Option<String> {
    datasource.body.iter().find_map(|line| {
        let value = assignment_value(line, "url")?;
        let database_url = parse_string_literal(value)
            .ok()
            .or_else(|| datasource_env_url(value))?;
        postgres_url_default_schema(&database_url)
    })
}

fn datasource_env_url(value: &str) -> Option<String> {
    let args = function_args(value, "env")?;
    let env_name = split_top_level(&args, ',').into_iter().next()?;
    let env_name = parse_string_literal(env_name).ok()?;
    std::env::var(env_name).ok()
}

fn postgres_url_default_schema(database_url: &str) -> Option<String> {
    let query = database_url.split_once('?')?.1.split('#').next()?;
    for param in query.split('&') {
        let (key, value) = param.split_once('=').unwrap_or((param, ""));
        if percent_decode(key).as_deref() == Some("schema") {
            let schema = percent_decode(value)?;
            if !schema.is_empty() {
                return Some(schema);
            }
        }
    }
    None
}

fn model_is_ignored(block: &Block) -> bool {
    block.body.iter().any(|line| line.starts_with("@@ignore"))
}

fn parse_enum_block(block: &Block, default_schema: &str) -> Result<EnumDef> {
    let mut db_name = block.name.clone();
    let mut schema = default_schema.to_owned();
    let mut variants = Vec::new();

    for line in &block.body {
        if line.starts_with("@@map") {
            db_name = attr_string_arg(line, "@@map")?;
        } else if line.starts_with("@@schema") {
            schema = attr_string_arg(line, "@@schema")?;
        } else if line.starts_with('@') {
            continue;
        } else {
            let mut parts = line.split_whitespace();
            let Some(name) = parts.next() else {
                continue;
            };
            let mapped = attr_string_arg_optional(line, "@map")?.unwrap_or_else(|| name.to_owned());
            variants.push(EnumVariantDef {
                rust_name: rust_variant_ident(name),
                db_name: mapped,
            });
        }
    }

    if variants.is_empty() {
        return Err(Error::codegen(format!(
            "Prisma enum {} has no variants",
            block.name
        )));
    }

    Ok(EnumDef {
        schema,
        name: db_name,
        rust_name: rust_type_ident(&block.name),
        variants,
    })
}

fn parse_model_block(
    block: &Block,
    datasource_name: &str,
    default_schema: &str,
    model_names: &BTreeSet<String>,
    model_field_columns: &BTreeMap<String, BTreeMap<String, String>>,
    enum_names: &BTreeSet<String>,
    enum_db_names: &BTreeMap<String, (String, String)>,
) -> Result<TableDef> {
    let mut table_name = block.name.clone();
    let mut schema = default_schema.to_owned();
    let mut columns = Vec::new();
    let mut primary_key = Vec::new();
    let mut primary_key_uses_prisma_fields = false;
    let mut uniques = Vec::new();
    let mut pending_relations = Vec::new();

    for line in &block.body {
        if line.starts_with("@@map") {
            table_name = attr_string_arg(line, "@@map")?;
        } else if line.starts_with("@@schema") {
            schema = attr_string_arg(line, "@@schema")?;
        } else if line.starts_with("@@id") {
            primary_key = attr_field_list_arg(line, "@@id", None)?;
            primary_key_uses_prisma_fields = true;
        } else if line.starts_with("@@unique") {
            uniques.push((attr_field_list_arg(line, "@@unique", None)?, true));
        }
    }

    for line in &block.body {
        if line.starts_with('@') {
            continue;
        }
        if has_attr(line, "@ignore") {
            continue;
        }

        let Some(field) = parse_field_line(line)? else {
            continue;
        };

        if model_names.contains(&field.base_type) {
            if let Some(relation) = parse_relation_field(&field)? {
                pending_relations.push(relation);
            }
            continue;
        }

        let column_name = attr_string_arg_optional(line, "@map")?.unwrap_or(field.name.clone());
        let mapped = map_prisma_field(&field, datasource_name, enum_names, enum_db_names)?;
        let mut column = ColumnDef {
            name: column_name.clone(),
            ordinal_position: (columns.len() + 1)
                .try_into()
                .map_err(|_| Error::codegen("too many Prisma model fields"))?,
            pg_type: mapped.pg_type,
            pg_type_kind: mapped.pg_type_kind,
            rust_type: mapped.rust_type,
            nullable: field.nullable,
            default_expression: attr_raw_arg_optional(line, "@default")?,
            primary_key: has_attr(line, "@id"),
            unique: has_attr(line, "@unique"),
            relation: None,
        };

        if column.primary_key && primary_key.is_empty() {
            primary_key.push(column_name.clone());
        }
        if column.unique {
            uniques.push((vec![column_name.clone()], false));
        }

        column.name = column_name;
        columns.push(column);
    }

    let prisma_to_column = model_field_columns
        .get(&block.name)
        .ok_or_else(|| Error::codegen(format!("missing Prisma field map for {}", block.name)))?;

    if primary_key_uses_prisma_fields {
        primary_key = map_constraint_fields(primary_key, &prisma_to_column);
        for column in &mut columns {
            column.primary_key = primary_key.iter().any(|field| field == &column.name);
        }
    }
    let uniques = uniques
        .into_iter()
        .map(|(fields, uses_prisma_fields)| {
            if uses_prisma_fields {
                map_constraint_fields(fields, &prisma_to_column)
            } else {
                fields
            }
        })
        .collect();

    for relation in pending_relations {
        let relation_fields: Vec<String> = relation
            .fields
            .iter()
            .filter_map(|field| prisma_to_column.get(field).cloned())
            .collect();
        let relation_references = model_field_columns
            .get(&relation.model)
            .map(|target_prisma_to_column| {
                map_constraint_fields(relation.references.clone(), target_prisma_to_column)
            })
            .unwrap_or_else(|| relation.references.clone());

        for column_name in &relation_fields {
            if let Some(column) = columns
                .iter_mut()
                .find(|column| column.name == *column_name)
            {
                column.relation = Some(RelationDef {
                    model: relation.model.clone(),
                    fields: relation_fields.clone(),
                    references: relation_references.clone(),
                });
            }
        }
    }

    if primary_key.is_empty() {
        primary_key.extend(
            columns
                .iter()
                .filter(|column| column.primary_key)
                .map(|column| column.name.clone()),
        );
    }

    Ok(TableDef {
        schema,
        name: table_name,
        columns,
        primary_key,
        uniques,
    })
}

fn map_constraint_fields(
    fields: Vec<String>,
    prisma_to_column: &BTreeMap<String, String>,
) -> Vec<String> {
    fields
        .into_iter()
        .map(|field| prisma_to_column.get(&field).cloned().unwrap_or(field))
        .collect()
}

fn prisma_field_column_map(
    block: &Block,
    model_names: &BTreeSet<String>,
) -> Result<BTreeMap<String, String>> {
    let mut prisma_to_column = BTreeMap::new();
    for line in &block.body {
        if line.starts_with('@') || has_attr(line, "@ignore") {
            continue;
        }
        let Some(field) = parse_field_line(line)? else {
            continue;
        };
        if model_names.contains(&field.base_type) {
            continue;
        }
        let column = attr_string_arg_optional(line, "@map")?.unwrap_or_else(|| field.name.clone());
        prisma_to_column.insert(field.name, column);
    }
    Ok(prisma_to_column)
}

#[derive(Debug)]
struct ParsedField {
    name: String,
    base_type: String,
    nullable: bool,
    list: bool,
    rest: String,
}

fn parse_field_line(line: &str) -> Result<Option<ParsedField>> {
    let mut parts = line.split_whitespace();
    let Some(name) = parts.next() else {
        return Ok(None);
    };
    if name.starts_with('@') {
        return Ok(None);
    }
    let Some(raw_type) = parts.next() else {
        return Ok(None);
    };
    let rest = line
        .split_once(raw_type)
        .map(|(_, rest)| rest.trim().to_owned())
        .unwrap_or_default();
    let nullable = raw_type.ends_with('?');
    let raw_type = raw_type.trim_end_matches('?');
    let list = raw_type.ends_with("[]");
    let base_type = raw_type.trim_end_matches("[]").to_owned();

    Ok(Some(ParsedField {
        name: name.to_owned(),
        base_type,
        nullable,
        list,
        rest,
    }))
}

#[derive(Debug)]
struct PendingRelation {
    model: String,
    fields: Vec<String>,
    references: Vec<String>,
}

fn parse_relation_field(field: &ParsedField) -> Result<Option<PendingRelation>> {
    if !field.rest.contains("@relation") {
        return Ok(None);
    }
    let Some(args) = attr_args(&field.rest, "@relation")? else {
        return Ok(None);
    };
    let Some(fields) = named_arg(&args, "fields") else {
        return Ok(None);
    };
    let Some(references) = named_arg(&args, "references") else {
        return Ok(None);
    };
    let fields = parse_field_list(fields)?;
    let references = parse_field_list(references)?;
    Ok(Some(PendingRelation {
        model: field.base_type.clone(),
        fields,
        references,
    }))
}

#[derive(Debug)]
struct MappedField {
    pg_type: String,
    pg_type_kind: String,
    rust_type: RustType,
}

fn map_prisma_field(
    field: &ParsedField,
    datasource_name: &str,
    enum_names: &BTreeSet<String>,
    enum_db_names: &BTreeMap<String, (String, String)>,
) -> Result<MappedField> {
    if enum_names.contains(&field.base_type) {
        if field.list {
            return Err(Error::codegen(format!(
                "Prisma enum list fields are not supported yet: {}",
                field.name
            )));
        }
        let (_, db_name) = enum_db_names
            .get(&field.base_type)
            .ok_or_else(|| Error::codegen(format!("unknown Prisma enum {}", field.base_type)))?;
        return Ok(MappedField {
            pg_type: db_name.clone(),
            pg_type_kind: "e".to_owned(),
            rust_type: RustType::enum_type(rust_type_ident(&field.base_type)),
        });
    }

    let native = native_db_type(&field.rest, datasource_name);
    let (pg_type, rust_path) = match field.base_type.as_str() {
        "String" => match native.as_deref() {
            Some("Uuid") => ("uuid", "fuwa::types::Uuid"),
            Some("Char") => ("bpchar", "String"),
            Some("VarChar") => ("varchar", "String"),
            Some("Text") | None => ("text", "String"),
            Some(other) => {
                return unsupported_prisma_type(&field.name, "String", datasource_name, other)
            }
        },
        "Uuid" => ("uuid", "fuwa::types::Uuid"),
        "Int" => match native.as_deref() {
            Some("SmallInt") => ("int2", "i16"),
            Some("Integer") | Some("Int") | None => ("int4", "i32"),
            Some(other) => {
                return unsupported_prisma_type(&field.name, "Int", datasource_name, other)
            }
        },
        "BigInt" => match native.as_deref() {
            Some("BigInt") | None => ("int8", "i64"),
            Some(other) => {
                return unsupported_prisma_type(&field.name, "BigInt", datasource_name, other)
            }
        },
        "Float" => match native.as_deref() {
            Some("Real") => ("float4", "f32"),
            Some("DoublePrecision") | Some("Double") | None => ("float8", "f64"),
            Some(other) => {
                return unsupported_prisma_type(&field.name, "Float", datasource_name, other)
            }
        },
        "Decimal" => match native.as_deref() {
            Some("Decimal") | None => ("numeric", "fuwa::types::Decimal"),
            Some(other) => {
                return unsupported_prisma_type(&field.name, "Decimal", datasource_name, other)
            }
        },
        "Boolean" | "Bool" => match native.as_deref() {
            Some("Boolean") | None => ("bool", "bool"),
            Some(other) => {
                return unsupported_prisma_type(&field.name, "Boolean", datasource_name, other)
            }
        },
        "DateTime" => match native.as_deref() {
            Some("Timestamptz") => ("timestamptz", "fuwa::types::DateTime<fuwa::types::Utc>"),
            Some("Timestamp") | None => ("timestamp", "fuwa::types::NaiveDateTime"),
            Some("Date") => ("date", "fuwa::types::NaiveDate"),
            Some(other) => {
                return unsupported_prisma_type(&field.name, "DateTime", datasource_name, other)
            }
        },
        "Bytes" => match native.as_deref() {
            Some("ByteA") | None => ("bytea", "Vec<u8>"),
            Some(other) => {
                return unsupported_prisma_type(&field.name, "Bytes", datasource_name, other)
            }
        },
        "Json" => match native.as_deref() {
            Some("Json") => ("json", "fuwa::types::Value"),
            Some("JsonB") | None => ("jsonb", "fuwa::types::Value"),
            Some(other) => {
                return unsupported_prisma_type(&field.name, "Json", datasource_name, other)
            }
        },
        other => {
            return Err(Error::codegen(format!(
                "unsupported Prisma field type {other:?} on {}",
                field.name
            )));
        }
    };

    let (pg_type, rust_path) = if field.list {
        (array_pg_type(pg_type)?, format!("Vec<{rust_path}>"))
    } else {
        (pg_type.to_owned(), rust_path.to_owned())
    };

    Ok(MappedField {
        pg_type,
        pg_type_kind: "b".to_owned(),
        rust_type: RustType::new(rust_path),
    })
}

fn unsupported_prisma_type<T>(
    field: &str,
    scalar: &str,
    datasource_name: &str,
    native: &str,
) -> Result<T> {
    Err(Error::codegen(format!(
        "unsupported Prisma native type @{datasource_name}.{native} for {scalar} field {field}"
    )))
}

fn array_pg_type(pg_type: &str) -> Result<String> {
    match pg_type {
        "int2" => Ok("_int2"),
        "int4" => Ok("_int4"),
        "int8" => Ok("_int8"),
        "float4" => Ok("_float4"),
        "float8" => Ok("_float8"),
        "numeric" => Ok("_numeric"),
        "bool" => Ok("_bool"),
        "text" => Ok("_text"),
        "varchar" => Ok("_varchar"),
        "bpchar" => Ok("_bpchar"),
        "uuid" => Ok("_uuid"),
        "timestamp" => Ok("_timestamp"),
        "timestamptz" => Ok("_timestamptz"),
        "date" => Ok("_date"),
        "json" => Ok("_json"),
        "jsonb" => Ok("_jsonb"),
        other => {
            return Err(Error::codegen(format!(
                "unsupported Prisma list mapping for PostgreSQL type {other}"
            )))
        }
    }
    .map(str::to_owned)
}

fn native_db_type(rest: &str, datasource_name: &str) -> Option<String> {
    let needle = format!("@{datasource_name}.");
    let index = find_top_level_at(rest, |index| {
        rest[index..].starts_with(&needle) && attr_start_boundary(rest, index)
    })? + needle.len();
    let tail = &rest[index..];
    let ident: String = tail
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect();
    if ident.is_empty() {
        None
    } else {
        Some(ident)
    }
}

fn has_attr(line: &str, name: &str) -> bool {
    find_top_level_attr(line, name).is_some()
}

fn find_top_level_attr(line: &str, name: &str) -> Option<usize> {
    find_top_level_at(line, |index| {
        if !line[index..].starts_with(name) || !attr_start_boundary(line, index) {
            return false;
        }

        let after = line[index + name.len()..].chars().next();
        !after.is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    })
}

fn find_top_level_at(line: &str, mut is_match: impl FnMut(usize) -> bool) -> Option<usize> {
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut in_string = false;
    let mut escaped = false;

    for (index, ch) in line.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }

        if in_string {
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '(' => parens += 1,
            ')' => parens -= 1,
            '[' => brackets += 1,
            ']' => brackets -= 1,
            '@' if parens == 0 && brackets == 0 && is_match(index) => return Some(index),
            _ => {}
        }
    }

    None
}

fn attr_start_boundary(line: &str, index: usize) -> bool {
    index == 0
        || line[..index]
            .chars()
            .last()
            .is_some_and(char::is_whitespace)
}

fn assignment_string(line: &str, name: &str) -> Option<String> {
    parse_string_literal(assignment_value(line, name)?).ok()
}

fn assignment_value<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    let trimmed = line.trim();
    let (left, right) = trimmed.split_once('=')?;
    if left.trim() != name {
        return None;
    }
    Some(right.trim())
}

fn function_args(value: &str, name: &str) -> Option<String> {
    let value = value.trim();
    let after_name = value.strip_prefix(name)?;
    let after_name = after_name.trim_start();
    if !after_name.starts_with('(') {
        return None;
    }
    let open = value.find('(')?;
    let close = find_matching_paren(value, open)?;
    Some(value[open + 1..close].to_owned())
}

fn attr_string_arg(line: &str, attr: &str) -> Result<String> {
    attr_string_arg_optional(line, attr)?.ok_or_else(|| {
        Error::codegen(format!(
            "missing string argument for Prisma attribute {attr} in {line:?}"
        ))
    })
}

fn attr_string_arg_optional(line: &str, attr: &str) -> Result<Option<String>> {
    let Some(args) = attr_args(line, attr)? else {
        return Ok(None);
    };
    if let Some(value) = named_arg(&args, "name") {
        return parse_string_literal(value).map(Some);
    }
    let first = args
        .split(',')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match first {
        Some(value) if value.starts_with('"') => parse_string_literal(value).map(Some),
        _ => Ok(None),
    }
}

fn attr_raw_arg_optional(line: &str, attr: &str) -> Result<Option<String>> {
    Ok(attr_args(line, attr)?.map(|args| args.trim().to_owned()))
}

fn attr_field_list_arg(line: &str, attr: &str, name: Option<&str>) -> Result<Vec<String>> {
    let args = attr_args(line, attr)?.ok_or_else(|| {
        Error::codegen(format!(
            "missing argument list for Prisma attribute {attr} in {line:?}"
        ))
    })?;
    let value = match name {
        Some(name) => named_arg(&args, name).ok_or_else(|| {
            Error::codegen(format!(
                "missing {name} argument for Prisma attribute {attr} in {line:?}"
            ))
        })?,
        None => split_top_level(&args, ',')
            .into_iter()
            .find(|arg| arg.trim().starts_with('['))
            .or_else(|| named_arg(&args, "fields"))
            .ok_or_else(|| Error::codegen(format!("missing field list in {line:?}")))?,
    };
    parse_field_list(value)
}

fn attr_args(line: &str, attr: &str) -> Result<Option<String>> {
    let Some(start) = find_top_level_attr(line, attr) else {
        return Ok(None);
    };
    let after_attr = start + attr.len();
    let after = line[after_attr..].trim_start();
    if !after.starts_with('(') {
        return Ok(None);
    }
    let open = line.len() - after.len();
    let close = find_matching_paren(line, open).ok_or_else(|| {
        Error::codegen(format!(
            "unterminated argument list for Prisma attribute {attr} in {line:?}"
        ))
    })?;
    Ok(Some(line[open + 1..close].to_owned()))
}

fn find_matching_paren(value: &str, open: usize) -> Option<usize> {
    let mut depth = 0_i32;
    let mut in_string = false;
    let mut escaped = false;
    for (index, ch) in value.char_indices().skip_while(|(index, _)| *index < open) {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_string => escaped = true,
            '"' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn named_arg<'a>(args: &'a str, name: &str) -> Option<&'a str> {
    split_top_level(args, ',').into_iter().find_map(|arg| {
        let (left, right) = arg.split_once(':')?;
        if left.trim() == name {
            Some(right.trim())
        } else {
            None
        }
    })
}

fn parse_field_list(value: &str) -> Result<Vec<String>> {
    let trimmed = value.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Err(Error::codegen(format!(
            "expected Prisma field list, got {value:?}"
        )));
    }
    Ok(trimmed[1..trimmed.len() - 1]
        .split(',')
        .map(str::trim)
        .filter(|field| !field.is_empty())
        .map(str::to_owned)
        .collect())
}

fn parse_string_literal(value: &str) -> Result<String> {
    let value = value.trim();
    if !value.starts_with('"') {
        return Err(Error::codegen(format!(
            "expected string literal, got {value:?}"
        )));
    }
    let mut out = String::new();
    let mut chars = value[1..].chars();
    let mut escaped = false;
    while let Some(ch) = chars.next() {
        if escaped {
            out.push(match ch {
                '"' => '"',
                '\\' => '\\',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                other => other,
            });
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return Ok(out);
        } else {
            out.push(ch);
        }
    }
    Err(Error::codegen(format!(
        "unterminated string literal {value:?}"
    )))
}

fn percent_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
            {
                decoded.push(high << 4 | low);
                index += 3;
                continue;
            }
        }

        decoded.push(bytes[index]);
        index += 1;
    }

    String::from_utf8(decoded).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn split_top_level(value: &str, delimiter: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut in_string = false;
    let mut escaped = false;

    for (index, ch) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_string => escaped = true,
            '"' => in_string = !in_string,
            '(' if !in_string => parens += 1,
            ')' if !in_string => parens -= 1,
            '[' if !in_string => brackets += 1,
            ']' if !in_string => brackets -= 1,
            ch if ch == delimiter && !in_string && parens == 0 && brackets == 0 => {
                parts.push(value[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(value[start..].trim());
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_datasource_url_schema_as_default_schema() {
        let schema = parse_prisma_schema(
            r#"
            datasource db {
              provider = "postgresql"
              url      = "postgresql://example.test/app?sslmode=require&schema=app"
            }

            enum Role {
              USER
              ADMIN
            }

            model User {
              id   String @id
              role Role
            }
            "#,
        )
        .unwrap();

        assert_eq!(schema.enums[0].schema, "app");
        assert_eq!(schema.tables[0].schema, "app");
    }

    #[test]
    fn uses_cli_schema_as_default_when_datasource_url_has_no_schema() {
        let schema = parse_prisma_schema_with_default_schema(
            r#"
            datasource db {
              provider = "postgresql"
            }

            model User {
              id String @id
            }
            "#,
            Some("app"),
        )
        .unwrap();

        assert_eq!(schema.tables[0].schema, "app");
    }

    #[test]
    fn parses_inline_prisma_blocks() {
        let schema = parse_prisma_schema(
            r#"
            datasource db { provider = "postgresql" } enum Role { USER } model User { id String @id @db.Uuid }
            "#,
        )
        .unwrap();

        assert_eq!(schema.enums[0].name, "Role");
        assert_eq!(schema.enums[0].variants[0].db_name, "USER");

        let users = schema
            .tables
            .iter()
            .find(|table| table.name == "User")
            .unwrap();
        assert_eq!(users.primary_key, vec!["id"]);
        let id = users
            .columns
            .iter()
            .find(|column| column.name == "id")
            .unwrap();
        assert_eq!(id.pg_type, "uuid");
        assert!(id.primary_key);
    }

    #[test]
    fn parses_prisma_models_enums_maps_and_relations() {
        let schema = parse_prisma_schema(
            r#"
            datasource db {
              provider = "postgresql"
              schemas = ["public", "auth"]
            }

            enum Role {
              USER
              ADMIN @map("admin")
              @@map("user_role")
              @@schema("auth")
            }

            model User {
              id        String   @id @default(uuid()) @db.Uuid
              email     String   @unique
              firstName String   @map("first_name")
              role      Role     @default(USER)
              posts     Post[]

              @@map("users")
              @@schema("auth")
            }

            model Post {
              id       BigInt @id @default(autoincrement())
              author   User   @relation(fields: [authorId], references: [id])
              authorId String @map("author_id") @db.Uuid
              title    String

              @@map("posts")
            }
            "#,
        )
        .unwrap();

        assert_eq!(schema.enums.len(), 1);
        assert_eq!(schema.enums[0].schema, "auth");
        assert_eq!(schema.enums[0].name, "user_role");
        assert_eq!(schema.enums[0].variants[1].db_name, "admin");

        let users = schema
            .tables
            .iter()
            .find(|table| table.name == "users")
            .unwrap();
        assert_eq!(users.schema, "auth");
        assert_eq!(users.primary_key, vec!["id"]);
        assert_eq!(users.uniques, vec![vec!["email"]]);
        assert!(users
            .columns
            .iter()
            .any(|column| column.name == "first_name"));
        let role = users
            .columns
            .iter()
            .find(|column| column.name == "role")
            .unwrap();
        assert_eq!(role.pg_type_kind, "e");
        assert_eq!(role.rust_type.path(), "Role");

        let posts = schema
            .tables
            .iter()
            .find(|table| table.name == "posts")
            .unwrap();
        let author_id = posts
            .columns
            .iter()
            .find(|column| column.name == "author_id")
            .unwrap();
        assert_eq!(author_id.rust_type.path(), "fuwa::types::Uuid");
        assert_eq!(
            author_id.relation.as_ref().unwrap().fields,
            vec!["author_id"]
        );
    }

    #[test]
    fn parses_compound_uniques_and_relation_backrefs() {
        let schema = parse_prisma_schema(
            r#"
            datasource db {
              provider = "postgresql"
            }

            model User {
              id    String @id
              posts Post[] @relation("AuthorPosts")
            }

            model Post {
              id       String @id
              authorId String
              title    String
              author   User   @relation("AuthorPosts", fields: [authorId], references: [id])

              @@unique([authorId, title], map: "post_author_title_key")
            }
            "#,
        )
        .unwrap();

        let posts = schema
            .tables
            .iter()
            .find(|table| table.name == "Post")
            .unwrap();
        assert_eq!(posts.uniques, vec![vec!["authorId", "title"]]);
        let author_id = posts
            .columns
            .iter()
            .find(|column| column.name == "authorId")
            .unwrap();
        assert_eq!(author_id.relation.as_ref().unwrap().model, "User");
    }

    #[test]
    fn parses_mapped_compound_constraints_as_column_names() {
        let schema = parse_prisma_schema(
            r#"
            datasource db {
              provider = "postgresql"
            }

            model Person {
              firstName String @map("first_name")
              lastName  String @map("last_name")
              email     String @map("email_address")

              @@id([firstName, lastName])
              @@unique([email, lastName])
            }
            "#,
        )
        .unwrap();

        let people = schema
            .tables
            .iter()
            .find(|table| table.name == "Person")
            .unwrap();
        assert_eq!(people.primary_key, vec!["first_name", "last_name"]);
        assert_eq!(people.uniques, vec![vec!["email_address", "last_name"]]);
        assert!(
            people
                .columns
                .iter()
                .find(|column| column.name == "first_name")
                .unwrap()
                .primary_key
        );
        assert!(
            people
                .columns
                .iter()
                .find(|column| column.name == "last_name")
                .unwrap()
                .primary_key
        );
        assert!(
            !people
                .columns
                .iter()
                .find(|column| column.name == "email_address")
                .unwrap()
                .primary_key
        );
    }

    #[test]
    fn maps_native_types_from_named_datasource_namespace() {
        let schema = parse_prisma_schema(
            r#"
            datasource pg {
              provider = "postgresql"
            }

            model Event {
              id        String   @id @pg.Uuid
              createdAt DateTime @pg.Timestamptz
            }
            "#,
        )
        .unwrap();

        let events = schema
            .tables
            .iter()
            .find(|table| table.name == "Event")
            .unwrap();
        let id = events
            .columns
            .iter()
            .find(|column| column.name == "id")
            .unwrap();
        let created_at = events
            .columns
            .iter()
            .find(|column| column.name == "createdAt")
            .unwrap();
        assert_eq!(id.pg_type, "uuid");
        assert_eq!(id.rust_type.path(), "fuwa::types::Uuid");
        assert_eq!(created_at.pg_type, "timestamptz");
        assert_eq!(
            created_at.rust_type.path(),
            "fuwa::types::DateTime<fuwa::types::Utc>"
        );
    }

    #[test]
    fn ignores_native_type_namespace_inside_string_defaults() {
        let schema = parse_prisma_schema(
            r#"
            datasource db {
              provider = "postgresql"
            }

            model Document {
              id     String @id
              marker String @default("@db.Uuid")
              actual String @db.Uuid
            }
            "#,
        )
        .unwrap();

        let documents = schema
            .tables
            .iter()
            .find(|table| table.name == "Document")
            .unwrap();
        let marker = documents
            .columns
            .iter()
            .find(|column| column.name == "marker")
            .unwrap();
        let actual = documents
            .columns
            .iter()
            .find(|column| column.name == "actual")
            .unwrap();

        assert_eq!(marker.pg_type, "text");
        assert_eq!(marker.rust_type.path(), "String");
        assert_eq!(marker.default_expression.as_deref(), Some("\"@db.Uuid\""));
        assert_eq!(actual.pg_type, "uuid");
        assert_eq!(actual.rust_type.path(), "fuwa::types::Uuid");
    }

    #[test]
    fn skips_ignored_models() {
        let schema = parse_prisma_schema(
            r#"
            datasource db {
              provider = "postgresql"
            }

            model User {
              id String @id
            }

            model LegacySearch {
              id     String @id
              search Unsupported("tsvector")

              @@ignore
            }
            "#,
        )
        .unwrap();

        let tables: Vec<_> = schema
            .tables
            .iter()
            .map(|table| table.name.as_str())
            .collect();
        assert_eq!(tables, vec!["User"]);
    }

    #[test]
    fn strips_block_comments_before_parsing() {
        let schema = parse_prisma_schema(
            r#"
            datasource db {
              provider = "postgresql"
            }

            /*
            model LegacySearch {
              id     String @id
              search Unsupported("tsvector")
            }
            */

            model Document {
              id    String @id
              title String
              path  String @default("http://example.test/*not-comment*/")

              /* search Unsupported("tsvector") */
            }
            "#,
        )
        .unwrap();

        let documents = schema
            .tables
            .iter()
            .find(|table| table.name == "Document")
            .unwrap();
        let columns: Vec<_> = documents
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(columns, vec!["id", "title", "path"]);
        assert_eq!(
            documents
                .columns
                .iter()
                .find(|column| column.name == "path")
                .unwrap()
                .default_expression
                .as_deref(),
            Some("\"http://example.test/*not-comment*/\"")
        );
    }

    #[test]
    fn parses_named_compound_constraints() {
        let schema = parse_prisma_schema(
            r#"
            datasource db {
              provider = "postgresql"
            }

            model Like {
              postId String @map("post_id")
              userId String @map("user_id")
              a      String @map("a_col")
              b      String @map("b_col")

              @@id(name: "likeId", [postId, userId])
              @@unique(name: "u", [a, b])
            }
            "#,
        )
        .unwrap();

        let likes = schema
            .tables
            .iter()
            .find(|table| table.name == "Like")
            .unwrap();
        assert_eq!(likes.primary_key, vec!["post_id", "user_id"]);
        assert_eq!(likes.uniques, vec![vec!["a_col", "b_col"]]);
        assert!(
            likes
                .columns
                .iter()
                .find(|column| column.name == "post_id")
                .unwrap()
                .primary_key
        );
        assert!(
            likes
                .columns
                .iter()
                .find(|column| column.name == "user_id")
                .unwrap()
                .primary_key
        );
    }

    #[test]
    fn skips_ignored_fields_before_mapping() {
        let schema = parse_prisma_schema(
            r#"
            datasource db {
              provider = "postgresql"
            }

            model Document {
              id     String @id
              title  String
              search Unsupported("tsvector") @ignore
              legacy String @map("legacy_value") @ignore
            }
            "#,
        )
        .unwrap();

        let documents = schema
            .tables
            .iter()
            .find(|table| table.name == "Document")
            .unwrap();
        let columns: Vec<_> = documents
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(columns, vec!["id", "title"]);
    }

    #[test]
    fn preserves_fields_with_attribute_names_inside_defaults() {
        let schema = parse_prisma_schema(
            r#"
            datasource db {
              provider = "postgresql"
            }

            model Document {
              id           String @id
              marker       String @default("@ignore")
              uniqueMarker String @default("@unique")
              mappedMarker String @default("@map(\"ignored\")")
              search       String @ignore
            }
            "#,
        )
        .unwrap();

        let documents = schema
            .tables
            .iter()
            .find(|table| table.name == "Document")
            .unwrap();
        let columns: Vec<_> = documents
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            columns,
            vec!["id", "marker", "uniqueMarker", "mappedMarker"]
        );

        let marker = documents
            .columns
            .iter()
            .find(|column| column.name == "marker")
            .unwrap();
        assert_eq!(marker.default_expression.as_deref(), Some("\"@ignore\""));

        let unique_marker = documents
            .columns
            .iter()
            .find(|column| column.name == "uniqueMarker")
            .unwrap();
        assert!(!unique_marker.unique);
        assert!(documents.uniques.is_empty());
    }

    #[test]
    fn maps_relation_references_to_target_column_names() {
        let schema = parse_prisma_schema(
            r#"
            datasource db {
              provider = "postgresql"
            }

            model User {
              id    String @id @map("user_id")
              posts Post[]
            }

            model Post {
              id       String @id
              authorId String @map("author_id")
              author   User   @relation(fields: [authorId], references: [id])
            }
            "#,
        )
        .unwrap();

        let posts = schema
            .tables
            .iter()
            .find(|table| table.name == "Post")
            .unwrap();
        let author_id = posts
            .columns
            .iter()
            .find(|column| column.name == "author_id")
            .unwrap();
        let relation = author_id.relation.as_ref().unwrap();
        assert_eq!(relation.fields, vec!["author_id"]);
        assert_eq!(relation.references, vec!["user_id"]);
    }

    #[test]
    fn parses_multiline_attribute_arguments() {
        let schema = parse_prisma_schema(
            r#"
            datasource db {
              provider = "postgresql"
            }

            model User {
              id    String @id
              posts Post[]
            }

            model Post {
              id       String @id
              authorId String
              title    String
              author   User   @relation(
                fields: [authorId],
                references: [id],
              )

              @@unique(
                [authorId, title],
                map: "post_author_title_key",
              )
            }
            "#,
        )
        .unwrap();

        let posts = schema
            .tables
            .iter()
            .find(|table| table.name == "Post")
            .unwrap();
        assert_eq!(posts.uniques, vec![vec!["authorId", "title"]]);
        let author_id = posts
            .columns
            .iter()
            .find(|column| column.name == "authorId")
            .unwrap();
        let relation = author_id.relation.as_ref().unwrap();
        assert_eq!(relation.model, "User");
        assert_eq!(relation.fields, vec!["authorId"]);
        assert_eq!(relation.references, vec!["id"]);
    }
}
