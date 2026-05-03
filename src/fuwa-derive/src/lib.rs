//! Derive macros for `fuwa`.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Field, Fields, LitStr, Path};

#[proc_macro_derive(FromRow, attributes(fuwa))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro_derive(Insertable, attributes(fuwa))]
pub fn derive_insertable(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_insertable(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro_derive(Patch, attributes(fuwa))]
pub fn derive_patch(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_patch(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand(input: DeriveInput) -> syn::Result<TokenStream2> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "fuwa FromRow derive does not support generic structs",
        ));
    }

    let name = input.ident;
    let container = ContainerAttrs::from_attrs(&input.attrs)?;

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &name,
                    "fuwa FromRow derive only supports structs with named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &name,
                "fuwa FromRow derive only supports structs",
            ))
        }
    };

    let decoders: Vec<TokenStream2> = fields
        .iter()
        .map(|field| field_decoder(field, &container))
        .collect::<syn::Result<_>>()?;

    Ok(quote! {
        impl ::fuwa::FromRow for #name {
            fn from_row(row: &::fuwa::Row) -> ::fuwa::Result<Self> {
                Ok(Self {
                    #(#decoders,)*
                })
            }
        }
    })
}

fn field_decoder(field: &Field, container: &ContainerAttrs) -> syn::Result<TokenStream2> {
    let ident = field
        .ident
        .as_ref()
        .expect("named field should have an ident");
    let ty = &field.ty;
    let attrs = FieldAttrs::from_attrs(&field.attrs)?;

    if attrs.skip {
        return Ok(quote! {
            #ident: ::core::default::Default::default()
        });
    }

    if attrs.flatten {
        return Ok(quote! {
            #ident: <#ty as ::fuwa::FromRow>::from_row(row)?
        });
    }

    let raw_name = ident.to_string().trim_start_matches("r#").to_owned();
    let column_name = match attrs.rename {
        Some(name) => name,
        None => container.apply_rename_all(&raw_name),
    };

    let decode = if let Some(decode_path) = attrs.decode_with.as_ref() {
        quote! {
            #decode_path(row, #column_name)?
        }
    } else {
        quote! {
            row.try_get::<_, #ty>(#column_name).map_err(|err| {
                ::fuwa::Error::row_decode(format!(
                    "failed to decode column {}: {}",
                    #column_name,
                    err
                ))
            })?
        }
    };

    if attrs.default {
        Ok(quote! {
            #ident: if row.columns().iter().any(|column| column.name() == #column_name) {
                #decode
            } else {
                ::core::default::Default::default()
            }
        })
    } else {
        Ok(quote! {
            #ident: #decode
        })
    }
}

#[derive(Default)]
struct ContainerAttrs {
    rename_all: Option<RenameAll>,
    table: Option<Path>,
}

impl ContainerAttrs {
    fn from_attrs(attrs: &[Attribute]) -> syn::Result<Self> {
        let mut out = Self::default();
        for attr in attrs.iter().filter(|a| a.path().is_ident("fuwa")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename_all") {
                    let lit: LitStr = meta.value()?.parse()?;
                    out.rename_all = Some(RenameAll::parse(&lit)?);
                    Ok(())
                } else if meta.path.is_ident("table") {
                    let path: Path = meta.value()?.parse()?;
                    out.table = Some(path);
                    Ok(())
                } else {
                    Err(meta
                        .error("unknown fuwa container attribute; supported: rename_all, table"))
                }
            })?;
        }
        Ok(out)
    }

    fn apply_rename_all(&self, raw: &str) -> String {
        match &self.rename_all {
            Some(rule) => rule.apply(raw),
            None => raw.to_owned(),
        }
    }
}

fn expand_insertable(input: DeriveInput) -> syn::Result<TokenStream2> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "fuwa Insertable derive does not support generic structs",
        ));
    }

    let name = input.ident;
    let container = ContainerAttrs::from_attrs(&input.attrs)?;
    let table_path = container.table.as_ref().ok_or_else(|| {
        syn::Error::new_spanned(
            &name,
            "fuwa Insertable requires a `#[fuwa(table = <module_path>)]` attribute pointing at the generated table module",
        )
    })?;

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &name,
                    "fuwa Insertable derive only supports structs with named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &name,
                "fuwa Insertable derive only supports structs",
            ))
        }
    };

    let assignments: Vec<TokenStream2> = fields
        .iter()
        .map(|field| {
            let attrs = FieldAttrs::from_attrs(&field.attrs)?;
            if attrs.skip {
                return Ok(None);
            }
            let ident = field
                .ident
                .as_ref()
                .expect("named field should have an ident");
            let raw_name = ident.to_string().trim_start_matches("r#").to_owned();
            let column_name = match attrs.rename {
                Some(name) => name,
                None => container.apply_rename_all(&raw_name),
            };
            let column_ident = rust_column_ident(&column_name, ident.span());
            Ok(Some(quote! {
                #table_path::#column_ident.set(self.#ident)
            }))
        })
        .collect::<syn::Result<Vec<Option<TokenStream2>>>>()?
        .into_iter()
        .flatten()
        .collect();

    Ok(quote! {
        impl ::fuwa::core::Assignments for #name {
            fn into_assignments(self) -> ::std::vec::Vec<::fuwa::core::Assignment> {
                ::std::vec![#(#assignments,)*]
            }
        }
    })
}

fn expand_patch(input: DeriveInput) -> syn::Result<TokenStream2> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "fuwa Patch derive does not support generic structs",
        ));
    }

    let name = input.ident;
    let container = ContainerAttrs::from_attrs(&input.attrs)?;
    let table_path = container.table.as_ref().ok_or_else(|| {
        syn::Error::new_spanned(
            &name,
            "fuwa Patch requires a `#[fuwa(table = <module_path>)]` attribute pointing at the generated table module",
        )
    })?;

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &name,
                    "fuwa Patch derive only supports structs with named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &name,
                "fuwa Patch derive only supports structs",
            ))
        }
    };

    let assignments: Vec<TokenStream2> = fields
        .iter()
        .map(|field| {
            let attrs = FieldAttrs::from_attrs(&field.attrs)?;
            if attrs.skip {
                return Ok(None);
            }
            let ident = field
                .ident
                .as_ref()
                .expect("named field should have an ident");
            let raw_name = ident.to_string().trim_start_matches("r#").to_owned();
            let column_name = match attrs.rename {
                Some(name) => name,
                None => container.apply_rename_all(&raw_name),
            };
            let column_ident = rust_column_ident(&column_name, ident.span());
            Ok(Some(quote! {
                if let ::core::option::Option::Some(__value) = self.#ident {
                    __assignments.push(#table_path::#column_ident.set(__value));
                }
            }))
        })
        .collect::<syn::Result<Vec<Option<TokenStream2>>>>()?
        .into_iter()
        .flatten()
        .collect();

    Ok(quote! {
        impl ::fuwa::core::Assignments for #name {
            fn into_assignments(self) -> ::std::vec::Vec<::fuwa::core::Assignment> {
                let mut __assignments: ::std::vec::Vec<::fuwa::core::Assignment> = ::std::vec::Vec::new();
                #(#assignments)*
                __assignments
            }
        }
    })
}

#[derive(Default)]
struct FieldAttrs {
    rename: Option<String>,
    skip: bool,
    default: bool,
    flatten: bool,
    decode_with: Option<Path>,
}

impl FieldAttrs {
    fn from_attrs(attrs: &[Attribute]) -> syn::Result<Self> {
        let mut out = Self::default();
        let mut first_fuwa: Option<&Attribute> = None;
        for attr in attrs.iter().filter(|a| a.path().is_ident("fuwa")) {
            if first_fuwa.is_none() {
                first_fuwa = Some(attr);
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    let lit: LitStr = meta.value()?.parse()?;
                    out.rename = Some(lit.value());
                } else if meta.path.is_ident("skip") {
                    out.skip = true;
                } else if meta.path.is_ident("default") {
                    out.default = true;
                } else if meta.path.is_ident("flatten") {
                    out.flatten = true;
                } else if meta.path.is_ident("decode_with") {
                    let lit: LitStr = meta.value()?.parse()?;
                    let path: Path = lit.parse()?;
                    out.decode_with = Some(path);
                } else {
                    return Err(meta.error(
                        "unknown fuwa field attribute; supported: rename, skip, default, flatten, decode_with",
                    ));
                }
                Ok(())
            })?;
        }
        if let Some(span_attr) = first_fuwa {
            if out.flatten && (out.rename.is_some() || out.default || out.decode_with.is_some()) {
                return Err(syn::Error::new_spanned(
                    span_attr,
                    "fuwa(flatten) cannot combine with rename, default, or decode_with",
                ));
            }
            if out.skip
                && (out.rename.is_some() || out.default || out.flatten || out.decode_with.is_some())
            {
                return Err(syn::Error::new_spanned(
                    span_attr,
                    "fuwa(skip) cannot combine with other fuwa attrs",
                ));
            }
        }
        Ok(out)
    }
}

enum RenameAll {
    Lower,
    Upper,
    SnakeCase,
    ScreamingSnake,
    Camel,
    Pascal,
    Kebab,
}

impl RenameAll {
    fn parse(lit: &LitStr) -> syn::Result<Self> {
        match lit.value().as_str() {
            "lowercase" => Ok(Self::Lower),
            "UPPERCASE" => Ok(Self::Upper),
            "snake_case" => Ok(Self::SnakeCase),
            "SCREAMING_SNAKE_CASE" => Ok(Self::ScreamingSnake),
            "camelCase" => Ok(Self::Camel),
            "PascalCase" => Ok(Self::Pascal),
            "kebab-case" => Ok(Self::Kebab),
            other => Err(syn::Error::new_spanned(
                lit,
                format!(
                    "unknown rename_all value `{other}`; supported: \
                     lowercase, UPPERCASE, snake_case, SCREAMING_SNAKE_CASE, \
                     camelCase, PascalCase, kebab-case"
                ),
            )),
        }
    }

    fn apply(&self, input: &str) -> String {
        match self {
            Self::Lower => input.to_lowercase(),
            Self::Upper => input.to_uppercase(),
            Self::SnakeCase => input.to_owned(),
            Self::ScreamingSnake => input.to_uppercase(),
            Self::Camel => snake_to_camel(input, false),
            Self::Pascal => snake_to_camel(input, true),
            Self::Kebab => input.replace('_', "-"),
        }
    }
}

fn rust_column_ident(value: &str, span: proc_macro2::Span) -> syn::Ident {
    let ident = rust_ident(value);
    if let Some(raw) = ident.strip_prefix("r#") {
        syn::Ident::new_raw(raw, span)
    } else {
        syn::Ident::new(&ident, span)
    }
}

fn rust_ident(value: &str) -> String {
    let mut ident = String::new();
    let mut previous_was_underscore = false;
    let mut previous_was_lower_or_digit = false;
    let chars: Vec<char> = value.chars().collect();

    for (index, ch) in chars.iter().copied().enumerate() {
        if ch.is_ascii_alphanumeric() {
            let next_is_lower = chars
                .get(index + 1)
                .is_some_and(|next| next.is_ascii_lowercase());
            let should_split_upper = ch.is_ascii_uppercase()
                && !ident.is_empty()
                && !previous_was_underscore
                && (previous_was_lower_or_digit || next_is_lower);
            if should_split_upper {
                ident.push('_');
            }
            ident.push(ch.to_ascii_lowercase());
            previous_was_underscore = false;
            previous_was_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        } else if !previous_was_underscore {
            ident.push('_');
            previous_was_underscore = true;
            previous_was_lower_or_digit = false;
        }
    }

    let ident = ident.trim_matches('_');
    let mut ident = if ident.is_empty() {
        "unnamed".to_owned()
    } else {
        ident.to_owned()
    };

    if ident.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        ident.insert(0, '_');
    }

    if is_rust_keyword(&ident) {
        format!("r#{ident}")
    } else {
        ident
    }
}

fn is_rust_keyword(value: &str) -> bool {
    matches!(
        value,
        "as" | "break"
            | "const"
            | "continue"
            | "crate"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "fn"
            | "for"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "Self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "type"
            | "unsafe"
            | "use"
            | "where"
            | "while"
            | "async"
            | "await"
            | "dyn"
            | "abstract"
            | "become"
            | "box"
            | "do"
            | "final"
            | "macro"
            | "override"
            | "priv"
            | "typeof"
            | "unsized"
            | "virtual"
            | "yield"
            | "try"
    )
}

fn snake_to_camel(input: &str, capitalize_first: bool) -> String {
    let mut out = String::with_capacity(input.len());
    let mut next_upper = capitalize_first;
    for ch in input.chars() {
        if ch == '_' {
            next_upper = true;
        } else if next_upper {
            out.extend(ch.to_uppercase());
            next_upper = false;
        } else {
            out.push(ch);
        }
    }
    out
}
