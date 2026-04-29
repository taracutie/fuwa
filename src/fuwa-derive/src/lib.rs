//! Derive macros for `fuwa`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

#[proc_macro_derive(FromRow)]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    if !input.generics.params.is_empty() {
        return quote! {
            compile_error!("fuwa FromRow derive does not support generic structs");
        }
        .into();
    }

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return quote! {
                    compile_error!("fuwa FromRow derive only supports structs with named fields");
                }
                .into();
            }
        },
        _ => {
            return quote! {
                compile_error!("fuwa FromRow derive only supports structs");
            }
            .into();
        }
    };

    let decoders = fields.into_iter().map(|field| {
        let ident = field.ident.expect("named field should have an ident");
        let ty = field.ty;
        let column_name = ident.to_string().trim_start_matches("r#").to_owned();
        quote! {
            #ident: row.try_get::<_, #ty>(#column_name).map_err(|err| {
                ::fuwa::Error::row_decode(format!(
                    "failed to decode column {}: {}",
                    #column_name,
                    err
                ))
            })?
        }
    });

    quote! {
        impl ::fuwa::FromRow for #name {
            fn from_row(row: &::fuwa::Row) -> ::fuwa::Result<Self> {
                Ok(Self {
                    #(#decoders,)*
                })
            }
        }
    }
    .into()
}
