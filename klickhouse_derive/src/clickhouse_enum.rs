use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Expr, Fields, Lit, Meta};

use crate::case::RenameRule;
use crate::symbol::{KLICKHOUSE, RENAME, RENAME_ALL};

/// Expand a `#[derive(ClickhouseEnum)]` on an enum.
pub fn expand(input: &DeriveInput) -> Result<TokenStream, Vec<syn::Error>> {
    let variants = match &input.data {
        Data::Enum(data) => &data.variants,
        _ => {
            return Err(vec![syn::Error::new_spanned(
                input,
                "ClickhouseEnum can only be derived for enums",
            )]);
        }
    };

    // Validate: only unit variants allowed
    for variant in variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(vec![syn::Error::new_spanned(
                variant,
                "ClickhouseEnum only supports unit variants (no fields)",
            )]);
        }
    }

    // Parse container-level rename_all
    let rename_rule = parse_rename_all(&input.attrs)?;

    // Build variant name mappings
    let enum_name = &input.ident;
    let (enum_impl_generics, enum_ty_generics, enum_where_clause) = input.generics.split_for_impl();

    let mut variant_idents = Vec::new();
    let mut clickhouse_names = Vec::new();

    for variant in variants {
        let ident = &variant.ident;

        // Check for per-variant #[klickhouse(rename = "...")]
        let per_variant_rename = parse_variant_rename(&variant.attrs)?;

        let ch_name = if let Some(renamed) = per_variant_rename {
            renamed
        } else {
            rename_rule.apply_to_variant(&ident.to_string())
        };

        variant_idents.push(ident.clone());
        clickhouse_names.push(ch_name);
    }

    // Generate FromSql impl
    let from_arms = variant_idents
        .iter()
        .zip(clickhouse_names.iter())
        .map(|(ident, name)| {
            quote! { #name => ::std::result::Result::Ok(#enum_name::#ident), }
        });

    // Generate ToSql impl
    let to_arms = variant_idents
        .iter()
        .zip(clickhouse_names.iter())
        .map(|(ident, name)| {
            quote! { #enum_name::#ident => #name, }
        });

    let expanded = quote! {
        impl #enum_impl_generics ::klickhouse::FromSql for #enum_name #enum_ty_generics #enum_where_clause {
            fn from_sql(
                type_: &::klickhouse::Type,
                value: ::klickhouse::Value,
            ) -> ::klickhouse::Result<Self> {
                let name = <::std::string::String as ::klickhouse::FromSql>::from_sql(type_, value)?;
                match name.as_str() {
                    #(#from_arms)*
                    other => ::std::result::Result::Err(
                        ::klickhouse::KlickhouseError::DeserializeError(
                            ::std::format!("unknown enum variant '{}' for {}", other, ::std::stringify!(#enum_name))
                        )
                    ),
                }
            }
        }

        impl #enum_impl_generics ::klickhouse::ToSql for #enum_name #enum_ty_generics #enum_where_clause {
            fn to_sql(
                self,
                type_hint: ::std::option::Option<&::klickhouse::Type>,
            ) -> ::klickhouse::Result<::klickhouse::Value> {
                let name: &str = match self {
                    #(#to_arms)*
                };
                <&str as ::klickhouse::ToSql>::to_sql(name, type_hint)
            }
        }
    };

    Ok(expanded)
}

/// Parse `#[klickhouse(rename_all = "...")]` from enum-level attributes.
fn parse_rename_all(attrs: &[syn::Attribute]) -> Result<RenameRule, Vec<syn::Error>> {
    for attr in attrs {
        if !attr.path().is_ident(&KLICKHOUSE.to_string()) {
            continue;
        }
        let nested = match attr
            .parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated)
        {
            Ok(n) => n,
            Err(e) => return Err(vec![e]),
        };
        for meta in &nested {
            if let Meta::NameValue(nv) = meta {
                if nv.path == RENAME_ALL {
                    if let Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Str(lit_str) = &expr_lit.lit {
                            return RenameRule::from_str(&lit_str.value()).map_err(|e| {
                                vec![syn::Error::new_spanned(lit_str, e.to_string())]
                            });
                        }
                    }
                    return Err(vec![syn::Error::new_spanned(
                        &nv.value,
                        "expected a string literal for rename_all",
                    )]);
                }
            }
        }
    }
    Ok(RenameRule::None)
}

/// Parse `#[klickhouse(rename = "...")]` from variant-level attributes.
fn parse_variant_rename(attrs: &[syn::Attribute]) -> Result<Option<String>, Vec<syn::Error>> {
    for attr in attrs {
        if !attr.path().is_ident(&KLICKHOUSE.to_string()) {
            continue;
        }
        let nested = match attr
            .parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated)
        {
            Ok(n) => n,
            Err(e) => return Err(vec![e]),
        };
        for meta in &nested {
            if let Meta::NameValue(nv) = meta {
                if nv.path == RENAME {
                    if let Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Str(lit_str) = &expr_lit.lit {
                            return Ok(Some(lit_str.value()));
                        }
                    }
                    return Err(vec![syn::Error::new_spanned(
                        &nv.value,
                        "expected a string literal for rename",
                    )]);
                }
            }
        }
    }
    Ok(None)
}
