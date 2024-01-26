use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DataEnum, DeriveInput, Ident};

#[proc_macro_derive(IsError)]
pub fn is_error_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Extract the identifier and data from the input
    let name = &input.ident;
    let data = if let Data::Enum(data) = input.data {
        data
    } else {
        // This macro only supports enums
        panic!("IsError can only be derived for enums");
    };

    // Generate match arms for each enum variant
    let match_arms = generate_match_arms(name, &data);

    // Generate the implementation of the `is_error` method
    let expanded = quote! {
        impl #name {
            pub fn is_error(&self) -> bool {
                match self {
                    #(#match_arms)*
                }
            }
        }
    };

    // Convert into a TokenStream and return it
    TokenStream::from(expanded)
}

fn generate_match_arms(name: &Ident, data_enum: &DataEnum) -> Vec<proc_macro2::TokenStream> {
    data_enum
        .variants
        .iter()
        .map(|variant| {
            let variant_ident = &variant.ident;
            let variant_str = variant_ident.to_string();
            let is_error = variant_str.contains("Failure");

            // Match against each variant, ignoring any inner data
            quote! {
                #name::#variant_ident(_) => #is_error,
            }
        })
        .collect()
}
