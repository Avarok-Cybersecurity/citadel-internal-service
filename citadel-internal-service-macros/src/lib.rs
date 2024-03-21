use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DataEnum, DeriveInput, Ident};

#[proc_macro_derive(IsError)]
pub fn is_error_derive(input: TokenStream) -> TokenStream {
    generate_function(input, "Failure", "is_error")
}

#[proc_macro_derive(IsNotification)]
pub fn is_notification_derive(input: TokenStream) -> TokenStream {
    generate_function(input, "Notification", "is_notification")
}

fn generate_function(input: TokenStream, contains: &str, function_name: &str) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Extract the identifier and data from the input
    let name = &input.ident;
    let data = if let Data::Enum(data) = input.data {
        data
    } else {
        // This macro only supports enums
        panic!("{function_name} can only be derived for enums");
    };

    // Convert the function name to a tokenstream
    let function_name = Ident::new(function_name, name.span());

    // Generate match arms for each enum variant
    let match_arms = generate_match_arms(name, &data, contains);

    // Generate the implementation of the `$function_name` method
    let expanded = quote! {
        impl #name {
            pub fn #function_name(&self) -> bool {
                match self {
                    #(#match_arms)*
                }
            }
        }
    };

    // Convert into a TokenStream and return it
    TokenStream::from(expanded)
}

fn generate_match_arms(
    name: &Ident,
    data_enum: &DataEnum,
    contains: &str,
) -> Vec<proc_macro2::TokenStream> {
    data_enum
        .variants
        .iter()
        .map(|variant| {
            let variant_ident = &variant.ident;
            let variant_str = variant_ident.to_string();
            let is_error = variant_str.contains(contains);

            // Match against each variant, ignoring any inner data
            quote! {
                #name::#variant_ident(_) => #is_error,
            }
        })
        .collect()
}

#[proc_macro_derive(RequestId)]
pub fn request_ids(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Extract the identifier and data from the input
    let name = &input.ident;
    let data = if let Data::Enum(data) = input.data {
        data
    } else {
        // This macro only supports enums
        panic!("RequestId can only be derived for enums");
    };

    // Generate match arms for each enum variant
    let match_arms = generate_match_arms_uuid(name, &data);

    // Generate the implementation of the `$function_name` method
    let expanded = quote! {
        impl #name {
            pub fn request_id(&self) -> Option<&Uuid> {
                match self {
                    #(#match_arms)*
                }
            }
        }
    };

    // Convert into a TokenStream and return it
    TokenStream::from(expanded)
}

fn generate_match_arms_uuid(name: &Ident, data_enum: &DataEnum) -> Vec<proc_macro2::TokenStream> {
    data_enum
        .variants
        .iter()
        .map(|variant| {
            let variant_ident = &variant.ident;
            // Match against each variant, ignoring any inner data
            quote! {
                #name::#variant_ident(inner) => inner.request_id.as_ref(),
            }
        })
        .collect()
}
