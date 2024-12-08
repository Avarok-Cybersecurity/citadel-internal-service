use proc_macro::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{parse_macro_input, Data, DataEnum, DeriveInput, Ident};

#[proc_macro_derive(IsError)]
pub fn is_error_derive(input: TokenStream) -> TokenStream {
    generate_function(input, "Failure", "is_error")
}

#[proc_macro_derive(IsNotification)]
pub fn is_notification_derive(input: TokenStream) -> TokenStream {
    generate_function(input, "Notification", "is_notification")
}

// Create a proc macro that generates a function that goes through each enum variant, looks at the first and only item in the variant, and creates a function called request_id(&self) -> Option<&Uuid>, that looks at the field "request_id" in the variant and returns a reference to it if it exists.
#[proc_macro_derive(RequestId)]
pub fn request_id_derive(input: TokenStream) -> TokenStream {
    generate_field_function(input, "request_id", "request_id")
}

fn generate_field_function(
    input: TokenStream,
    field_name: &str,
    function_name: &str,
) -> TokenStream {
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
    let match_arms = generate_field_match_arms(name, &data, field_name);

    // Generate the implementation of the `is_error` method
    let expanded = quote! {
        impl #name {
            pub fn #function_name(&self) -> Option<&Uuid> {
                match self {
                    #(#match_arms)*
                }
            }
        }
    };

    // Convert into a TokenStream and return it
    TokenStream::from(expanded)
}

fn generate_field_match_arms(
    name: &Ident,
    data_enum: &DataEnum,
    field_name: &str,
) -> Vec<proc_macro2::TokenStream> {
    data_enum
        .variants
        .iter()
        .map(|variant| {
            let variant_ident = &variant.ident;
            let field = variant.fields.iter().next().unwrap();
            let field_name = field_name.to_string();
            let field_name = Ident::new(&field_name, field.span());

            // Determine if the enum is of the form Enum::Variant(inner) or Enum::Variant { inner, .. }
            let is_tuple_variant = variant
                .fields
                .iter()
                .next()
                .map_or(false, |field| field.ident.is_none());
            if is_tuple_variant {
                if let syn::Type::Path(type_path) = &field.ty {
                    if type_path.path.segments.len() == 1 {
                        return quote! {
                            #name::#variant_ident(inner) => inner.#field_name.as_ref(),
                        };
                    }
                }

                // Match against each variant, ignoring any inner data
                quote! {
                    #name::#variant_ident(inner, ..) => inner.#field_name.as_ref(),
                }
            } else {
                // Match against each variant, ignoring any inner data
                quote! {
                    #name::#variant_ident { #field_name, .. } => Some(#field_name.as_ref()),
                }
            }
        })
        .collect()
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

    // Generate the implementation of the `is_error` method
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
