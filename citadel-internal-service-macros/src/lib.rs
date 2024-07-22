use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DataEnum, DeriveInput, Fields, Ident};

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



#[proc_macro_derive(RequestId)]
pub fn derive_request_id(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let request_id_impl = if let Data::Enum(data_enum) = &input.data {
        let match_arms = data_enum.variants.iter().map(|variant| {
            let variant_name = &variant.ident;
            let request_id_field = if let Fields::Named(fields_named) = &variant.fields {
                fields_named.named.iter().find(|field| field.ident.as_ref().unwrap() == "request_id")
            } else {
                None
            };

            if let Some(_) = request_id_field {
                quote! {
                    #name::#variant_name { ref request_id, .. } => *request_id,
                }
            } else {
                quote! {}
            }
        });

        quote! {
            impl RequestId for #name {
                fn request_id(&self) -> u32 {
                    match self {
                        #(#match_arms)*
                        _ => panic!("Variant does not contain a request_id field"),
                    }
                }
            }
        }
    } else {
        quote! {}
    };

    TokenStream::from(request_id_impl)
}