use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef},
    compute::CastOptions,
    error::Result,
};
use arrow_schema::{ArrowError, Field};
use parquet_variant::path::VariantPath;

use crate::{VariantArray, VariantArrayBuilder};

/// Returns an array with the specified path extracted from the variant values.
///
/// The return array type depends on the `as_type` field of the options parameter
/// 1. `as_type: None`: a VariantArray is returned. The values in this new VariantArray will point
///    to the specified path.
/// 2. `as_type: Some(<specific field>)`: an array of the specified type is returned.
pub fn variant_get(input: &ArrayRef, options: GetOptions) -> Result<ArrayRef> {
    let variant_array: &VariantArray = input.as_any().downcast_ref().ok_or_else(|| {
        ArrowError::InvalidArgumentError(
            "expected a VariantArray as the input for variant_get".to_owned(),
        )
    })?;

    if let Some(as_type) = options.as_type {
        return Err(ArrowError::NotYetImplemented(format!(
            "getting a {} from a VariantArray is not implemented yet",
            as_type
        )));
    }

    let mut builder = VariantArrayBuilder::new(variant_array.len());
    for i in 0..variant_array.len() {
        let new_variant = variant_array.value(i);
        // TODO: perf?
        let new_variant = new_variant.get_path(&options.path);
        if let Some(new_variant) = new_variant {
            // TODO: we're decoding the value and doing a copy into a variant value again. This
            // copy can be much smarter.
            builder.append_variant(new_variant);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.build()))
}

/// Controls the action of the variant_get kernel.
#[derive(Debug, Clone)]
pub struct GetOptions<'a> {
    /// What path to extract
    pub path: VariantPath,
    /// if `as_type` is None, the returned array will itself be a VariantArray.
    ///
    /// if `as_type` is `Some(type)` the field is returned as the specified type if possible. To specify returning
    /// a Variant, pass a Field with variant type in the metadata.
    pub as_type: Option<Field>,
    /// Controls the casting behavior (e.g. error vs substituting null on cast error).
    pub cast_options: CastOptions<'a>,
}

impl<'a> GetOptions<'a> {
    /// Construct options to get the specified path as a variant.
    pub fn new_with_path(path: VariantPath) -> Self {
        Self {
            path,
            as_type: None,
            cast_options: Default::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef};
    use parquet_variant::{path::VariantPathElement, VariantBuilder};

    use crate::{VariantArray, VariantArrayBuilder};

    use super::{variant_get, GetOptions};

    #[test]
    fn get_primitive_variant() {
        let mut builder = VariantBuilder::new();
        builder.add_field_name("some_field");
        let mut object = builder.new_object();
        object.insert("some_field", 1234i64);
        object.finish().unwrap();
        let (metadata, value) = builder.finish();

        let mut builder = VariantArrayBuilder::new(1);
        builder.append_variant_buffers(&metadata, &value);

        let variant_array = builder.build();

        let input = Arc::new(variant_array) as ArrayRef;

        let result = variant_get(
            &input,
            GetOptions::new_with_path(
                vec![VariantPathElement::field("some_field".to_owned())].into(),
            ),
        )
        .unwrap();

        let result: &VariantArray = result.as_any().downcast_ref().unwrap();
        assert!(result.nulls().is_none());
        let result = result.value(0);
        assert_eq!(result.as_int64().unwrap(), 1234);
    }

    #[test]
    #[should_panic(
        expected = "Nested values are handled specially by ObjectBuilder and ListBuilder"
    )]
    fn get_complex_variant() {
        let mut builder = VariantBuilder::new();
        builder.add_field_name("top_level_field");
        builder.add_field_name("inner_field");

        let mut object = builder.new_object();
        let mut inner_object = object.new_object("top_level_field");
        inner_object.insert("inner_field", 1234i64);
        inner_object.finish().unwrap();
        object.finish().unwrap();
        let (metadata, value) = builder.finish();

        let mut builder = VariantArrayBuilder::new(1);
        builder.append_variant_buffers(&metadata, &value);

        let variant_array = builder.build();

        let input = Arc::new(variant_array) as ArrayRef;

        let result = variant_get(
            &input,
            GetOptions::new_with_path(
                vec![VariantPathElement::field("top_level_field".to_owned())].into(),
            ),
        )
        .unwrap();

        // uncomment once implemented
        todo!("{:?}", result);
        // let result: &VariantArray = result.as_any().downcast_ref().unwrap();
        // assert!(result.nulls().is_none());
        // let result = result.value(0);
        // let result = result.as_object().unwrap();
        // let binding = result.get("top_level_field").unwrap();
        // let result = binding.as_object().unwrap();
        // let result = result.get("inner_field").unwrap();
        // assert_eq!(result.as_int64().unwrap(), 1234);
    }
}
