use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray, PrimitiveBuilder},
    compute::CastOptions,
    datatypes::UInt64Type,
    error::Result,
};
use arrow_schema::{ArrowError, DataType, Field};
use parquet_variant::Variant;

use crate::VariantArray;

/// Returns an array with the specified path extracted from the variant values.
pub fn variant_get(input: &ArrayRef, options: GetOptions) -> Result<ArrayRef> {
    let variant_array: &VariantArray = input.as_any().downcast_ref().ok_or_else(|| {
        ArrowError::InvalidArgumentError(
            "expected a VariantArray as the input for variant_get".to_owned(),
        )
    })?;

    // TODO: can we use OffsetBuffer and NullBuffer here instead?
    //       I couldn't find a way to set individual indices here, so went with vecs.
    let mut offsets = vec![0; variant_array.len()];
    let mut nulls = if let Some(struct_nulls) = variant_array.nulls() {
        struct_nulls.iter().collect()
    } else {
        vec![true; variant_array.len()]
    };

    for path in options
        .path
        .0
        .iter()
        .take(options.path.0.len().saturating_sub(1))
    {
        match path {
            VariantPathElement::Field { name } => {
                go_to_object_field(variant_array, name, &mut offsets, &mut nulls)?;
            }
            VariantPathElement::Index { offset } => {
                go_to_array_index(variant_array, *offset, &mut offsets, &mut nulls)?;
            }
        }
    }

    let as_type = options.as_type.ok_or_else(|| {
        ArrowError::NotYetImplemented(
            "getting variant from variant is not implemented yet".to_owned(),
        )
    })?;
    match as_type.data_type() {
        DataType::UInt64 => {
            Ok(Arc::new(get_top_level_primitive::<UInt64Type, _>(
                variant_array,
                |variant, builder| {
                    match variant {
                        // TODO: narrowing?
                        Variant::Int64(i) => builder.append_value(i as u64),
                        Variant::Null => builder.append_null(),
                        // TODO: throw error based on CastOptions
                        _ => builder.append_null(),
                    }
                },
                &offsets,
                &nulls,
            )?))
        }
        other_type => Err(ArrowError::NotYetImplemented(format!(
            "getting variant as {} is not yet implemented",
            other_type
        ))),
    }
}

fn get_top_level_primitive<T: ArrowPrimitiveType, F: Fn(Variant, &mut PrimitiveBuilder<T>)>(
    variant_array: &VariantArray,
    extractor: F,
    offsets: &[i32],
    nulls: &[bool],
) -> Result<PrimitiveArray<T>> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(variant_array.len());
    for i in 0..variant_array.len() {
        if !nulls[i] {
            builder.append_null();
            continue;
        }
        let variant = variant_array.value_at_offset(i, offsets[i] as usize)?;

        extractor(variant, &mut builder);
    }

    Ok(builder.finish())
}

fn go_to_object_field(
    variant_array: &VariantArray,
    name: &str,
    offsets: &mut [i32],
    nulls: &mut [bool],
) -> Result<()> {
    for i in 0..variant_array.len() {
        if !nulls[i] {
            continue;
        }
        let variant = variant_array.value_at_offset(i, offsets[i] as usize)?;

        let Variant::Object(variant) = variant else {
            nulls[i] = false;
            continue;
        };

        let offset = variant.field_offset(name).unwrap_or(Ok(0))?;
        offsets[i] += offset as i32;
    }

    Ok(())
}

fn go_to_array_index(
    variant_array: &VariantArray,
    index: usize,
    offsets: &mut [i32],
    nulls: &mut [bool],
) -> Result<()> {
    for i in 0..variant_array.len() {
        if !nulls[i] {
            continue;
        }
        let variant = variant_array.value_at_offset(i, offsets[i] as usize)?;

        let Variant::List(variant) = variant else {
            nulls[i] = false;
            continue;
        };

        if index >= variant.len() {
            nulls[i] = false;
            continue;
        }

        let offset = variant.get_offset(index)?;
        offsets[i] += offset as i32;
    }

    Ok(())
}

/// Controls the action of the variant_get kernel
///
/// If `as_type` is specified `cast_options` controls what to do if the
///
pub struct GetOptions<'a> {
    /// What path to extract
    path: VariantPath,
    /// if `as_type` is None, the returned array will itself be a StructArray with Variant values
    ///
    /// if `as_type` is `Some(type)` the field is returned as the specified type if possible. To specify returning
    /// a Variant, pass a Field with variant type in the metadata.
    as_type: Option<Field>,
    /// Controls the casting behavior (e.g. error vs substituting null on cast error)
    cast_options: CastOptions<'a>,
}

/// Represents a qualified path to a potential subfield of an element
pub struct VariantPath(Vec<VariantPathElement>);

/// Element of a path
enum VariantPathElement {
    /// Access field with name `name`
    Field { name: String },
    /// Access the list element offset
    Index { offset: usize },
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{
            Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray,
        },
        datatypes::UInt64Type,
    };
    use arrow_schema::Field;
    use parquet_variant::{Variant, VariantBuilder};

    use crate::VariantArrayBuilder;

    use super::{variant_get, GetOptions, VariantPath};

    #[test]
    fn primitive_u64() {
        let mut builder = VariantBuilder::new();
        builder.append_value(1234i64);
        let (metadata, value) = builder.finish();

        let mut builder = VariantArrayBuilder::new(1);
        builder.append_variant(Variant::try_new(&metadata, &value).unwrap());

        let variant_array = builder.build();

        let input = Arc::new(variant_array) as ArrayRef;

        let result = variant_get(
            &input,
            GetOptions {
                path: VariantPath(vec![]),
                as_type: Some(Field::new("", UInt64Type::DATA_TYPE, true)),
                cast_options: Default::default(),
            },
        )
        .unwrap();

        let result: &PrimitiveArray<UInt64Type> = result.as_any().downcast_ref().unwrap();
        assert!(result.nulls().is_none());
        let result = result.values().to_vec();
        assert_eq!(result, vec![1234]);
    }
}
