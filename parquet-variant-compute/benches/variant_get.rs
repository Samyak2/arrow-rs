use std::sync::Arc;

use arrow::{
    array::{ArrayRef, ArrowPrimitiveType},
    datatypes::UInt64Type,
};
use arrow_schema::Field;
use criterion::{criterion_group, criterion_main, Criterion};
use parquet_variant::{path::VariantPath, Variant, VariantBuilder};
use parquet_variant_compute::{
    variant_get::{variant_get, GetOptions},
    VariantArray, VariantArrayBuilder,
};
use rand::{rngs::StdRng, Rng, SeedableRng};

fn create_primitive_variant(size: usize) -> VariantArray {
    let mut rng = StdRng::seed_from_u64(42);

    let mut variant_builder = VariantArrayBuilder::new(1);

    for _ in 0..size {
        let mut builder = VariantBuilder::new();
        builder.append_value(rng.random::<i64>());
        let (metadata, value) = builder.finish();
        variant_builder.append_variant(Variant::try_new(&metadata, &value).unwrap());
    }

    variant_builder.build()
}

pub fn variant_get_bench(c: &mut Criterion) {
    let variant_array = create_primitive_variant(8192);
    let input: ArrayRef = Arc::new(variant_array);

    let options = GetOptions {
        path: VariantPath(vec![]),
        as_type: Some(Field::new("", UInt64Type::DATA_TYPE, true)),
        cast_options: Default::default(),
    };

    c.bench_function("variant_get_primitive_columnar", |b| {
        b.iter(|| variant_get(&input.clone(), options.clone()))
    });

    c.bench_function("variant_get_primitive_rowwise", |b| {
        b.iter(|| variant_get(&input.clone(), options.clone()))
    });
}

criterion_group!(benches, variant_get_bench);
criterion_main!(benches);
