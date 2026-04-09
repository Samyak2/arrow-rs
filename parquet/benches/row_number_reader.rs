// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::hint;
use std::sync::Arc;

use criterion::*;
use parquet::arrow::array_reader::ArrayReader;
use parquet::arrow::array_reader::row_number::RowNumberReader;
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData,
};
use parquet::schema::types::{SchemaDescriptor, Type as SchemaType};

fn create_test_metadata(num_row_groups: usize, rows_per_group: i64) -> ParquetMetaData {
    let schema = SchemaType::group_type_builder("schema")
        .with_fields(vec![Arc::new(
            SchemaType::primitive_type_builder("col", PhysicalType::INT32)
                .build()
                .unwrap(),
        )])
        .build()
        .unwrap();
    let schema_descr = Arc::new(SchemaDescriptor::new(Arc::new(schema)));

    let row_groups: Vec<_> = (0..num_row_groups)
        .map(|i| {
            let columns: Vec<_> = schema_descr
                .columns()
                .iter()
                .map(|col| ColumnChunkMetaData::builder(col.clone()).build().unwrap())
                .collect();
            RowGroupMetaData::builder(schema_descr.clone())
                .set_num_rows(rows_per_group)
                .set_ordinal(i as i16)
                .set_total_byte_size(100)
                .set_column_metadata(columns)
                .build()
                .unwrap()
        })
        .collect();

    let total_rows = num_row_groups as i64 * rows_per_group;
    let file_metadata = FileMetaData::new(1, total_rows, None, None, schema_descr, None);
    ParquetMetaData::new(file_metadata, row_groups)
}

fn criterion_benchmark(c: &mut Criterion) {
    let metadata = create_test_metadata(100, 10_000);
    let batch_size = 8192;

    c.bench_function("row_number_read_consume", |b| {
        b.iter(|| {
            let all_rgs: Vec<_> = metadata.row_groups().iter().collect();
            let mut reader = RowNumberReader::try_new(&metadata, all_rgs.into_iter()).unwrap();
            loop {
                let n = reader.read_records(batch_size).unwrap();
                if n == 0 {
                    break;
                }
                let batch = reader.consume_batch().unwrap();
                hint::black_box(&batch);
            }
        })
    });

    c.bench_function("row_number_skip_and_read", |b| {
        b.iter(|| {
            let all_rgs: Vec<_> = metadata.row_groups().iter().collect();
            let mut reader = RowNumberReader::try_new(&metadata, all_rgs.into_iter()).unwrap();
            let mut is_skip = false;
            loop {
                if is_skip {
                    let n = reader.skip_records(batch_size).unwrap();
                    if n == 0 {
                        break;
                    }
                } else {
                    let n = reader.read_records(batch_size).unwrap();
                    if n == 0 {
                        break;
                    }
                    let batch = reader.consume_batch().unwrap();
                    hint::black_box(&batch);
                }
                is_skip = !is_skip;
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
