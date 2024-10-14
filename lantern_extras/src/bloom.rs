use fastbloom::BloomFilter;
use pgrx::prelude::*;
use pgrx::PostgresType;
use serde::{Deserialize, Serialize};

const BLOOM_HASHER_SEED: u128 = 42;

// this is called "Bloom" to make sure postgres type has the name 'bloom'
#[derive(Clone, Debug, Serialize, Deserialize, PostgresType)]
pub struct Bloom {
    #[serde(with = "serde_bytes")]
    bitmap: Vec<u8>,
    num_hashes: u32,
}

impl From<BloomFilter> for Bloom {
    fn from(bloom_filter: BloomFilter) -> Self {
        let v = bloom_filter.as_slice().to_vec();
        let bitmap =
            unsafe { Vec::from_raw_parts(v.as_ptr() as *mut u8, v.len() * 8, v.capacity() * 8) };
        std::mem::forget(v);
        Bloom {
            bitmap,
            num_hashes: bloom_filter.num_hashes(),
        }
    }
}

impl From<Bloom> for BloomFilter {
    #[inline(never)]
    fn from(bloom: Bloom) -> Self {
        let bitmap = unsafe {
            Vec::from_raw_parts(
                bloom.bitmap.as_ptr() as *mut u64,
                bloom.bitmap.len() / 8,
                bloom.bitmap.capacity() / 8,
            )
        };
        std::mem::forget(bloom.bitmap);
        BloomFilter::from_vec(bitmap)
            .seed(&BLOOM_HASHER_SEED)
            .hashes(bloom.num_hashes)
    }
}

fn array_to_bloom<T: std::hash::Hash>(arr: Vec<T>) -> Bloom {
    let mut bloom = BloomFilter::with_false_pos(0.01)
        .seed(&BLOOM_HASHER_SEED)
        .expected_items(arr.len());
    for i in arr {
        bloom.insert(&i);
    }
    return bloom.into();
}

#[pg_extern(immutable, parallel_safe, name = "array_to_bloom")]
fn array_to_bloom_smallint(arr: Vec<i16>) -> Bloom {
    return array_to_bloom(arr);
}

#[pg_extern(immutable, parallel_safe, name = "array_to_bloom")]
fn array_to_bloom_integer(arr: Vec<i32>) -> Bloom {
    return array_to_bloom(arr);
}

#[pg_extern(immutable, parallel_safe, name = "array_to_bloom")]
fn array_to_bloom_bigint(arr: Vec<i64>) -> Bloom {
    return array_to_bloom(arr);
}

#[pg_extern(requires = [Bloom])]
fn elem_in_bloom(elem: i32, bloom: Bloom) -> bool {
    let bloom: BloomFilter = bloom.into();
    bloom.contains(&elem)
}

extension_sql!(
    r#"
    CREATE CAST (smallint[] AS bloom) WITH FUNCTION array_to_bloom(smallint[]);
    CREATE CAST (integer[] AS bloom) WITH FUNCTION array_to_bloom(integer[]);
    CREATE CAST (bigint[] AS bloom) WITH FUNCTION array_to_bloom(bigint[]);
"#,
    name = "bloom_type_casts",
    requires = [
        Bloom,
        array_to_bloom_smallint,
        array_to_bloom_integer,
        array_to_bloom_bigint,
    ]
);
