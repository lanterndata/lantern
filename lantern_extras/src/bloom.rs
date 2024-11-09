use fastbloom::BloomFilter;
use pgrx::prelude::*;
use pgrx::PostgresType;
use serde::{Deserialize, Serialize};
use xorf::{BinaryFuse8, Filter};

const BLOOM_HASHER_SEED: u128 = 42;

// this is called "Bloom" to make sure postgres type has the name 'bloom'
#[derive(Clone, Debug, Serialize, Deserialize, PostgresType)]
pub struct Bloom {
    #[serde(with = "serde_bytes")]
    bitmap: Vec<u8>,
    xorf: BinaryFuse8,
    num_hashes: u32,
}

impl Bloom {
    pub fn contains(&self, elem: &u64) -> bool {
        self.xorf.contains(elem)
    }
}
impl From<BinaryFuse8> for Bloom {
    fn from(bloom_filter: BinaryFuse8) -> Self {
        // let v = bloom_filter.as_slice().to_vec();
        // let bitmap =
        //     unsafe { Vec::from_raw_parts(v.as_ptr() as *mut u8, v.len() * 8, v.capacity() * 8) };
        // std::mem::forget(v);
        Bloom {
            bitmap: Vec::new(),
            xorf: bloom_filter,
            num_hashes: 0,
            // num_hashes: bloom_filter.num_hashes(),
        }
    }
}

impl From<Bloom> for BinaryFuse8 {
    #[inline(never)]
    fn from(bloom: Bloom) -> Self {
        bloom.xorf
    }
}

fn array_to_bloom<T: Into<u64>>(arr: Vec<T>) -> Bloom {
    // let mut bloom = BinaryFuse8::::with_false_pos(0.01)
    //     .seed(&BLOOM_HASHER_SEED)
    //     .expected_items(arr.len());
    //
    // for i in arr {
    //     bloom.insert(&i);
    // }
    let arr_u64: Vec<u64> = arr.into_iter().map(|x| x.into()).collect();
    let bloom = BinaryFuse8::try_from(arr_u64).unwrap();
    return bloom.into();
}

#[pg_extern(immutable, parallel_safe, name = "array_to_bloom")]
fn array_to_bloom_smallint(arr: Vec<i16>) -> Bloom {
    let arr_u64: Vec<u64> = arr.iter().map(|&x| x as u64).collect();
    return array_to_bloom(arr_u64);
}

#[pg_extern(immutable, parallel_safe, name = "array_to_bloom")]
fn array_to_bloom_integer(arr: Vec<i32>) -> Bloom {
    let arr_u64: Vec<u64> = arr.iter().map(|&x| x as u64).collect();
    return array_to_bloom(arr_u64);
}

#[pg_extern(immutable, parallel_safe, name = "array_to_bloom")]
fn array_to_bloom_bigint(arr: Vec<i64>) -> Bloom {
    let arr_u64: Vec<u64> = arr.iter().map(|&x| x as u64).collect();
    return array_to_bloom(arr_u64);
}

#[pg_extern(requires = [Bloom])]
fn elem_in_bloom(elem: i32, bloom: Bloom) -> bool {
    let bloom: BinaryFuse8 = bloom.into();
    bloom.contains(&(elem as u64))
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
