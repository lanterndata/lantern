use pgrx::prelude::*;

use itertools::Itertools;
use std::fs::File;
use std::io::{BufReader, Read};
use std::mem::size_of;
use std::path::Path;

trait VectorElement {
    fn from_le_bytes(bytes: &[u8]) -> Self;
}

impl VectorElement for f32 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        return f32::from_le_bytes(bytes.try_into().unwrap());
    }
}
impl VectorElement for i32 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        return i32::from_le_bytes(bytes.try_into().unwrap());
    }
}
impl VectorElement for u8 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        return u8::from_le_bytes(bytes.try_into().unwrap());
    }
}

#[pg_extern]
fn parse_fvecs(
    path: String,
    count: i32,
) -> TableIterator<'static, (name!(vector, Vec<Option<f32>>),)> {
    let path = Path::new(&path);
    let mut f = File::open(path).unwrap();

    return match parse_vecs::<f32>(&mut f, count) {
        Ok(vectors) => TableIterator::new(vectors.into_iter().map(|v| (v,))),

        Err(e) => {
            error!("error parsing fvecs file: {}", e);
        }
    };
}

#[pg_extern]
fn parse_ivecs(
    path: String,
    count: i32,
) -> TableIterator<'static, (name!(vector, Vec<Option<i32>>),)> {
    let path = Path::new(&path);
    let mut f = File::open(path).unwrap();

    return match parse_vecs::<i32>(&mut f, count) {
        Ok(vectors) => TableIterator::new(vectors.into_iter().map(|v| (v,))),
        Err(e) => {
            error!("error parsing fvecs file: {}", e);
        }
    };
}

#[pg_extern]
fn parse_bvecs(
    path: String,
    count: i32,
) -> TableIterator<'static, (name!(vector, Vec<Option<i16>>),)> {
    let path = Path::new(&path);
    let mut f = File::open(path).unwrap();

    return match parse_vecs::<u8>(&mut f, count) {
        Ok(vectors) => TableIterator::new(
            vectors
                .into_iter()
                .map(|v| (v.into_iter().map(|o| o.map(|num| num as i16)).collect(),)),
        ),
        Err(e) => {
            error!("error parsing fvecs file: {}", e);
        }
    };
}

fn parse_vecs<T: VectorElement>(f: &mut File, count: i32) -> std::io::Result<Vec<Vec<Option<T>>>> {
    use std::io::Seek;

    let mut dimbuf = vec![0; 4];
    f.read_exact(&mut dimbuf).unwrap();
    let vector_dim = u32::from_le_bytes(dimbuf.try_into().unwrap()) as usize;

    f.seek(std::io::SeekFrom::Start(0)).unwrap();

    let buf = BufReader::new(f);

    let mut inconsistent_dim = false;
    let vectors = buf
        .bytes()
        //stores u32 of vec dimension followed by the vector
        .chunks((vector_dim + 1) * 4)
        .into_iter()
        .map(|mut chunk| {
            let dimbuf = chunk
                .by_ref()
                .take(size_of::<u32>())
                .map(|e| e.unwrap())
                .collect::<Vec<u8>>();

            let dim = u32::from_le_bytes(dimbuf.try_into().unwrap()) as usize;
            if dim != vector_dim {
                inconsistent_dim = true;
            }
            // drop the bytes for dim and assert that the rest exist per valid fvecs format
            return chunk
                .dropping(size_of::<u32>())
                .map(|e| e.unwrap())
                .collect::<Vec<u8>>();
        })
        .map(|chunk| {
            return chunk
                .chunks(size_of::<T>())
                .map(|b| T::from_le_bytes(b.try_into().unwrap()))
                .map(Some)
                .collect::<Vec<Option<T>>>();
        })
        .take(count as usize)
        // I could turn this into a lazy iterator but postgres runs SRFs and TRFs to completion
        // ignoring LIMITs and then post-processes LIMITs so the limit has to be given as an argument
        .collect::<Vec<Vec<Option<T>>>>();

    if inconsistent_dim {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "fvecs file with varying vector dimensions is not supported",
        ));
    }
    return Ok(vectors);
}
