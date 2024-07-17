use lantern_cli::external_index::cli::UMetricKind;
use lantern_cli::external_index::server::{END_MSG, ERR_MSG, INIT_MSG, PROTOCOL_HEADER_SIZE};
use lantern_cli::external_index::{self, cli::IndexServerArgs};
use std::fs;
use std::io::prelude::*;
use std::net::TcpStream;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::str;
use std::sync::Once;
use std::time::Duration;
use usearch::ffi::{IndexOptions, ScalarKind};
use usearch::Index;

static INIT: Once = Once::new();

pub fn initialize() {
    INIT.call_once(|| {
        // download index file for test
        // download quantized index file for test
        // download sift dataset
        // download pq table
        std::thread::spawn(move || {
            external_index::server::start_tcp_server(
                IndexServerArgs {
                    host: "127.0.0.1".to_owned(),
                    port: 8998,
                },
                None,
            )
            .unwrap();
        });
        std::thread::sleep(Duration::from_secs(1));
    });
}

#[tokio::test]
async fn test_external_index_server_invalid_header() {
    initialize();
    let mut stream = TcpStream::connect("127.0.0.1:8998").unwrap();
    let bytes_written = stream.write(&[0, 1, 1, 1, 1, 1]).unwrap();
    assert_eq!(bytes_written, 6);
    let mut buf: [u8; 64] = [0; 64];
    stream.read(&mut buf).unwrap();

    assert_eq!(
        u32::from_le_bytes(buf[..PROTOCOL_HEADER_SIZE].try_into().unwrap()),
        ERR_MSG
    );

    let expected_msg = "Invalid message header";
    assert_eq!(
        str::from_utf8(
            buf[PROTOCOL_HEADER_SIZE..PROTOCOL_HEADER_SIZE + expected_msg.len()]
                .try_into()
                .unwrap()
        )
        .unwrap(),
        expected_msg
    );
}

#[tokio::test]
async fn test_external_index_server_short_message() {
    initialize();
    let mut stream = TcpStream::connect("127.0.0.1:8998").unwrap();
    let bytes_written = stream.write(&[0, 1]).unwrap();
    assert_eq!(bytes_written, 2);
    let mut buf: [u8; 64] = [0; 64];
    stream.read(&mut buf).unwrap();

    assert_eq!(
        u32::from_le_bytes(buf[..PROTOCOL_HEADER_SIZE].try_into().unwrap()),
        ERR_MSG
    );

    let expected_msg = "Invalid frame received";
    assert_eq!(
        str::from_utf8(
            buf[PROTOCOL_HEADER_SIZE..PROTOCOL_HEADER_SIZE + expected_msg.len()]
                .try_into()
                .unwrap()
        )
        .unwrap(),
        expected_msg
    );
}

#[tokio::test]
async fn test_external_index_server_indexing() {
    initialize();
    let pq_codebook: *const f32 = std::ptr::null();
    let index_options = IndexOptions {
        dimensions: 3,
        metric: UMetricKind::from_u32(1).unwrap().value(),
        quantization: ScalarKind::F32,
        multi: false,
        connectivity: 12,
        expansion_add: 64,
        expansion_search: 32,
        num_threads: 0, // automatic
        pq_construction: false,
        pq_output: false,
        num_centroids: 0,
        num_subvectors: 0,
        codebook: pq_codebook,
    };

    let tuples = vec![
        (0, vec![0.0, 0.0, 0.0]),
        (1, vec![0.0, 0.0, 1.0]),
        (2, vec![0.0, 0.0, 2.0]),
        (3, vec![0.0, 0.0, 3.0]),
        (4, vec![0.0, 1.0, 0.0]),
        (5, vec![0.0, 1.0, 1.0]),
        (6, vec![0.0, 1.0, 2.0]),
        (7, vec![0.0, 1.0, 3.0]),
        (8, vec![1.0, 0.0, 0.0]),
        (9, vec![1.0, 0.0, 1.0]),
        (10, vec![1.0, 0.0, 2.0]),
        (11, vec![1.0, 0.0, 3.0]),
        (12, vec![1.0, 1.0, 0.0]),
        (13, vec![1.0, 1.0, 1.0]),
    ];

    let mut stream = TcpStream::connect("127.0.0.1:8998").unwrap();
    let init_msg = [
        INIT_MSG.to_le_bytes(),
        (0 as u32).to_le_bytes(),
        (1 as u32).to_le_bytes(),
        (1 as u32).to_le_bytes(),
        (index_options.dimensions as u32).to_le_bytes(),
        (index_options.connectivity as u32).to_le_bytes(),
        (index_options.expansion_add as u32).to_le_bytes(),
        (index_options.expansion_search as u32).to_le_bytes(),
        (index_options.num_centroids as u32).to_le_bytes(),
        (index_options.num_subvectors as u32).to_le_bytes(),
        (tuples.len() as u32).to_le_bytes(),
    ]
    .concat();

    let bytes_written = stream.write(&init_msg).unwrap();
    assert_eq!(bytes_written, init_msg.len());
    let mut buf: [u8; 1] = [1; 1];
    stream.read(&mut buf).unwrap();

    assert_eq!(buf[0], 0);
    let index = Index::new(&index_options).unwrap();
    index.reserve(tuples.len()).unwrap();
    for tuple in &tuples {
        index.add(tuple.0 as u64, &*tuple.1).unwrap();
        let tuple_buf = unsafe {
            let byte_count = tuple.1.len() * std::mem::size_of::<f32>();

            // Allocate a buffer for the bytes
            let mut byte_vec: Vec<u8> = Vec::with_capacity(byte_count);
            let float_slice = std::slice::from_raw_parts(tuple.1.as_ptr() as *const u8, byte_count);
            //
            // // Copy the bytes into the byte vector
            byte_vec.extend_from_slice(float_slice);
            let label = (tuple.0 as u64).to_le_bytes();
            vec![&label, byte_vec.as_slice()].concat()
        };
        stream.write_all(&tuple_buf).unwrap();
    }

    let buf = END_MSG.to_le_bytes();
    stream.write(&buf).unwrap();
    let index_file_name = "/tmp/test_external_index_server_indexing.usearch";
    let index_file_path = Path::new(&index_file_name);
    index.save(index_file_name).unwrap();
    let mut reader = fs::File::open(index_file_path).unwrap();
    let index_size = reader.metadata().unwrap().size();
    let mut expected_index_buffer = Vec::with_capacity(index_size as usize);
    reader.read_to_end(&mut expected_index_buffer).unwrap();

    // receiver num tuples added
    let mut uint64_buf = [0; 8];
    stream.read_exact(&mut uint64_buf).unwrap();
    assert_eq!(u64::from_le_bytes(uint64_buf), tuples.len() as u64);

    stream.read_exact(&mut uint64_buf).unwrap();
    assert_eq!(u64::from_le_bytes(uint64_buf), index_size as u64);

    let mut received_index_buffer = vec![0; index_size as usize];
    stream.read_exact(&mut received_index_buffer).unwrap();

    let received_index = Index::new(&index_options).unwrap();
    received_index.reserve(tuples.len()).unwrap();
    Index::load_from_buffer(&received_index, &received_index_buffer).unwrap();

    assert_eq!(index.size(), received_index.size());
}

#[tokio::test]
async fn test_external_index_server_indexing_scalar_quantization() {
    initialize();
    let pq_codebook: *const f32 = std::ptr::null();
    let index_options = IndexOptions {
        dimensions: 3,
        metric: UMetricKind::from_u32(1).unwrap().value(),
        quantization: ScalarKind::F16,
        multi: false,
        connectivity: 12,
        expansion_add: 64,
        expansion_search: 32,
        num_threads: 0, // automatic
        pq_construction: false,
        pq_output: false,
        num_centroids: 0,
        num_subvectors: 0,
        codebook: pq_codebook,
    };

    let tuples = vec![
        (0, vec![0.0, 0.0, 0.0]),
        (1, vec![0.0, 0.0, 1.0]),
        (2, vec![0.0, 0.0, 2.0]),
        (3, vec![0.0, 0.0, 3.0]),
        (4, vec![0.0, 1.0, 0.0]),
        (5, vec![0.0, 1.0, 1.0]),
        (6, vec![0.0, 1.0, 2.0]),
        (7, vec![0.0, 1.0, 3.0]),
        (8, vec![1.0, 0.0, 0.0]),
        (9, vec![1.0, 0.0, 1.0]),
        (10, vec![1.0, 0.0, 2.0]),
        (11, vec![1.0, 0.0, 3.0]),
        (12, vec![1.0, 1.0, 0.0]),
        (13, vec![1.0, 1.0, 1.0]),
    ];

    let mut stream = TcpStream::connect("127.0.0.1:8998").unwrap();
    let init_msg = [
        INIT_MSG.to_le_bytes(),
        (0 as u32).to_le_bytes(),
        (1 as u32).to_le_bytes(),
        (3 as u32).to_le_bytes(),
        (index_options.dimensions as u32).to_le_bytes(),
        (index_options.connectivity as u32).to_le_bytes(),
        (index_options.expansion_add as u32).to_le_bytes(),
        (index_options.expansion_search as u32).to_le_bytes(),
        (index_options.num_centroids as u32).to_le_bytes(),
        (index_options.num_subvectors as u32).to_le_bytes(),
        (tuples.len() as u32).to_le_bytes(),
    ]
    .concat();

    let bytes_written = stream.write(&init_msg).unwrap();
    assert_eq!(bytes_written, init_msg.len());
    let mut buf: [u8; 1] = [1; 1];
    stream.read(&mut buf).unwrap();

    assert_eq!(buf[0], 0);
    let index = Index::new(&index_options).unwrap();
    index.reserve(tuples.len()).unwrap();
    for tuple in &tuples {
        index.add(tuple.0 as u64, &*tuple.1).unwrap();
        let tuple_buf = unsafe {
            let byte_count = tuple.1.len() * std::mem::size_of::<f32>();

            // Allocate a buffer for the bytes
            let mut byte_vec: Vec<u8> = Vec::with_capacity(byte_count);
            let float_slice = std::slice::from_raw_parts(tuple.1.as_ptr() as *const u8, byte_count);
            //
            // // Copy the bytes into the byte vector
            byte_vec.extend_from_slice(float_slice);
            let label = (tuple.0 as u64).to_le_bytes();
            vec![&label, byte_vec.as_slice()].concat()
        };
        stream.write_all(&tuple_buf).unwrap();
    }

    let buf = END_MSG.to_le_bytes();
    stream.write(&buf).unwrap();
    let index_file_name = "/tmp/test_external_index_server_indexing.usearch";
    let index_file_path = Path::new(&index_file_name);
    index.save(index_file_name).unwrap();
    let mut reader = fs::File::open(index_file_path).unwrap();
    let index_size = reader.metadata().unwrap().size();
    let mut expected_index_buffer = Vec::with_capacity(index_size as usize);
    reader.read_to_end(&mut expected_index_buffer).unwrap();

    // receiver num tuples added
    let mut uint64_buf = [0; 8];
    stream.read_exact(&mut uint64_buf).unwrap();
    assert_eq!(u64::from_le_bytes(uint64_buf), tuples.len() as u64);

    stream.read_exact(&mut uint64_buf).unwrap();
    assert_eq!(u64::from_le_bytes(uint64_buf), index_size as u64);

    let mut received_index_buffer = vec![0; index_size as usize];
    stream.read_exact(&mut received_index_buffer).unwrap();

    let received_index = Index::new(&index_options).unwrap();
    received_index.reserve(tuples.len()).unwrap();
    Index::load_from_buffer(&received_index, &received_index_buffer).unwrap();

    assert_eq!(index.size(), received_index.size());
}
#[tokio::test]
async fn test_external_index_server_indexing_pq() {
    initialize();
    // codebook with num_centroids = 4, num_subvectors = 3
    let pq_codebook = vec![
        [0.0, 0.1, 0.0],
        [0.1, 0.1, 0.1],
        [0.1, 0.1, 0.2],
        [0.1, 0.2, 0.1],
    ];
    let codebook = pq_codebook.concat().as_ptr();
    let index_options = IndexOptions {
        dimensions: 3,
        metric: UMetricKind::from_u32(1).unwrap().value(),
        quantization: ScalarKind::F32,
        multi: false,
        connectivity: 12,
        expansion_add: 64,
        expansion_search: 32,
        num_threads: 0, // automatic
        pq_construction: true,
        pq_output: true,
        num_centroids: 4,
        num_subvectors: 3,
        codebook,
    };

    let tuples = vec![
        (0, vec![0.0, 0.0, 0.0]),
        (1, vec![0.0, 0.0, 1.0]),
        (2, vec![0.0, 0.0, 2.0]),
        (3, vec![0.0, 0.0, 3.0]),
        (4, vec![0.0, 1.0, 0.0]),
        (5, vec![0.0, 1.0, 1.0]),
        (6, vec![0.0, 1.0, 2.0]),
        (7, vec![0.0, 1.0, 3.0]),
        (8, vec![1.0, 0.0, 0.0]),
        (9, vec![1.0, 0.0, 1.0]),
        (10, vec![1.0, 0.0, 2.0]),
        (11, vec![1.0, 0.0, 3.0]),
        (12, vec![1.0, 1.0, 0.0]),
        (13, vec![1.0, 1.0, 1.0]),
    ];

    let mut stream = TcpStream::connect("127.0.0.1:8998").unwrap();
    let init_msg = [
        INIT_MSG.to_le_bytes(),
        (1 as u32).to_le_bytes(),
        (1 as u32).to_le_bytes(),
        (0 as u32).to_le_bytes(),
        (index_options.dimensions as u32).to_le_bytes(),
        (index_options.connectivity as u32).to_le_bytes(),
        (index_options.expansion_add as u32).to_le_bytes(),
        (index_options.expansion_search as u32).to_le_bytes(),
        (index_options.num_centroids as u32).to_le_bytes(),
        (index_options.num_subvectors as u32).to_le_bytes(),
        (tuples.len() as u32).to_le_bytes(),
    ]
    .concat();

    let bytes_written = stream.write(&init_msg).unwrap();
    assert_eq!(bytes_written, init_msg.len());

    // send codebook
    for vec in &pq_codebook {
        let byte_count = vec.len() * std::mem::size_of::<f32>();
        let mut byte_vec: Vec<u8> = Vec::with_capacity(byte_count);
        let tuple_buf = unsafe {
            // Allocate a buffer for the bytes
            let float_slice = std::slice::from_raw_parts(vec.as_ptr() as *const u8, byte_count);

            // Copy the bytes into the byte vector
            byte_vec.extend_from_slice(float_slice);
            byte_vec.as_slice()
        };
        stream.write_all(&tuple_buf).unwrap();
    }
    let buf = END_MSG.to_le_bytes();
    stream.write(&buf).unwrap();

    let mut buf: [u8; 1] = [1; 1];
    stream.read(&mut buf).unwrap();

    assert_eq!(buf[0], 0);
    let index = Index::new(&index_options).unwrap();
    index.reserve(tuples.len()).unwrap();
    for tuple in &tuples {
        index.add(tuple.0 as u64, &*tuple.1).unwrap();
        let tuple_buf = unsafe {
            let byte_count = tuple.1.len() * std::mem::size_of::<f32>();

            // Allocate a buffer for the bytes
            let mut byte_vec: Vec<u8> = Vec::with_capacity(byte_count);
            let float_slice = std::slice::from_raw_parts(tuple.1.as_ptr() as *const u8, byte_count);
            //
            // // Copy the bytes into the byte vector
            byte_vec.extend_from_slice(float_slice);
            let label = (tuple.0 as u64).to_le_bytes();
            vec![&label, byte_vec.as_slice()].concat()
        };
        stream.write_all(&tuple_buf).unwrap();
    }

    let buf = END_MSG.to_le_bytes();
    stream.write(&buf).unwrap();

    let index_file_name = "/tmp/test_external_index_server_indexing.usearch";
    let index_file_path = Path::new(&index_file_name);
    index.save(index_file_name).unwrap();
    let mut reader = fs::File::open(index_file_path).unwrap();
    let index_size = reader.metadata().unwrap().size();
    let mut expected_index_buffer = Vec::with_capacity(index_size as usize);
    reader.read_to_end(&mut expected_index_buffer).unwrap();

    // receiver num tuples added
    let mut uint64_buf = [0; 8];
    stream.read_exact(&mut uint64_buf).unwrap();
    assert_eq!(u64::from_le_bytes(uint64_buf), tuples.len() as u64);

    stream.read_exact(&mut uint64_buf).unwrap();
    assert_eq!(u64::from_le_bytes(uint64_buf), index_size as u64);

    let mut received_index_buffer = vec![0; index_size as usize];
    stream.read_exact(&mut received_index_buffer).unwrap();
    assert_eq!(received_index_buffer.len(), expected_index_buffer.len());
}
