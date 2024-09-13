use isahc::{ReadResponseExt, Request};
use lantern_cli::external_index::cli::UMetricKind;
use lantern_cli::external_index::server::{
    END_MSG, ERR_MSG, INIT_MSG, PROTOCOL_HEADER_SIZE, PROTOCOL_VERSION,
};
use lantern_cli::external_index::{self, cli::IndexServerArgs};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct};
use serde::Deserialize;
use std::fs;
use std::io::prelude::*;
use std::net::TcpStream;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::Command;
use std::str;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use usearch::ffi::{IndexOptions, ScalarKind};
use usearch::Index;

static INIT: Once = Once::new();
static SSL_INIT: Once = Once::new();

#[derive(Debug)]
struct NoServerAuth;

#[derive(Deserialize)]
struct ServerStatusResponse {
    status: u8,
}

impl ServerCertVerifier for NoServerAuth {
    fn verify_server_cert(
        &self,
        _roots: &CertificateDer,
        _presented_certs: &[CertificateDer],
        _dns_name: &rustls::pki_types::ServerName,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

fn initialize() {
    INIT.call_once(|| {
        std::thread::spawn(move || {
            external_index::server::start_tcp_server(
                IndexServerArgs {
                    host: "127.0.0.1".to_owned(),
                    port: 8998,
                    status_port: 8999,
                    tmp_dir: "/tmp".to_owned(),
                    cert: None,
                    key: None,
                },
                None,
            )
            .unwrap();
        });
        std::thread::sleep(Duration::from_secs(2));
    });
}

fn initialize_ssl() {
    SSL_INIT.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("Failed to install default CryptoProvider");
        std::thread::spawn(move || {
            Command::new("openssl")
                .args([
                    "req",
                    "-x509",
                    "-nodes",
                    "-days",
                    "365",
                    "-newkey",
                    "rsa:2048",
                    "-keyout",
                    "/tmp/lantern-index-server-test-key.pem",
                    "-out",
                    "/tmp/lantern-index-server-test-cert.pem",
                    "-subj",
                    "/C=US/ST=California/L=San Francisco/O=Lantern/CN=lantern.dev",
                ])
                .output()
                .unwrap();
            external_index::server::start_tcp_server(
                IndexServerArgs {
                    host: "127.0.0.1".to_owned(),
                    port: 8990,
                    status_port: 8991,
                    tmp_dir: "/tmp".to_owned(),
                    cert: Some("/tmp/lantern-index-server-test-cert.pem".to_owned()),
                    key: Some("/tmp/lantern-index-server-test-key.pem".to_owned()),
                },
                None,
            )
            .unwrap();
        });
        std::thread::sleep(Duration::from_secs(2));
    });
}

#[tokio::test]
async fn test_external_index_server_invalid_header() {
    initialize();
    let mut stream = TcpStream::connect("127.0.0.1:8998").unwrap();
    let mut uint32_buf = [0; 4];
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), PROTOCOL_VERSION);
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), 0x1);
    let bytes_written = stream.write(&[0, 1, 1, 1, 1, 1]).unwrap();
    assert_eq!(bytes_written, 6);
    let mut header_buf: [u8; PROTOCOL_HEADER_SIZE] = [0; PROTOCOL_HEADER_SIZE];
    stream.read(&mut header_buf).unwrap();
    let expected_msg = "Invalid message header";
    let expected_msg_bytes = expected_msg.as_bytes();

    assert_eq!(u32::from_le_bytes(header_buf.try_into().unwrap()), ERR_MSG);

    stream.read_exact(&mut header_buf).unwrap();
    let err_msg_size = u32::from_le_bytes(header_buf);
    assert_eq!(err_msg_size, expected_msg_bytes.len() as u32);

    let mut error_buf = vec![0 as u8; err_msg_size as usize];
    stream.read_exact(&mut error_buf).unwrap();

    assert_eq!(str::from_utf8(error_buf.as_slice()).unwrap(), expected_msg);
}

#[tokio::test]
async fn test_external_index_server_short_message() {
    initialize();
    let mut stream = TcpStream::connect("127.0.0.1:8998").unwrap();
    let mut uint32_buf = [0; 4];
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), PROTOCOL_VERSION);
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), 0x1);
    let bytes_written = stream.write(&[0, 1]).unwrap();
    assert_eq!(bytes_written, 2);
    let mut header_buf: [u8; PROTOCOL_HEADER_SIZE] = [0; PROTOCOL_HEADER_SIZE];
    stream.read(&mut header_buf).unwrap();
    let expected_msg = "Invalid frame received";
    let expected_msg_bytes = expected_msg.as_bytes();

    assert_eq!(u32::from_le_bytes(header_buf.try_into().unwrap()), ERR_MSG);

    stream.read_exact(&mut header_buf).unwrap();
    let err_msg_size = u32::from_le_bytes(header_buf);
    assert_eq!(err_msg_size, expected_msg_bytes.len() as u32);

    let mut error_buf = vec![0 as u8; err_msg_size as usize];
    stream.read_exact(&mut error_buf).unwrap();

    assert_eq!(str::from_utf8(error_buf.as_slice()).unwrap(), expected_msg);
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
    let mut uint32_buf = [0; 4];
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), PROTOCOL_VERSION);
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), 0x1);

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
        ((tuples.len() / 2) as u32).to_le_bytes(), // test resizing
    ]
    .concat();

    let bytes_written = stream.write(&init_msg).unwrap();
    assert_eq!(bytes_written, init_msg.len());
    let mut buf: [u8; 1] = [1; 1];
    stream.read(&mut buf).unwrap();

    assert_eq!(buf[0], 0);

    let request = Request::get(&format!("http://127.0.0.1:8999"))
        .body("")
        .unwrap();
    let mut response = isahc::send(request).unwrap();
    let mut body: Vec<u8> = Vec::new();
    response.copy_to(&mut body).unwrap();
    let body_json = String::from_utf8(body).unwrap();
    let body_json: ServerStatusResponse = serde_json::from_str(&body_json).unwrap();
    assert_eq!(body_json.status, 1);

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
    let received_index_size = u64::from_le_bytes(uint64_buf);
    assert!(received_index_size > 0);

    let mut received_index_buffer = vec![0; received_index_size as usize];
    stream.read_exact(&mut received_index_buffer).unwrap();

    let received_index = Index::new(&index_options).unwrap();
    received_index.reserve(tuples.len()).unwrap();
    Index::load_from_buffer(&received_index, &received_index_buffer).unwrap();

    assert_eq!(index.size(), received_index.size());
    drop(stream);
    std::thread::sleep(Duration::from_secs(1));

    let request = Request::get(&format!("http://127.0.0.1:8999"))
        .body("")
        .unwrap();
    let mut response = isahc::send(request).unwrap();
    let mut body: Vec<u8> = Vec::new();
    response.copy_to(&mut body).unwrap();
    let body_json = String::from_utf8(body).unwrap();
    let body_json: ServerStatusResponse = serde_json::from_str(&body_json).unwrap();
    assert_eq!(body_json.status, 3);
}

#[tokio::test]
async fn test_external_index_server_indexing_ssl() {
    initialize_ssl();
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
    // Server details
    let hostname = "localhost";
    let port = 8990;
    let addr = format!("{}:{}", hostname, port);

    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoServerAuth))
        .with_no_client_auth();
    let config = Arc::new(config);

    let mut sock = TcpStream::connect(addr).unwrap();
    let mut conn = rustls::ClientConnection::new(config, "localhost".try_into().unwrap()).unwrap();
    let mut stream = rustls::Stream::new(&mut conn, &mut sock);
    let mut uint32_buf = [0; 4];
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), PROTOCOL_VERSION);
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), 0x1);

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
        ((tuples.len() / 2) as u32).to_le_bytes(), // test resizing
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
    let received_index_size = u64::from_le_bytes(uint64_buf);
    assert!(received_index_size > 0);

    let mut received_index_buffer = vec![0; received_index_size as usize];
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
    let mut uint32_buf = [0; 4];
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), PROTOCOL_VERSION);
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), 0x1);
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
    let received_index_size = u64::from_le_bytes(uint64_buf);
    assert!(received_index_size > 0);

    let mut received_index_buffer = vec![0; received_index_size as usize];
    stream.read_exact(&mut received_index_buffer).unwrap();

    let received_index = Index::new(&index_options).unwrap();
    received_index.reserve(tuples.len()).unwrap();
    Index::load_from_buffer(&received_index, &received_index_buffer).unwrap();

    assert_eq!(index.size(), received_index.size());
}

#[tokio::test]
async fn test_external_index_server_indexing_hamming_distance() {
    initialize();
    let pq_codebook: *const f32 = std::ptr::null();
    let index_options = IndexOptions {
        dimensions: 3 * 32,
        metric: UMetricKind::from_u32(8).unwrap().value(),
        quantization: ScalarKind::B1,
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
    let mut uint32_buf = [0; 4];
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), PROTOCOL_VERSION);
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), 0x1);
    let init_msg = [
        INIT_MSG.to_le_bytes(),
        (0 as u32).to_le_bytes(),
        (8 as u32).to_le_bytes(),
        (5 as u32).to_le_bytes(),
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
    let received_index_size = u64::from_le_bytes(uint64_buf);
    assert!(received_index_size > 0);

    let mut received_index_buffer = vec![0; received_index_size as usize];
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
    let mut uint32_buf = [0; 4];
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), PROTOCOL_VERSION);
    stream.read_exact(&mut uint32_buf).unwrap();
    assert_eq!(u32::from_le_bytes(uint32_buf), 0x1);
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
    let received_index_size = u64::from_le_bytes(uint64_buf);
    assert!(received_index_size > 0);

    // TODO::check why index size sometimes differ from our local index
    let mut received_index_buffer = vec![0; received_index_size as usize];
    stream.read_exact(&mut received_index_buffer).unwrap();
}
