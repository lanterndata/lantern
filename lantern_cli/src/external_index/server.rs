use super::cli::{IndexServerArgs, UMetricKind};
use bitvec::prelude::*;
use byteorder::{ByteOrder, LittleEndian};
use itertools::Itertools;
use rand::Rng;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use usearch::ffi::{IndexOptions, MetricKind, ScalarKind};
use usearch::Index;

use crate::logger::{LogLevel, Logger};
use crate::types::*;

const CHAR_BITS: usize = 8;
const LABEL_SIZE: usize = 8;
const INTEGER_SIZE: usize = 4;
const SOCKET_TIMEOUT: u64 = 5;
pub const PROTOCOL_HEADER_SIZE: usize = 4;
pub const INIT_MSG: u32 = 0x13333337;
pub const END_MSG: u32 = 0x31333337;
pub const ERR_MSG: u32 = 0x37333337;
// magic byte + pq + metric_kind + quantization + dim + m + efc + ef + num_centroids +
// num_subvectors + capacity
static INDEX_HEADER_LENGTH: usize = INTEGER_SIZE * 11;

enum VectorType {
    F32(Vec<f32>),
    I8(Vec<i8>),
}

type Row = (u64, VectorType);

struct ThreadSafeIndex(Index);

unsafe impl Sync for ThreadSafeIndex {}
unsafe impl Send for ThreadSafeIndex {}

fn parse_index_options(
    logger: Arc<Logger>,
    stream: Arc<Mutex<TcpStream>>,
    buf: &[u8],
) -> Result<IndexOptions, anyhow::Error> {
    let mut params: [u32; 9] = [0; 9];

    for i in 0..params.len() {
        let start_idx = INTEGER_SIZE * i;
        params[i] = u32::from_le_bytes(buf[start_idx..(start_idx + INTEGER_SIZE)].try_into()?);
    }

    let [pq, metric_kind, quantization, dim, m, ef_construction, ef, num_centroids, num_subvectors] =
        params;

    let pq = pq == 1;

    let quantization = match quantization {
        0..=1 => ScalarKind::F32,
        2 => ScalarKind::F64,
        3 => ScalarKind::F16,
        4 => ScalarKind::I8,
        5 => ScalarKind::B1,
        _ => anyhow::bail!("Invalid scalar quantization"),
    };

    let metric = UMetricKind::from_u32(metric_kind)?.value();

    logger.info(&format!("Index Params - pq: {pq}, metric_kind: {:?}, quantization: {:?}, dim: {dim}, m: {m}, ef_construction: {ef_construction}, ef: {ef}, num_subvectors: {num_subvectors}, num_centroids: {num_centroids}", metric, quantization));

    let mut pq_codebook: *const f32 = std::ptr::null();

    if pq {
        let expected_payload_size = dim as usize * INTEGER_SIZE;
        let mut buf = vec![0 as u8; expected_payload_size];
        let mut stream = stream.lock().unwrap();
        let mut codebook = Vec::with_capacity(num_centroids as usize);

        loop {
            match read_frame(&mut stream, &mut buf, expected_payload_size, None)? {
                ProtocolMessage::Exit => break,
                ProtocolMessage::Data(buf) => {
                    let mut vec: Vec<f32> = bytes_to_f32_vec_le(&buf);

                    codebook.append(&mut vec);
                }
                _ => anyhow::bail!("Invalid message received"),
            }
        }

        logger.info(&format!("Received codebook with {} items", codebook.len()));

        pq_codebook = codebook.as_ptr();
    }

    Ok(IndexOptions {
        dimensions: dim as usize,
        metric,
        quantization,
        multi: false,
        connectivity: m as usize,
        expansion_add: ef_construction as usize,
        expansion_search: ef as usize,
        num_threads: 0, // automatic
        // note: pq_construction and pq_output distinction is not yet implemented in usearch
        // in the future, if pq_construction is false, we will use full vectors in memory (and
        // require large memory for construction) but will output pq-quantized graph
        //
        // currently, regardless of pq_construction value, as long as pq_output is true,
        // we construct a pq_quantized index using quantized values during construction
        pq_construction: pq,
        pq_output: pq,
        num_centroids: num_centroids as usize,
        num_subvectors: num_subvectors as usize,
        codebook: pq_codebook,
    })
}

fn bytes_to_f32_vec_le(bytes: &[u8]) -> Vec<f32> {
    let mut float_vec = Vec::with_capacity(bytes.len() / 4);

    for chunk in bytes.chunks_exact(4) {
        float_vec.push(LittleEndian::read_f32(chunk));
    }

    float_vec
}

fn parse_tuple(buf: &[u8], element_bits: usize) -> Result<Row, anyhow::Error> {
    let label = u64::from_le_bytes(buf[..LABEL_SIZE].try_into()?);
    let vec: VectorType = match element_bits {
        1 => VectorType::I8(
            buf[LABEL_SIZE..]
                .iter()
                .map(|e| {
                    BitSlice::<_, Lsb0>::from_element(e)
                        .iter()
                        .map(|n| if *n.as_ref() { 0 } else { 1 })
                        .collect::<Vec<i8>>()
                })
                .concat(),
        ),
        _ => VectorType::F32(bytes_to_f32_vec_le(&buf[LABEL_SIZE..])),
    };

    Ok((label, vec))
}

fn initialize_index(
    logger: Arc<Logger>,
    stream: Arc<Mutex<TcpStream>>,
) -> Result<(usize, ThreadSafeIndex), anyhow::Error> {
    let mut buf = vec![0 as u8; INDEX_HEADER_LENGTH];
    let mut soc_stream = stream.lock().unwrap();
    match read_frame(
        &mut soc_stream,
        &mut buf,
        INDEX_HEADER_LENGTH,
        Some(INIT_MSG),
    )? {
        ProtocolMessage::Init(buf) => {
            drop(soc_stream);
            let index_options = parse_index_options(
                logger.clone(),
                stream.clone(),
                &buf[PROTOCOL_HEADER_SIZE..INDEX_HEADER_LENGTH - PROTOCOL_HEADER_SIZE],
            )?;
            logger.info(&format!(
                "Creating index with parameters dimensions={} m={} ef={} ef_construction={}",
                index_options.dimensions,
                index_options.connectivity,
                index_options.expansion_search,
                index_options.expansion_add
            ));

            let index = Index::new(&index_options)?;
            let estimated_capacity: u32 = u32::from_le_bytes(
                buf[INDEX_HEADER_LENGTH - INTEGER_SIZE..INDEX_HEADER_LENGTH]
                    .try_into()
                    .unwrap(),
            );
            logger.info(&format!("Estimated capcity is {estimated_capacity}"));
            index.reserve(estimated_capacity as usize)?;
            let mut soc_stream = stream.lock().unwrap();
            // send success code
            soc_stream.write(&[0]).unwrap();

            let element_bits = match index_options.metric {
                MetricKind::Hamming => 1,
                _ => INTEGER_SIZE * CHAR_BITS,
            };

            Ok((element_bits, ThreadSafeIndex(index)))
        }
        _ => anyhow::bail!("send init message first"),
    }
}

fn receive_rows(
    stream: Arc<Mutex<TcpStream>>,
    logger: Arc<Logger>,
    index: Arc<RwLock<ThreadSafeIndex>>,
    worker_tx: SyncSender<Row>,
    element_bits: usize,
) -> AnyhowVoidResult {
    let idx = index.read().unwrap();
    let mut current_capacity = idx.0.capacity();
    let mut stream = stream.lock().unwrap();
    let mut received_rows = 0;

    let expected_payload_size = if element_bits < CHAR_BITS {
        LABEL_SIZE + idx.0.dimensions().div_ceil(CHAR_BITS)
    } else {
        LABEL_SIZE + idx.0.dimensions() * (element_bits / CHAR_BITS)
    };

    let mut buf = vec![0 as u8; expected_payload_size];

    drop(idx);

    loop {
        match read_frame(&mut stream, &mut buf, expected_payload_size, None)? {
            ProtocolMessage::Exit => break,
            ProtocolMessage::Data(buf) => {
                let row = parse_tuple(&buf, element_bits)?;

                if received_rows == current_capacity {
                    current_capacity *= 2;
                    index.write().unwrap().0.reserve(current_capacity)?;
                    logger.debug(&format!("Index resized to {current_capacity}"));
                }

                received_rows += 1;

                worker_tx.send(row)?;
            }
            _ => anyhow::bail!("Invalid message received"),
        }
    }

    Ok(())
}

enum ProtocolMessage<'a> {
    Init(&'a mut Vec<u8>),
    Data(&'a mut Vec<u8>),
    Exit,
}

fn read_frame<'a>(
    stream: &mut TcpStream,
    buf: &'a mut Vec<u8>,
    expected_size: usize,
    match_header: Option<u32>,
) -> Result<ProtocolMessage<'a>, anyhow::Error> {
    let hdr_size = stream.read(buf)?;
    if hdr_size < PROTOCOL_HEADER_SIZE {
        anyhow::bail!("Invalid frame received");
    }

    match LittleEndian::read_u32(&buf[0..PROTOCOL_HEADER_SIZE]) {
        END_MSG => Ok(ProtocolMessage::Exit),
        msg => {
            if let Some(wanted_hdr) = match_header {
                if msg != wanted_hdr {
                    anyhow::bail!("Invalid message header");
                }
            }

            if expected_size > hdr_size {
                // if didn't read the necessarry amount of bytes
                // wait until the buffer will be filled
                // we have 1min timeout for socket
                stream.read_exact(&mut buf[hdr_size..])?;
            }

            if msg == INIT_MSG {
                Ok(ProtocolMessage::Init(buf))
            } else {
                Ok(ProtocolMessage::Data(buf))
            }
        }
    }
}

pub fn create_streaming_usearch_index(
    stream: Arc<Mutex<TcpStream>>,
    logger: Arc<Logger>,
    tmp_dir: Arc<String>,
) -> Result<(), anyhow::Error> {
    let start_time = Instant::now();
    let num_cores: usize = std::thread::available_parallelism().unwrap().into();
    logger.info(&format!("Number of available CPU cores: {}", num_cores));
    let (element_bits, index) = initialize_index(logger.clone(), stream.clone())?;
    let index = Arc::new(RwLock::new(index));

    // Create a vector to store thread handles
    let mut handles = vec![];

    let (tx, rx): (SyncSender<Row>, Receiver<Row>) = mpsc::sync_channel(2000);
    let rx_arc = Arc::new(Mutex::new(rx));

    for _ in 0..num_cores {
        // spawn thread
        let index_ref = index.clone();
        let receiver = rx_arc.clone();

        let handle = std::thread::spawn(move || -> AnyhowVoidResult {
            loop {
                let lock = receiver.lock();

                if let Err(e) = lock {
                    anyhow::bail!("{e}");
                }

                let rx = lock.unwrap();
                let row_result = rx.recv();

                // release the lock so other threads can take rows
                drop(rx);

                if let Ok(row) = row_result {
                    let index = index_ref.read().unwrap();
                    match row.1 {
                        VectorType::F32(vec) => index.0.add(row.0, &vec)?,
                        VectorType::I8(vec) => index.0.add(row.0, &vec)?,
                    }
                } else {
                    // channel has been closed
                    break;
                }
            }
            Ok(())
        });

        handles.push(handle);
    }

    receive_rows(
        stream.clone(),
        logger.clone(),
        index.clone(),
        tx,
        element_bits,
    )?;

    // Wait for all threads to finish processing
    for handle in handles {
        if let Err(e) = handle.join() {
            logger.error("{e}");
            anyhow::bail!("{:?}", e);
        }
    }

    logger.debug(&format!(
        "Indexing took {}s",
        start_time.elapsed().as_secs()
    ));

    // Send added row count
    let mut stream = stream.lock().unwrap();
    let index = index.read().unwrap();
    stream.write(&(index.0.size() as u64).to_le_bytes())?;

    // Send index file back
    logger.info("Start streaming index");

    let mut rng = rand::thread_rng();
    let index_path = format!("{tmp_dir}/ldb-index-{}.usearch", rng.gen_range(0..1000));

    let streaming_start = Instant::now();
    index.0.save(&index_path)?;
    drop(index);
    logger.debug(&format!(
        "Writing index to file took {}s{}ms",
        streaming_start.elapsed().as_secs(),
        streaming_start.elapsed().subsec_millis()
    ));

    let streaming_start = Instant::now();
    let index_file_path = Path::new(&index_path);

    let mut reader = fs::File::open(index_file_path)?;
    let mut index_buffer = Vec::with_capacity(reader.metadata()?.size() as usize);
    reader.read_to_end(&mut index_buffer)?;
    logger.debug(&format!(
        "Reading index file took {}s{}ms",
        streaming_start.elapsed().as_secs(),
        streaming_start.elapsed().subsec_millis()
    ));

    // Send index file size
    stream.write(&(index_buffer.len() as u64).to_le_bytes())?;

    let streaming_start = Instant::now();
    stream.write_all(&index_buffer)?;
    logger.debug(&format!(
        "Sending index file took {}s{}ms",
        streaming_start.elapsed().as_secs(),
        streaming_start.elapsed().subsec_millis()
    ));

    fs::remove_file(index_file_path)?;

    let _ = stream.shutdown(Shutdown::Both);

    logger.debug(&format!(
        "Total indexing took {}s",
        start_time.elapsed().as_secs()
    ));

    Ok(())
}

pub fn start_tcp_server(args: IndexServerArgs, logger: Option<Logger>) -> AnyhowVoidResult {
    let listener = TcpListener::bind(&format!("{}:{}", args.host, args.port))?;
    let logger =
        Arc::new(logger.unwrap_or(Logger::new("Lantern Indexing Server", LogLevel::Debug)));

    logger.info(&format!(
        "External Indexing Server started on {}:{}",
        args.host, args.port,
    ));

    let tmp_dir = Arc::new(args.tmp_dir);

    // TODO:: this now accepts only one request at a time
    // As single indexing job consumes whole CPU
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                logger.debug(&format!("New connection: {}", stream.peer_addr().unwrap()));
                stream.set_read_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT)))?;
                stream.set_write_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT)))?;

                let stream = Arc::new(Mutex::new(stream));
                if let Err(e) =
                    create_streaming_usearch_index(stream.clone(), logger.clone(), tmp_dir.clone())
                {
                    logger.error(&format!("Indexing error: {e}"));
                    let mut error_text: Vec<u8> = e.to_string().bytes().collect();
                    let error_header: [u8; PROTOCOL_HEADER_SIZE] =
                        unsafe { std::mem::transmute(ERR_MSG.to_le()) };
                    let mut error_header = error_header.to_vec();
                    error_header.append(&mut error_text);
                    let mut stream = stream.lock().unwrap();
                    let _ = stream.write(error_header.as_slice());
                    let _ = stream.shutdown(Shutdown::Both);
                };
            }
            Err(e) => {
                logger.error(&format!("Connection error: {e}"));
            }
        }
    }
    Ok(())
}
