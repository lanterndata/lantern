use clap::{ArgAction, Parser, ValueEnum};
use usearch::ffi::*;

#[derive(Debug, Clone, ValueEnum)] // ArgEnum here
pub enum UMetricKind {
    L2sq,
    Cos,
    Hamming,
}

impl UMetricKind {
    pub fn from_ops(ops: &str) -> Result<UMetricKind, anyhow::Error> {
        match ops {
            "dist_l2sq_ops" => {
                return Ok(UMetricKind::L2sq);
            }
            "dist_cos_ops" => {
                return Ok(UMetricKind::Cos);
            }
            "dist_hamming_ops" => {
                return Ok(UMetricKind::Hamming);
            }
            _ => anyhow::bail!("Invalid ops {ops}"),
        }
    }
    pub fn to_ops(&self) -> String {
        match self {
            UMetricKind::L2sq => {
                return "dist_l2sq_ops".to_owned();
            }
            UMetricKind::Cos => {
                return "dist_cos_ops".to_owned();
            }
            UMetricKind::Hamming => {
                return "dist_hamming_ops".to_owned();
            }
        }
    }
    pub fn from(metric_kind: &str) -> Result<UMetricKind, anyhow::Error> {
        match metric_kind {
            "l2sq" => {
                return Ok(UMetricKind::L2sq);
            }
            "cos" => {
                return Ok(UMetricKind::Cos);
            }
            "cosine" => {
                return Ok(UMetricKind::Cos);
            }
            "hamming" => {
                return Ok(UMetricKind::Hamming);
            }
            _ => anyhow::bail!("Invalid metric {metric_kind}"),
        }
    }
    pub fn from_u32(metric_kind: u32) -> Result<UMetricKind, anyhow::Error> {
        match metric_kind {
            3 => {
                return Ok(UMetricKind::L2sq);
            }
            1 => {
                return Ok(UMetricKind::Cos);
            }
            8 => {
                return Ok(UMetricKind::Hamming);
            }
            _ => anyhow::bail!("Invalid metric {metric_kind}"),
        }
    }
    pub fn to_string(&self) -> String {
        match self {
            UMetricKind::L2sq => {
                return "l2sq".to_owned();
            }
            UMetricKind::Cos => {
                return "cos".to_owned();
            }
            UMetricKind::Hamming => {
                return "hamming".to_owned();
            }
        }
    }
    pub fn value(&self) -> MetricKind {
        match self {
            UMetricKind::L2sq => {
                return MetricKind::L2sq;
            }
            UMetricKind::Cos => {
                return MetricKind::Cos;
            }
            UMetricKind::Hamming => {
                return MetricKind::Hamming;
            }
        }
    }

    pub fn sql_function(&self) -> String {
        match self {
            UMetricKind::L2sq => {
                return "l2sq_dist".to_owned();
            }
            UMetricKind::Cos => {
                return "cos_dist".to_owned();
            }
            UMetricKind::Hamming => {
                return "hamming_dist".to_owned();
            }
        }
    }

    pub fn sql_operator(&self) -> String {
        match self {
            UMetricKind::L2sq => {
                return "<->".to_owned();
            }
            UMetricKind::Cos => {
                return "<=>".to_owned();
            }
            UMetricKind::Hamming => {
                return "<+>".to_owned();
            }
        }
    }
}

#[derive(Parser, Debug)]
pub struct CreateIndexArgs {
    /// Fully associated database connection string including db name
    #[arg(short, long)]
    pub uri: String,

    /// Schema name
    #[arg(short, long, default_value = "public")]
    pub schema: String,

    /// Table name
    #[arg(short, long)]
    pub table: String,

    /// Column name
    #[arg(short, long)]
    pub column: String,

    /// Use already created codebook to create product-quantized binary index
    #[arg(short, long, default_value_t = false)]
    pub pq: bool,

    /// Number of neighbours for each vector
    #[arg(short, default_value_t = 16)]
    pub m: usize,

    /// The size of the dynamic list for the nearest neighbors in construction
    #[arg(long, default_value_t = 128)]
    pub efc: usize,

    /// The size of the dynamic list for the nearest neighbors in search
    #[arg(long, default_value_t = 64)]
    pub ef: usize,

    /// Dimensions of vector
    #[arg(short, default_value_t = 0)]
    pub dims: usize,

    /// Distance algorithm
    #[arg(long, value_enum, default_value_t = UMetricKind::L2sq)] // arg_enum here
    pub metric_kind: UMetricKind,

    /// Index output file
    #[arg(short, long, default_value = "index.usearch")] // arg_enum here
    pub out: String,

    /// Import index to database (should be run as db superuser to have access)
    #[arg(short, long, default_value_t = false)]
    pub import: bool,

    /// If database is not on the same server where the job is running
    #[arg(short, long, default_value_t = true, action = ArgAction::Set)]
    pub remote_database: bool,

    /// Index name to use when imporrting index to database
    #[arg(long)]
    pub index_name: Option<String>,
}

#[derive(Parser, Debug, Clone)]
pub struct IndexServerArgs {
    /// Host to bind
    #[arg(long, default_value = "0.0.0.0")]
    pub host: String,

    /// Temp directory to save intermediate files
    #[arg(long, default_value = "/tmp")]
    pub tmp_dir: String,

    /// Port to bind
    #[arg(long, default_value_t = 8998)]
    pub port: usize,

    /// Status Server Port to bind
    #[arg(long, default_value_t = 8999)]
    pub status_port: usize,

    /// SSL Certificate path
    #[arg(long)]
    pub cert: Option<String>,

    /// SSL Certificate key path
    #[arg(long)]
    pub key: Option<String>,
}
