use clap::{Parser, ValueEnum};
use usearch::ffi::*;

#[derive(Debug, Clone, ValueEnum)] // ArgEnum here
pub enum UMetricKind {
    L2sq,
    Cos,
    Hamming,
}

impl UMetricKind {
    pub fn value(&self) -> MetricKind {
        match self {
            UMetricKind::L2sq => {
                return MetricKind::L2Sq;
            }
            UMetricKind::Cos => {
                return MetricKind::Cos;
            }
            UMetricKind::Hamming => {
                return MetricKind::Hamming;
            }
        }
    }
}

#[derive(Parser, Debug)]
pub struct CreateIndexArgs {
    /// Fully associated database connection string including db name
    #[arg(short, long)]
    pub uri: String,

    /// Table name
    #[arg(short, long)]
    pub table: String,

    /// Column name
    #[arg(short, long)]
    pub column: String,

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
    #[arg(short)]
    pub dims: usize,

    /// Distance algorithm
    #[arg(long, value_enum, default_value_t = UMetricKind::L2sq)] // arg_enum here
    pub metric_kind: UMetricKind,

    /// Index output file
    #[arg(short, long, default_value = "index.usearch")] // arg_enum here
    pub out: String,
}
