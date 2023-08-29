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
#[command(version, about, long_about = None)]
pub struct Args {
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
    #[arg(short)]
    pub m: usize,

    /// The size of the dynamic list for the nearest neighbors in construction
    #[arg(long)]
    pub efc: usize,

    /// The size of the dynamic list for the nearest neighbors in search
    #[arg(long)]
    pub ef: usize,

    /// Dimensions of vector
    #[arg(short)]
    pub dims: usize,

    /// Distance algorithm
    #[arg(long)] // arg_enum here
    pub metric_kind: UMetricKind,

    /// Index output file
    #[arg(short, long)] // arg_enum here
    pub out: String,
}
