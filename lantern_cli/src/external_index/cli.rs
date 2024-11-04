use clap::{Parser, ValueEnum};
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
