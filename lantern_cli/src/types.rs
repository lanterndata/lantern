pub static JOB_CANCELLED_MESSAGE: &'static str = "Job cancelled";

pub type AnyhowUsizeResult = Result<usize, anyhow::Error>;
pub type AnyhowVoidResult = Result<(), anyhow::Error>;
pub type ProgressCbFn = Box<dyn Fn(u8) + Send + Sync>;
