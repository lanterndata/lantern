use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct PQArgs {
    /// Fully associated database connection string including db name
    #[arg(short, long)]
    pub uri: String,

    /// Table name
    #[arg(short, long)]
    pub table: String,

    /// Schema name
    #[arg(short, long, default_value = "public")]
    pub schema: String,

    /// Column name to quantize
    #[arg(short, long)]
    pub column: String,

    /// Name for codebook table
    #[arg(long)]
    pub codebook_table_name: Option<String>,

    /// Dataset limit. Limit should be greater or equal to cluster count
    #[arg(long)]
    pub dataset_limit: Option<usize>,

    /// Start Offset ID, used in GCP job, to keep the random generated offset for parallel tasks
    #[arg(long)]
    pub start_offset_id: Option<usize>,

    /// Dataset size, used in GCP job, to not fetch it on every task
    #[arg(long)]
    pub dataset_size: Option<usize>,

    /// Cluster count for kmeans
    #[arg(long, default_value_t = 256)]
    pub clusters: usize,

    /// Subvector count to split vector
    #[arg(long, default_value_t = 1)]
    pub splits: usize,

    /// Subvector part to process
    #[arg(long)]
    pub subvector_id: Option<usize>,

    /// If true, codebook table will not be created and pq column will not be added to table. So
    /// they should be set up externally
    #[arg(long, default_value_t = false)]
    pub skip_table_setup: bool,

    /// If true vectors will not be quantized and exported to the table
    #[arg(long, default_value_t = false)]
    pub skip_vector_quantization: bool,

    /// If true codebook will not be created
    #[arg(long, default_value_t = false)]
    pub skip_codebook_creation: bool,

    /// Primary key of the table, needed for quantization job
    #[arg(long, default_value = "id")]
    pub pk: String,

    /// If true codebook table and pq column will be deleted if exists
    #[arg(long, default_value_t = false)]
    pub overwrite: bool,

    /// Number of total tasks running (used in gcp batch jobs)
    #[arg(long)]
    pub total_task_count: Option<usize>,

    /// Number of tasks running in parallel (used in gcp batch jobs)
    #[arg(long)]
    pub parallel_task_count: Option<usize>,

    /// Task id of currently running quantization job (used in gcp batch jobs)
    #[arg(long)]
    pub quantization_task_id: Option<usize>,

    // GCP ARGS
    /// If true job will be submitted to gcp
    #[arg(long, default_value_t = false)]
    pub run_on_gcp: bool,

    /// Image tag to use for GCR. example: 0.0.38-cpu
    #[arg(long)]
    pub gcp_cli_image_tag: Option<String>,

    /// GCP project ID
    #[arg(long)]
    pub gcp_project: Option<String>,

    /// GCP region. Default: us-central1
    #[arg(long)]
    pub gcp_region: Option<String>,

    /// Full GCR image name. default: {gcp_region}-docker.pkg.dev/{gcp_project_id}/lanterndata/lantern-cli:{gcp_cli_image_tag}
    #[arg(long)]
    pub gcp_image: Option<String>,

    /// Task count for quantization. default: calculated automatically based on dataset size
    #[arg(long)]
    pub gcp_quantization_task_count: Option<usize>,

    /// Parallel tasks for quantization. default: calculated automatically based on
    /// max connections
    #[arg(long)]
    pub gcp_quantization_task_parallelism: Option<usize>,

    /// Parallel tasks for quantization. default: calculated automatically based on
    /// max connections and dataset size
    #[arg(long)]
    pub gcp_clustering_task_parallelism: Option<usize>,

    /// If image is hosted on GCR this will speed up the VM startup time
    #[arg(long, default_value_t = true)]
    pub gcp_enable_image_streaming: bool,

    /// CPU count for one VM in clustering task. default: calculated based on dataset size
    #[arg(long)]
    pub gcp_clustering_cpu: Option<usize>,

    /// Memory GB for one VM in clustering task. default: calculated based on CPU count
    #[arg(long)]
    pub gcp_clustering_memory_gb: Option<usize>,

    /// CPU count for one VM in quantization task. default: calculated based on dataset size
    #[arg(long)]
    pub gcp_quantization_cpu: Option<usize>,

    /// Memory GB for one VM in quantization task. default: calculated based on CPU count
    #[arg(long)]
    pub gcp_quantization_memory_gb: Option<usize>,
}
