use std::{
    env,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

use lantern_cli::pq;
use lantern_cli::pq::*;
use lantern_cli::utils::{get_full_table_name, quote_ident};
use postgres::{Client, NoTls};

fn setup_db_tables(client: &mut Client, table_name: &str, range_start: usize, range_end: usize) {
    client
        .batch_execute(&format!(
            "
    DROP TABLE IF EXISTS \"{table_name}\";
    CREATE TABLE \"{table_name}\" (id SERIAL PRIMARY KEY, v REAL[]);
    INSERT INTO \"{table_name}\" SELECT generate_series({range_start}, {range_end}), (select array_agg(random() * 1.0) from generate_series (0, 128 - 1));
"
        ))
        .expect("Could not create necessarry tables");
}

fn drop_db_tables(client: &mut Client, table_name: &str, codebook_table_name: &str) {
    client
        .batch_execute(&format!(
            "
        DROP TABLE IF EXISTS \"{table_name}\";
        DROP TABLE IF EXISTS {codebook_table_name};
    "
        ))
        .expect("Could not drop tables");
}

#[test]
fn test_full_pq() {
    let db_url = env::var("DB_URL").expect("`DB_URL` not specified");
    let table_name = String::from("_pq_test");
    let codebook_table_name = get_full_table_name("_lantern_internal", "pq__pq_test_v");
    let mut db_client = Client::connect(&db_url, NoTls).expect("Database connection failed");
    drop_db_tables(&mut db_client, &table_name, &codebook_table_name);
    setup_db_tables(&mut db_client, &table_name, 1, 1000);

    let final_progress = Arc::new(AtomicU8::new(0));
    let final_progress_r1 = final_progress.clone();

    let callback = move |progress: u8| {
        final_progress_r1.store(progress, Ordering::SeqCst);
    };

    lantern_cli::pq::quantize_table(
        cli::PQArgs {
            uri: db_url.clone(),
            column: "v".to_owned(),
            table: table_name.clone(),
            schema: "public".to_owned(),
            codebook_table_name: None,
            clusters: 10,
            splits: 32,
            dataset_limit: None,
            subvector_id: None,
            skip_table_setup: false,
            skip_vector_quantization: false,
            skip_codebook_creation: false,
            pk: "id".to_owned(),
            total_task_count: None,
            parallel_task_count: None,
            quantization_task_id: None,
            run_on_gcp: false,
            gcp_cli_image_tag: None,
            gcp_project: None,
            gcp_region: None,
            gcp_image: None,
            gcp_quantization_task_count: None,
            gcp_quantization_task_parallelism: None,
            gcp_clustering_task_parallelism: None,
            gcp_enable_image_streaming: false,
            gcp_clustering_cpu: None,
            gcp_clustering_memory_gb: None,
            gcp_quantization_cpu: None,
            gcp_quantization_memory_gb: None,
            dataset_size: None,
            start_offset_id: None,
        },
        Some(Box::new(callback)),
        None,
        None,
    )
    .unwrap();

    let centroid_dim = 128 / 32;
    let cnt = db_client
        .query_one(
            &format!("SELECT COUNT(*) FROM {codebook_table_name} WHERE ARRAY_LENGTH(c, 1)={centroid_dim}"),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 10 * 32);
    assert_eq!(final_progress.load(Ordering::SeqCst), 100);

    let cnt = db_client
        .query_one(
            &format!("SELECT COUNT(*) FROM {table_name} WHERE ARRAY_LENGTH(v_pq::INT[], 1) != 32 or v_pq is null"),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 0);

    drop_db_tables(&mut db_client, &table_name, &codebook_table_name);
}

#[test]
fn test_chunked_pq() {
    let db_url = env::var("DB_URL").expect("`DB_URL` not specified");
    let table_name = String::from("_lantern_Pq_TeSt_2");
    let quoted_table_name = quote_ident(&table_name);
    let codebook_table_name = get_full_table_name("_lantern_internal", "pq__lantern_Pq_TeSt_2_v");
    let mut db_client = Client::connect(&db_url, NoTls).expect("Database connection failed");
    drop_db_tables(&mut db_client, &table_name, &codebook_table_name);
    setup_db_tables(&mut db_client, &table_name, 0, 999);

    // ================= Run setup job ================
    pq::quantize_table(
        cli::PQArgs {
            uri: db_url.clone(),
            column: "v".to_owned(),
            table: table_name.clone(),
            schema: "public".to_owned(),
            codebook_table_name: None,
            clusters: 10,
            splits: 32,
            dataset_limit: None,
            subvector_id: None,
            skip_table_setup: false,
            skip_vector_quantization: true,
            skip_codebook_creation: true,
            pk: "id".to_owned(),
            total_task_count: None,
            parallel_task_count: None,
            quantization_task_id: None,
            run_on_gcp: false,
            gcp_cli_image_tag: None,
            gcp_project: None,
            gcp_region: None,
            gcp_image: None,
            gcp_quantization_task_count: None,
            gcp_quantization_task_parallelism: None,
            gcp_clustering_task_parallelism: None,
            gcp_enable_image_streaming: false,
            gcp_clustering_cpu: None,
            gcp_clustering_memory_gb: None,
            gcp_quantization_cpu: None,
            gcp_quantization_memory_gb: None,
            dataset_size: None,
            start_offset_id: None,
        },
        None,
        None,
        None,
    )
    .unwrap();

    let centroid_dim = 128 / 32;
    let cnt = db_client
        .query_one(
            &format!("SELECT COUNT(*) FROM {codebook_table_name} WHERE ARRAY_LENGTH(c, 1)={centroid_dim}"),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 0);

    let cnt = db_client
        .query_one(
            &format!("SELECT COUNT(*) FROM {quoted_table_name} WHERE v_pq IS NULL"),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 1000);
    // ==================================================================================

    // ================= Run clustering job ================
    for i in 0..32 {
        pq::quantize_table(
            cli::PQArgs {
                uri: db_url.clone(),
                column: "v".to_owned(),
                table: table_name.clone(),
                schema: "public".to_owned(),
                codebook_table_name: None,
                clusters: 10,
                splits: 32,
                dataset_limit: None,
                subvector_id: Some(i),
                skip_table_setup: true,
                skip_vector_quantization: true,
                skip_codebook_creation: false,
                pk: "id".to_owned(),
                total_task_count: None,
                parallel_task_count: Some(1),
                quantization_task_id: None,
                run_on_gcp: false,
                gcp_cli_image_tag: None,
                gcp_project: None,
                gcp_region: None,
                gcp_image: None,
                gcp_quantization_task_count: None,
                gcp_quantization_task_parallelism: None,
                gcp_clustering_task_parallelism: None,
                gcp_enable_image_streaming: false,
                gcp_clustering_cpu: None,
                gcp_clustering_memory_gb: None,
                gcp_quantization_cpu: None,
                gcp_quantization_memory_gb: None,
                dataset_size: None,
                start_offset_id: None,
            },
            None,
            None,
            None,
        )
        .unwrap();
    }

    let centroid_dim = 128 / 32;
    let cnt = db_client
        .query_one(
            &format!("SELECT COUNT(*) FROM {codebook_table_name} WHERE ARRAY_LENGTH(c, 1)={centroid_dim}"),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 10 * 32);

    let cnt = db_client
        .query_one(
            &format!(
                "SELECT COUNT(*) FROM {quoted_table_name} WHERE ARRAY_LENGTH(v_pq::INT[], 1) IS NULL"
            ),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 1000);
    // ==================================================================================

    // ================= Run quantization job ================
    for i in 0..3 {
        pq::quantize_table(
            cli::PQArgs {
                uri: db_url.clone(),
                column: "v".to_owned(),
                table: table_name.clone(),
                schema: "public".to_owned(),
                codebook_table_name: None,
                clusters: 10,
                splits: 32,
                dataset_limit: None,
                subvector_id: None,
                skip_table_setup: true,
                skip_vector_quantization: false,
                skip_codebook_creation: true,
                pk: "id".to_owned(),
                total_task_count: Some(3),
                parallel_task_count: Some(1),
                quantization_task_id: Some(i),
                run_on_gcp: false,
                gcp_cli_image_tag: None,
                gcp_project: None,
                gcp_region: None,
                gcp_image: None,
                gcp_quantization_task_count: None,
                gcp_quantization_task_parallelism: None,
                gcp_clustering_task_parallelism: None,
                gcp_enable_image_streaming: false,
                gcp_clustering_cpu: None,
                gcp_clustering_memory_gb: None,
                gcp_quantization_cpu: None,
                gcp_quantization_memory_gb: None,
                dataset_size: None,
                start_offset_id: None,
            },
            None,
            None,
            None,
        )
        .unwrap();
    }

    let centroid_dim = 128 / 32;
    let cnt = db_client
        .query_one(
            &format!("SELECT COUNT(*) FROM {codebook_table_name} WHERE ARRAY_LENGTH(c, 1)={centroid_dim}"),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 10 * 32);

    let cnt = db_client
        .query_one(
            &format!(
                "SELECT COUNT(*) FROM {quoted_table_name} WHERE ARRAY_LENGTH(v_pq::INT[], 1) != 32 or v_pq is null"
            ),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 0);
    // ==================================================================================
    drop_db_tables(&mut db_client, &table_name, &codebook_table_name);
}

#[test]
fn test_chunked_pq_with_limit() {
    let db_url = env::var("DB_URL").expect("`DB_URL` not specified");
    let table_name = String::from("_lantern_Pq_TeSt_3");
    let quoted_table_name = quote_ident(&table_name);
    let codebook_table_name = get_full_table_name("_lantern_internal", "pq__lantern_Pq_TeSt_3_v");
    let mut db_client = Client::connect(&db_url, NoTls).expect("Database connection failed");
    drop_db_tables(&mut db_client, &table_name, &codebook_table_name);
    setup_db_tables(&mut db_client, &table_name, 1, 1000);

    // ================= Run setup job ================
    pq::quantize_table(
        cli::PQArgs {
            uri: db_url.clone(),
            column: "v".to_owned(),
            table: table_name.clone(),
            schema: "public".to_owned(),
            codebook_table_name: None,
            clusters: 10,
            splits: 32,
            dataset_limit: Some(200),
            subvector_id: None,
            skip_table_setup: false,
            skip_vector_quantization: true,
            skip_codebook_creation: true,
            pk: "id".to_owned(),
            total_task_count: None,
            parallel_task_count: None,
            quantization_task_id: None,
            run_on_gcp: false,
            gcp_cli_image_tag: None,
            gcp_project: None,
            gcp_region: None,
            gcp_image: None,
            gcp_quantization_task_count: None,
            gcp_quantization_task_parallelism: None,
            gcp_clustering_task_parallelism: None,
            gcp_enable_image_streaming: false,
            gcp_clustering_cpu: None,
            gcp_clustering_memory_gb: None,
            gcp_quantization_cpu: None,
            gcp_quantization_memory_gb: None,
            dataset_size: None,
            start_offset_id: None,
        },
        None,
        None,
        None,
    )
    .unwrap();

    let centroid_dim = 128 / 32;
    let cnt = db_client
        .query_one(
            &format!("SELECT COUNT(*) FROM {codebook_table_name} WHERE ARRAY_LENGTH(c, 1)={centroid_dim}"),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 0);

    let cnt = db_client
        .query_one(
            &format!("SELECT COUNT(*) FROM {quoted_table_name} WHERE v_pq IS NULL"),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 1000);
    // ==================================================================================

    // ================= Run clustering job ================
    for i in 0..32 {
        pq::quantize_table(
            cli::PQArgs {
                uri: db_url.clone(),
                column: "v".to_owned(),
                table: table_name.clone(),
                schema: "public".to_owned(),
                codebook_table_name: None,
                clusters: 10,
                dataset_limit: Some(200),
                splits: 32,
                subvector_id: Some(i),
                skip_table_setup: true,
                skip_vector_quantization: true,
                skip_codebook_creation: false,
                pk: "id".to_owned(),
                total_task_count: None,
                parallel_task_count: Some(1),
                quantization_task_id: None,
                run_on_gcp: false,
                gcp_cli_image_tag: None,
                gcp_project: None,
                gcp_region: None,
                gcp_image: None,
                gcp_quantization_task_count: None,
                gcp_quantization_task_parallelism: None,
                gcp_clustering_task_parallelism: None,
                gcp_enable_image_streaming: false,
                gcp_clustering_cpu: None,
                gcp_clustering_memory_gb: None,
                gcp_quantization_cpu: None,
                gcp_quantization_memory_gb: None,
                dataset_size: None,
                start_offset_id: None,
            },
            None,
            None,
            None,
        )
        .unwrap();
    }

    let centroid_dim = 128 / 32;
    let cnt = db_client
        .query_one(
            &format!("SELECT COUNT(*) FROM {codebook_table_name} WHERE ARRAY_LENGTH(c, 1)={centroid_dim}"),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 10 * 32);

    let cnt = db_client
        .query_one(
            &format!(
                "SELECT COUNT(*) FROM {quoted_table_name} WHERE ARRAY_LENGTH(v_pq::INT[], 1) IS NULL"
            ),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 1000);
    // ==================================================================================

    // ================= Run quantization job ================
    for i in 0..3 {
        pq::quantize_table(
            cli::PQArgs {
                uri: db_url.clone(),
                column: "v".to_owned(),
                table: table_name.clone(),
                schema: "public".to_owned(),
                codebook_table_name: None,
                clusters: 10,
                dataset_limit: Some(200),
                splits: 32,
                subvector_id: None,
                skip_table_setup: true,
                skip_vector_quantization: false,
                skip_codebook_creation: true,
                pk: "id".to_owned(),
                total_task_count: Some(3),
                parallel_task_count: Some(1),
                quantization_task_id: Some(i),
                run_on_gcp: false,
                gcp_cli_image_tag: None,
                gcp_project: None,
                gcp_region: None,
                gcp_image: None,
                gcp_quantization_task_count: None,
                gcp_quantization_task_parallelism: None,
                gcp_clustering_task_parallelism: None,
                gcp_enable_image_streaming: false,
                gcp_clustering_cpu: None,
                gcp_clustering_memory_gb: None,
                gcp_quantization_cpu: None,
                gcp_quantization_memory_gb: None,
                dataset_size: None,
                start_offset_id: None,
            },
            None,
            None,
            None,
        )
        .unwrap();
    }

    let centroid_dim = 128 / 32;
    let cnt = db_client
        .query_one(
            &format!("SELECT COUNT(*) FROM {codebook_table_name} WHERE ARRAY_LENGTH(c, 1)={centroid_dim}"),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 10 * 32);

    let cnt = db_client
        .query_one(
            &format!(
                "SELECT COUNT(*) FROM {quoted_table_name} WHERE ARRAY_LENGTH(v_pq::INT[], 1) != 32"
            ),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    assert_eq!(cnt, 0);
    // ==================================================================================
    drop_db_tables(&mut db_client, &table_name, &codebook_table_name);
}
